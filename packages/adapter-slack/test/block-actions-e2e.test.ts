import { describe, expect, it, mock, spyOn } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import { SlackWebApi } from "../src/api";
import { describePanelResponse, type SlackBlockAction } from "../src/interactions";
import { type GatewayClientLike, settleSlackDelivery } from "../src/main";
import { approvalPanelBlocks, askUserPanelBlocks } from "../src/panels";

const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C123" };
const warning = "[recovered - may be a duplicate]";

function settlementGateway() {
	const gateway: Pick<GatewayClientLike, "request"> = {
		async request<T = unknown>(_verb: string, _params?: unknown): Promise<T> {
			throw new Error("Unexpected unmocked gateway request");
		},
	};
	// Keep the generic method on the gateway; Bun's spy exposes call inspection
	// but its own call signature erases the method's type parameter.
	const request = spyOn(gateway, "request").mockResolvedValue(undefined);
	return { gateway, request };
}

function panelMessage(kind: "ask-user" | "approval", expired = false): ChatMessagePayload {
	const expiresAt = expired ? "2000-01-01T00:00:00Z" : "2999-01-01T00:00:00Z";
	return {
		turnId: "turn123",
		deliveryId: "delivery123",
		origin,
		role: "assistant",
		text: "Panel delivery",
		final: true,
		replyToMessageId: "C123:123.456",
		...(kind === "ask-user"
			? {
					askUserPanel: {
						panelId: "question_001",
						question: "Choose a tool",
						expiresAt,
						options: [
							{ id: "option_a", label: "Tool A" },
							{ id: "option_b", label: "Tool B" },
						],
					},
				}
			: { approvalPanel: { panelId: "approval_001", message: "Approve deployment?", expiresAt } }),
	};
}

function actionEvent(action: SlackBlockAction["actions"][number], userId = "U123"): SlackBlockAction {
	return {
		type: "block_actions",
		actions: [action],
		trigger_id: "trigger123",
		user: { id: userId, username: "user", name: "Test User", team_id: "T123" },
		team: { id: "T123", domain: "workspace" },
		channel: { id: "C123", name: "general" },
		message: { type: "message", ts: "123.456" },
		token: "test-token",
		api_app_id: "A123",
		event_ts: "123.457",
		event_id: "Ev123",
	};
}

function renderedButtons(blocks: Record<string, unknown>[]): SlackBlockAction["actions"] {
	const buttons: SlackBlockAction["actions"] = [];
	for (const block of blocks) {
		if (block.type !== "actions") continue;
		if (!Array.isArray(block.elements)) throw new Error("Actions block lacks elements");
		for (const element of block.elements) {
			if (
				typeof element !== "object" ||
				element === null ||
				element.type !== "button" ||
				typeof element.action_id !== "string" ||
				typeof element.value !== "string"
			) {
				throw new Error("Rendered button lacks its action ID or value");
			}
			buttons.push({ type: element.type, action_id: element.action_id, value: element.value });
		}
	}
	return buttons;
}

// These are adapter-boundary tests, not Socket Mode or gateway ledger integration.
describe("Slack production panel render -> parse", () => {
	it("preserves panel and option IDs containing underscores", () => {
		const message = panelMessage("ask-user");
		const buttons = renderedButtons(askUserPanelBlocks(message));
		expect(buttons).toHaveLength(2);
		expect(buttons.map((button) => describePanelResponse(actionEvent(button), origin))).toEqual([
			expect.objectContaining({
				panelId: "question_001",
				selectedOptionId: "option_a",
				responseKind: "option_selected",
				responderId: "U123",
			}),
			expect.objectContaining({
				panelId: "question_001",
				selectedOptionId: "option_b",
				responseKind: "option_selected",
				responderId: "U123",
			}),
		]);
	});

	it("parses rendered approval allow and deny buttons", () => {
		const buttons = renderedButtons(approvalPanelBlocks(panelMessage("approval")));
		expect(buttons).toHaveLength(2);
		expect(buttons.map((button) => describePanelResponse(actionEvent(button), origin))).toEqual([
			expect.objectContaining({ panelId: "approval_001", responseKind: "approved" }),
			expect.objectContaining({ panelId: "approval_001", responseKind: "denied" }),
		]);
	});

	it("preserves offered approval option IDs including rejection", () => {
		const message: ChatMessagePayload = {
			...panelMessage("approval"),
			approvalPanel: {
				panelId: "permission_001",
				message: "Run command?",
				expiresAt: "2999-01-01T00:00:00Z",
				options: [
					{ optionId: "allow_once", name: "Allow once", kind: "allow_once" },
					{ optionId: "reject_once", name: "Reject once", kind: "reject_once" },
				],
			},
		};
		const buttons = renderedButtons(approvalPanelBlocks(message));
		expect(buttons).toHaveLength(2);
		expect(buttons.map((button) => describePanelResponse(actionEvent(button), origin))).toEqual([
			expect.objectContaining({
				panelId: "permission_001",
				responseKind: "option_selected",
				selectedOptionId: "allow_once",
			}),
			expect.objectContaining({
				panelId: "permission_001",
				responseKind: "option_selected",
				selectedOptionId: "reject_once",
			}),
		]);
	});
});

describe("Slack panel parser only", () => {
	it("describes both responders without claiming first-response arbitration", () => {
		const button = { type: "button", action_id: "ask_user_question_001_option_a", value: "option_a" };
		expect(describePanelResponse(actionEvent(button, "U111"), origin)?.responderId).toBe("U111");
		expect(describePanelResponse(actionEvent(button, "U222"), origin)?.responderId).toBe("U222");
	});

	it("preserves DM and channel engagement context", () => {
		const event = actionEvent({ type: "button", action_id: "ask_user_question_001_option_a", value: "option_a" });
		const dm: OriginRef = { platform: "slack", kind: "dm", conversationId: "D123", peerId: "U123" };
		expect(describePanelResponse(event, dm)).toMatchObject({ origin: dm, engagement: { group: false } });
		expect(describePanelResponse(event, origin)).toMatchObject({ origin, engagement: { group: true } });
	});

	for (const [actionId, value] of [
		["ask_user_question_001_option_a", undefined],
		["ask_user_question_001_option_a", ""],
		["ask_user_question_001_option_a", "option_b"],
		["ask_user_question_001_option_a", "a_extra"],
		["ask_user_question_001_option_a", "001_option_a_extra"],
		["ask_user__option_a", "option_a"],
		["ask_user_option_a", "option_a"],
		["ask_user_question_001option_a", "option_a"],
		["other_question_001_option_a", "option_a"],
	]) {
		it(`rejects invalid ask-user boundary ${actionId} / ${String(value)}`, () => {
			if (actionId === undefined) throw new Error("Missing test action ID");
			expect(describePanelResponse(actionEvent({ type: "button", action_id: actionId, value }), origin)).toBeNull();
		});
	}
});

describe("Slack panel delivery settlement (mock transports)", () => {
	for (const kind of ["ask-user", "approval"] satisfies Array<"ask-user" | "approval">) {
		for (const expired of [false, true]) {
			it(`${kind}, expired=${expired}: posts visible uncertainty and confirms only after acknowledgement`, async () => {
				const { gateway, request } = settlementGateway();
				const posted = Promise.withResolvers<void>();
				const acknowledgement = Promise.withResolvers<Response>();
				const payloads: unknown[] = [];
				const api = new SlackWebApi("test-token", async (url, init) => {
					expect(url).toBe("https://slack.com/api/chat.postMessage");
					if (typeof init?.body !== "string") throw new Error("Expected JSON post body");
					const payload: unknown = JSON.parse(init.body);
					payloads.push(payload);
					posted.resolve();
					return acknowledgement.promise;
				});
				const settlement = settleSlackDelivery(gateway, api, {
					...panelMessage(kind, expired),
					redelivered: true,
					duplicateWarning: true,
				});
				await posted.promise;
				expect(request).not.toHaveBeenCalled();
				const title = kind === "ask-user" ? "Choose a tool" : "Approve deployment?";
				expect(payloads).toEqual([
					expect.objectContaining({
						channel: "C123",
						thread_ts: "123.456",
						text: expect.stringContaining(`${warning} ${title}`),
						blocks: [
							{ type: "section", text: { type: "mrkdwn", text: `${warning} ${title}` } },
							expired
								? { type: "section", text: { type: "mrkdwn", text: expect.stringContaining("expired") } }
								: { type: "actions", elements: expect.any(Array) },
						],
					}),
				]);
				if (expired) expect(payloads).toEqual([expect.objectContaining({ text: expect.stringContaining("expired") })]);
				acknowledgement.resolve(Response.json({ ok: true, channel: "C123", ts: "124.000" }));
				await settlement;
				expect(request.mock.calls).toEqual([["delivery.confirm", { deliveryId: "delivery123" }]]);
			});

			for (const failure of ["rejected", "uncertain"]) {
				it(`${kind}, expired=${expired}: ${failure} post fails without confirming`, async () => {
					const { gateway, request } = settlementGateway();
					const post = mock(async () => {
						if (failure === "uncertain") throw new Error("connection lost after write");
						return Response.json({ ok: false, error: "channel_not_found" });
					});
					await settleSlackDelivery(gateway, new SlackWebApi("test-token", post), {
						...panelMessage(kind, expired),
						redelivered: true,
						duplicateWarning: true,
					});
					expect(post).toHaveBeenCalledTimes(1);
					expect(request.mock.calls).toEqual([
						[
							"delivery.fail",
							{
								deliveryId: "delivery123",
								ambiguous: failure === "uncertain",
								reason:
									failure === "uncertain"
										? "connection lost after write"
										: "Slack API request failed: channel_not_found",
							},
						],
					]);
				});
			}
		}
	}
});

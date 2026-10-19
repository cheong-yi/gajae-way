import { expect, test } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import {
	type DiscordClientLike,
	openDiscordPane,
	settleDiscordDelivery,
	subscribeDiscordPaneRequests,
} from "../src/main";

const origin: OriginRef = {
	platform: "discord",
	kind: "thread",
	conversationId: "child",
	parentId: "parent",
	threadVisibility: "public",
};
for (const failure of ["missing", "archived"] as const) {
	test(`pane noChildFallback refuses ${failure} thread without fetching parent`, async () => {
		const fetched: string[] = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		const discord: DiscordClientLike = {
			channels: {
				fetch: async (id) => {
					fetched.push(id);
					if (failure === "missing") throw Object.assign(new Error("Unknown Channel"), { code: 10003 });
					return {
						archived: true,
						setArchived: async () => {
							throw new Error("Missing Permissions");
						},
						send: async () => {
							throw new Error("must not send");
						},
					};
				},
			},
		};
		const payload: ChatMessagePayload = {
			origin,
			turnId: "turn",
			deliveryId: "delivery",
			role: "assistant",
			text: "private pane transcript",
			final: false,
			noChildFallback: true,
		};
		await settleDiscordDelivery(
			{
				request: async <T>(verb: string, params?: unknown) => {
					requests.push({ verb, params });
					return {} as T;
				},
			},
			discord,
			payload,
		);
		expect(fetched).toEqual(["child"]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.verb).toBe("delivery.fail");
	});
}

test("pane gateway request starts a fresh thread under the parent channel independently of mentions", async () => {
	const fetched: string[] = [];
	const starts: unknown[] = [];
	const anchors: unknown[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id) => {
				fetched.push(id);
				return {
					send: async (text: unknown) => {
						anchors.push(text);
						return {
							startThread: async (input: unknown) => {
								starts.push(input);
								return { id: "new-child" };
							},
						};
					},
				};
			},
		},
	};
	expect(await openDiscordPane(discord, { parent: origin, name: "FirstMate parser" })).toBe("new-child");
	expect(fetched).toEqual(["parent"]);
	expect(anchors).toEqual(["FirstMate parser"]);
	expect(starts).toEqual([{ name: "FirstMate parser", autoArchiveDuration: 1440 }]);
});

test("pane subscription reports request success and failure to the gateway", async () => {
	let handler!: (payload: unknown) => void;
	let unsubscribed = false;
	const responses: unknown[] = [];
	const off = subscribeDiscordPaneRequests(
		{
			on: (event, callback) => {
				expect(event).toBe("discord.thread.open");
				handler = callback;
				return () => {
					unsubscribed = true;
				};
			},
			onChatMessage: () => () => {},
			request: async <T>(_verb: string, params?: unknown) => {
				responses.push(params);
				return {} as T;
			},
		},
		{ channels: { fetch: async () => ({ send: async () => ({ startThread: async () => ({ id: "child" }) }) }) } },
	);
	handler({
		requestId: "request",
		parent: { platform: "discord", kind: "channel", conversationId: "parent" },
		name: "pane",
	});
	for (let index = 0; index < 100 && responses.length === 0; index++) await Bun.sleep(1);
	expect(responses).toEqual([{ requestId: "request", threadId: "child" }]);
	handler({
		requestId: "invalid",
		parent: { platform: "discord", kind: "dm", conversationId: "dm", peerId: "peer" },
		name: "pane",
	});
	for (let index = 0; index < 100 && responses.length < 2; index++) await Bun.sleep(1);
	expect(responses[1]).toMatchObject({ requestId: "invalid", error: "invalid pane parent" });
	off();
	expect(unsubscribed).toBe(true);
});

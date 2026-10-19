import { expect, test } from "bun:test";
import {
	type FirstmateApproval,
	type FirstmateDecisionBinding,
	type Frame,
	PROFILE_VERSION,
	type WorkFirstmateRetireParams,
	type WorkFirstmateRetireResult,
	type WorkInspectParams,
	type WorkInspectResult,
	type WorkRetireParams,
	type WorkRetireResult,
} from "@gajae-gateway/protocol";
import { GajaewayClient } from "../src/client";

type RequestFrame = Extract<Frame, { type: "request" }>;

async function withResponses(
	responses: readonly unknown[],
	body: (client: GajaewayClient, requests: RequestFrame[]) => Promise<void>,
): Promise<void> {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	const requests: RequestFrame[] = [];
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write(data) {
				const frame: Frame = JSON.parse(String(data));
				if (frame.type !== "hello" && frame.type !== "request") {
					throw new Error(`unexpected client frame: ${frame.type}`);
				}
				const reply =
					frame.type === "hello"
						? {
								v: PROFILE_VERSION,
								type: "negotiated",
								payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
							}
						: {
								v: PROFILE_VERSION,
								type: "response",
								id: frame.id,
								result: responses[requests.push(frame) - 1],
							};
				controller.enqueue(new TextEncoder().encode(`${JSON.stringify(reply)}\n`));
			},
		},
	});
	try {
		await body(client, requests);
		expect(requests).toHaveLength(responses.length);
	} finally {
		controller.close();
		await client.close();
	}
}

const binding: FirstmateDecisionBinding = {
	name: "firstmate-wire-sdk-20261014",
	identity: {
		taskId: "task-original",
		jobId: "job-original",
		opRef: "op-original",
		ordinal: 1,
		sessionId: "session-original",
		epoch: 2,
	},
	ownerId: "owner-original",
	messageId: "message-original",
	thread: {
		platform: "discord",
		kind: "thread",
		conversationId: "thread-original",
		parentId: "channel-original",
	},
	mandateId: "mandate-original",
	scope: "release-session-only",
};

const firstmate: WorkFirstmateRetireParams = {
	firstmate: {
		...binding,
		approvalId: "approval-exact-not-latest",
		action: "release_session_preserve_work",
	},
};

test("SDK retirement union forwards exact native and FirstMate envelopes and preserves held/release results", async () => {
	const native: WorkRetireParams = { name: "native-lane", force: true };
	const nativeResult: WorkRetireResult = {
		retired: true,
		sessionKey: "native-session-key",
		sessionId: "native-session",
		closed: true,
		forced: true,
	};
	const held: WorkFirstmateRetireResult = {
		retired: false,
		held: true,
		reason: "report_steer_acceptance_unproven",
		obligations: [{ id: "report-original", reason: "report_steer_acceptance_unproven" }],
	};
	const released: WorkFirstmateRetireResult = {
		retired: true,
		release: {
			releaseId: "release-original",
			request: firstmate.firstmate,
			requestedAt: "2026-10-14T16:00:00Z",
			releasedAt: "2026-10-14T16:01:00Z",
			sessionKey: "work:firstmate-wire-sdk-20261014",
			preserved: true,
		},
	};
	await withResponses([nativeResult, held, released], async (client, requests) => {
		const retire = (
			params: WorkRetireParams | WorkFirstmateRetireParams,
		): Promise<WorkRetireResult | WorkFirstmateRetireResult> => client.workRetire(params);
		expect(await retire(native)).toEqual(nativeResult);
		const heldResult = await retire(firstmate);
		expect(heldResult).toEqual(held);
		expect(heldResult.retired).toBe(false);
		expect(heldResult).not.toHaveProperty("delivered");
		expect(heldResult).not.toHaveProperty("release");
		expect(await retire(firstmate)).toEqual(released);
		expect(requests.map(({ verb, params }) => ({ verb, params }))).toEqual([
			{ verb: "work.retire", params: native },
			{ verb: "work.retire", params: firstmate },
			{ verb: "work.retire", params: firstmate },
		]);
	});
});

test("SDK inspection selects approval only by explicit approvalId and never fills in latest authority", async () => {
	const params: WorkInspectParams = {
		name: binding.name,
		taskId: binding.identity.taskId,
		opRef: binding.identity.opRef,
	};
	const approval: FirstmateApproval = {
		...binding,
		approvalId: "approval-explicit-older",
		decision: "acknowledge_unavailable",
		evidence: { completeness: "unavailable", reasons: ["retained_original_unavailable"] },
		recordedAt: "2026-10-14T15:00:00Z",
	};
	const unselected: WorkInspectResult = {
		identity: null,
		lifecycle: null,
		evidence: { completeness: "unavailable" },
		receipts: { approval: null, release: null },
		execution: "not_evaluated",
		reasons: ["retained_original_unavailable"],
	};
	const selected: WorkInspectResult = {
		...unselected,
		identity: {
			...binding.identity,
			sessionKey: "work:firstmate-wire-sdk-20261014",
		},
		receipts: { approval, release: null },
	};
	const explicit: WorkInspectParams = { ...params, approvalId: approval.approvalId, native: true };
	await withResponses([unselected, selected], async (client, requests) => {
		expect(await client.workInspect(params)).toEqual(unselected);
		expect(requests[0]?.params).not.toHaveProperty("approvalId");
		expect(await client.workInspect(explicit)).toEqual(selected);
		expect(requests.map(({ verb, params }) => ({ verb, params }))).toEqual([
			{ verb: "work.inspect", params },
			{ verb: "work.inspect", params: explicit },
		]);
	});
});

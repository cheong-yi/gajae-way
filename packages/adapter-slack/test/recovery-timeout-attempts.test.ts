import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProtocolError } from "@gajae-gateway/protocol";
import { type SlackHistoryPage, SlackWebApi } from "../src/api";
import { type GatewayClientLike, ReconnectingGateway, startSlackAdapter } from "../src/main";
import {
	classifyRecoveryFailure,
	EMPTY_RECOVERY_STATE,
	loadRecoveryCursors,
	RECOVERY_MAX_ATTEMPTS,
	type RecoveryCursorState,
	recordAttempt,
} from "../src/recovery";
import type { WebSocketLike } from "../src/socket";

const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

const originalTs = "1700000000.123456";
const originalId = `C1:${originalTs}`;
// The exact error the SDK's request timer raises when a deadline expires with no
// response (packages/sdk/src/client.ts): a silent link, not a payload refusal.
const deadline = () => new ProtocolError("verb_failed", "request timed out after 40ms");

describe("Recovery failure classification", () => {
	test("request deadline errors are retryable, never terminal", () => {
		expect(classifyRecoveryFailure(deadline())).toBe("retryable");
	});

	test("explicit invalid_params and payload_too_large refusals are terminal", () => {
		expect(classifyRecoveryFailure(Object.assign(new Error("refused"), { code: "invalid_params" }))).toBe(
			"terminal-message",
		);
		expect(classifyRecoveryFailure(Object.assign(new Error("too big"), { code: "payload_too_large" }))).toBe(
			"terminal-message",
		);
	});

	test("link and close errors remain retryable", () => {
		expect(classifyRecoveryFailure(new Error("gateway is not connected"))).toBe("retryable");
		expect(classifyRecoveryFailure(new Error("client closed"))).toBe("retryable");
		expect(classifyRecoveryFailure(new Error("connection closed"))).toBe("retryable");
	});

	test("unrecognized failures stay write-path-unknown", () => {
		expect(classifyRecoveryFailure(new Error("boom"))).toBe("write-path-unknown");
	});
});

describe("Recovery attempt budget accounting", () => {
	test("repeated deadline failures never spend the terminal budget", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS + 2; i++) {
			const result = recordAttempt(
				state,
				"slack-msg-1",
				"C123",
				classifyRecoveryFailure(deadline()),
				"request timed out after 40ms",
				Date.now(),
			);
			state = result.state;
			expect(result.exhausted).toBe(false);
		}
		expect(state.attempts["slack-msg-1"]?.attempts).toBe(0);
		expect(state.deadLetters).toEqual([]);
	});

	test("a link outage in between never increments a recorded terminal count", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;
		state = recordAttempt(state, "slack-msg-2", "C123", "terminal-message", "invalid_params", Date.now()).state;
		expect(state.attempts["slack-msg-2"]?.attempts).toBe(1);
		state = recordAttempt(state, "slack-msg-2", "C123", "retryable", "offline", Date.now()).state;
		expect(state.attempts["slack-msg-2"]?.attempts).toBe(1);
	});

	test("explicit payload refusals exhaust exactly at RECOVERY_MAX_ATTEMPTS", () => {
		let state: RecoveryCursorState = EMPTY_RECOVERY_STATE;
		let exhausted = false;
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS; i++) {
			const result = recordAttempt(state, "slack-msg-3", "C123", "terminal-message", "payload_too_large", Date.now());
			state = result.state;
			exhausted = result.exhausted;
		}
		expect(state.attempts["slack-msg-3"]?.attempts).toBe(RECOVERY_MAX_ATTEMPTS);
		expect(exhausted).toBe(true);
	});
});

class Client implements GatewayClientLike {
	readonly calls: Array<{ verb: string; params: unknown }> = [];
	failure?: Error;
	engaged = false;
	async request<T>(verb: string, params?: unknown): Promise<T> {
		this.calls.push({ verb, params });
		if (this.failure) throw this.failure;
		return { engaged: this.engaged } as T;
	}
	onChatMessage() {
		return () => {};
	}
}

class Api extends SlackWebApi {
	historyMessages: Record<string, unknown>[] = [];
	readonly historyCalls: string[] = [];
	constructor() {
		super("unused", async () => {
			throw new Error("Slack test must not fetch");
		});
	}
	override async authTest() {
		return { user_id: "UBOT", bot_id: "B1", user: "bot", team_id: "T1", team: "Workspace" };
	}
	override async usersInfo(id: string) {
		return { id, name: id };
	}
	override async conversationsInfo(id: string) {
		return { id, name: id };
	}
	override async connectionsOpen() {
		return { url: "wss://slack.test" };
	}
	override async conversationsHistory(channel: string) {
		this.historyCalls.push(channel);
		return { messages: this.historyMessages, has_more: false };
	}
	override async conversationsReplies(): Promise<SlackHistoryPage> {
		return { messages: [], has_more: false };
	}
	override async postMessage(channel: string, text: string) {
		return { channel, ts: "2.0", text };
	}
	override async addReaction() {}
}

class Socket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	send() {}
	close() {}
}

/** Same composition the adapter suite drives: production startSlackAdapter + cursor store. */
async function fixture() {
	// Same guard as the B7 reproduction harness, scoped per fixture and restored on
	// cleanup: no adapter built here can ever open a real gateway socket.
	const connect = spyOn(ReconnectingGateway.prototype, "connect").mockImplementation(async () => {});
	cleanups.push(() => connect.mockRestore());
	const home = await mkdtemp(join(tmpdir(), "slack-timeout-recovery-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const api = new Api();
	const recoveryCursorPath = join(home, "cursor.json");
	const adapter = await startSlackAdapter(
		{
			botToken: "xoxb-test",
			appToken: "xapp-test",
			botTokenFile: "bot",
			appTokenFile: "app",
			configPath: "test",
			gatewaySocket: join(home, "absent.sock"),
			channels: { C1: { engagement: "open" } },
		},
		{
			api,
			recoveryCursorPath,
			now: () => 1_700_000_100_000,
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
		},
	);
	cleanups.push(() => {
		adapter.socket.stop();
		adapter.recovery.stop();
		adapter.gateway.stop();
	});
	// Deterministic hand-driven passes: no scheduler, no auto-recovery on connect.
	adapter.recovery.stop();
	await adapter.recovery.idle();
	adapter.gateway.onConnected = undefined;
	const client = new Client();
	adapter.gateway.adoptClient(client);
	return {
		api,
		client,
		gateway: adapter.gateway,
		recoveryCursorPath,
		recoverMissedMessages: adapter.recoverMissedMessages,
		/**
		 * Every failed send detaches the link (production scheduleReconnect), so each
		 * pass re-adopts the client first, exactly like a reconnect between passes.
		 */
		pass: async (failure?: Error) => {
			client.failure = failure;
			adapter.gateway.adoptClient(client);
			return adapter.recoverMissedMessages();
		},
	};
}

describe("Composed production recovery regression", () => {
	test("repeated request deadlines never dead-letter or advance the cursor; restoration admits the original", async () => {
		const f = await fixture();
		f.api.historyMessages = [{ type: "message", user: "U1", ts: originalTs, text: "valid unchanged payload" }];
		// More passes than the terminal budget: the old defect exhausted it at three.
		for (let i = 0; i < RECOVERY_MAX_ATTEMPTS + 1; i++) expect(await f.pass(deadline())).toBe(false);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.attempts[originalId]).toMatchObject({ attempts: 0, classification: "retryable" });
		expect(cursors.deadLetters).toEqual([]);
		expect(cursors.recoveredThrough.C1).toBeUndefined();

		// Link restored: the same original id is retried and admitted.
		expect(await f.pass()).toBe(true);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe(originalTs);
		expect(cursors.attempts[originalId]).toBeUndefined();
		expect(cursors.deadLetters).toEqual([]);
		const sends = f.client.calls.filter((call) => call.verb === "chat.send");
		// One send per deadline pass plus the restored admission.
		expect(sends).toHaveLength(RECOVERY_MAX_ATTEMPTS + 2);
		expect(sends.every((call) => (call.params as { messageId: string }).messageId === originalId)).toBe(true);
	});

	test("an already-admitted message deduplicates through the real gateway client, not a test-side map", async () => {
		const f = await fixture();
		// Seed the admission through the actual gateway client: its own admission
		// memory records the ack; no live ingress runs and no cursor moves.
		const seeded = await f.gateway.requestRecovered(
			originalId,
			{ platform: "slack", kind: "channel", conversationId: "C1" },
			"hello",
			{ mentioned: false, group: true, authorId: "U1" },
		);
		expect(seeded.verdict).toBe("acked");
		const sends = () => f.client.calls.filter((call) => call.verb === "chat.send");
		expect(sends()).toHaveLength(1);
		expect((sends()[0].params as { messageId: string }).messageId).toBe(originalId);
		expect((await loadRecoveryCursors(f.recoveryCursorPath)).recoveredThrough).toEqual({});

		// Recovery meets the same id from history. The cursor starts empty, so only an
		// actual delivered/duplicate verdict can advance it — the id cannot have been
		// filtered out by the fetch window — and the unchanged send count proves the
		// verdict came back "duplicate" from the gateway client's admission memory.
		f.api.historyMessages = [{ type: "message", user: "U1", ts: originalTs, text: "hello" }];
		expect(await f.recoverMissedMessages()).toBe(true);
		expect(sends()).toHaveLength(1);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe(originalTs);
		expect(cursors.attempts).toEqual({});
		expect(cursors.deadLetters).toEqual([]);

		// The closed window keeps it that way: a later pass fetches nothing new.
		expect(await f.recoverMissedMessages()).toBe(true);
		expect(sends()).toHaveLength(1);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe(originalTs);
	});

	test("explicit invalid_params and payload_too_large each spend the terminal budget and dead-letter", async () => {
		const f = await fixture();
		const refusal = (code: string) => Object.assign(new Error(`fixture refusal ${code}`), { code });

		f.api.historyMessages = [{ type: "message", user: "U1", ts: originalTs, text: "poison-a" }];
		const invalid = refusal("invalid_params");
		expect(await f.pass(invalid)).toBe(false);
		expect(await f.pass(invalid)).toBe(false);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.attempts[originalId]?.attempts).toBe(2);
		expect(cursors.deadLetters).toEqual([]);
		expect(cursors.recoveredThrough.C1).toBeUndefined();
		expect(await f.pass(invalid)).toBe(true);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.deadLetters).toHaveLength(1);
		expect(cursors.deadLetters[0]).toMatchObject({
			messageId: originalId,
			classification: "terminal-message",
			attempts: RECOVERY_MAX_ATTEMPTS,
		});
		expect(cursors.recoveredThrough.C1).toBe(originalTs);

		// A second poison message in the same channel gets its own full budget.
		const laterTs = "1700000001.123456";
		f.api.historyMessages = [{ type: "message", user: "U1", ts: laterTs, text: "poison-b" }];
		const oversized = refusal("payload_too_large");
		expect(await f.pass(oversized)).toBe(false);
		expect(await f.pass(oversized)).toBe(false);
		expect(await f.pass(oversized)).toBe(true);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.deadLetters).toHaveLength(2);
		expect(cursors.deadLetters[1]).toMatchObject({
			messageId: `C1:${laterTs}`,
			classification: "terminal-message",
			attempts: RECOVERY_MAX_ATTEMPTS,
		});
		expect(cursors.deadLetterDigest.C1?.count).toBe(2);
		expect(cursors.recoveredThrough.C1).toBe(laterTs);
	});
});

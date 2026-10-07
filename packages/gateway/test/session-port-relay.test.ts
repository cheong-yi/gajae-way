// Issue #316: session-scoped controls and queries ride the caller's live
// `serve --stdio` relay instead of spawning `gjc sdk session raw ...`. A relay
// transport failure falls back to the CLI exactly once; a relay refusal is the
// host's answer and never reaches the CLI.
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CliResult, type CliRunner, TranscriptIncompleteError } from "@gajae-gateway/subsession";
import { originKey } from "@gajae-gateway/protocol";
import { LaneGovernor } from "../src/orchestrator/lane-governor";
import { BrokerSessionPort, isSessionBusy } from "../src/orchestrator/session-port";
import { isDefinitiveSteerRejection } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { GatewayDatabase } from "../src/store/db";
import {
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	type ScriptedRelayReply,
	type ScriptedRelayRequest,
	ScriptedSessionPort,
	scriptedRelay,
} from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

type Mode = "answer" | "tear" | "refuse";

async function fixture(options: {
	readonly mode: Mode;
	readonly originKey?: string;
	readonly sessionId?: string;
	readonly relayReply: (request: ScriptedRelayRequest) => Record<string, unknown>;
	readonly cliReply: (args: readonly string[]) => CliResult;
}) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-relay-control-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const sessionId = options.sessionId ?? "sdk-1";
	await createOwnedSessionFixture(database, authority, { sessionId, repo, originKey: options.originKey ?? "relay", epoch: 0 });
	const cli: string[][] = [];
	const run: CliRunner = async (args) => {
		cli.push([...args]);
		return options.cliReply(args);
	};
	// A torn relay never answers: the request times out (a transport failure).
	const relay = scriptedRelay((request) =>
		options.mode === "tear"
			? new Promise<ScriptedRelayReply>(() => {})
			: options.mode === "refuse"
				? { ok: false, error: { code: "invalid_params", message: "refused by host" } }
				: options.relayReply(request) as ScriptedRelayReply,
	);
	const tailRunner = new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 20 });
	const sleeps: number[] = [];
	const port = new BrokerSessionPort({
		database, authority, cli: run, instanceId: "relay-1", tailRunner,
		sleep: async (ms) => {
			sleeps.push(ms);
			throw new Error("ambiguous relay evidence must not enter busy retry");
		},
	});
	const handle = await port.attachTail({ sessionId, brokerGeneration: 0, repo });
	return { port, repo, relay, handle, cli, sleeps, authority };
}

const ok = (result: unknown): CliResult => ({ exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" });
const page = (value: Record<string, unknown>): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ type: "query_response", ok: true, page: value }),
	stderr: "",
});

type Case = {
	readonly name: string;
	readonly op: string;
	readonly relayReply: ScriptedRelayReply;
	readonly cliReply: CliResult;
	readonly call: (f: Awaited<ReturnType<typeof fixture>>) => Promise<unknown>;
	readonly expected: unknown;
};

const turnResult = {
	kind: "prompt",
	clientRef: "gw-out-1",
	status: "terminal_ok",
	terminalAt: 2_000,
	receiptState: "present",
	content: { version: 1, type: "text", text: "body", byteLength: 4, truncated: false },
};

const cases: readonly Case[] = [
	{
		name: "setModel (model.set)",
		op: "model.set",
		relayReply: { ok: true, result: { changed: true } },
		cliReply: ok({ changed: true }),
		call: ({ port, repo, handle }) => port.setModel({ sessionId: "sdk-1", repo, selection: "gpt-x", relay: handle }),
		expected: { changed: true },
	},
	{
		name: "setModel (model.profile.set, bare boolean receipt)",
		op: "model.profile.set",
		relayReply: { ok: true, result: false },
		cliReply: ok(false),
		call: ({ port, repo, handle }) =>
			port.setModel({ sessionId: "sdk-1", repo, selection: { preset: "heavy" }, relay: handle }),
		expected: { changed: false },
	},
	{
		name: "setServiceTier",
		op: "service_tier.set",
		relayReply: { ok: true, result: { changed: true } },
		cliReply: ok({ changed: true }),
		call: ({ port, repo, handle }) =>
			port.setServiceTier({ sessionId: "sdk-1", repo, tier: "priority", relay: handle }),
		expected: { changed: true },
	},
	{
		name: "fetchWorkerOutput",
		op: "turn.result",
		relayReply: { ok: true, result: turnResult },
		cliReply: ok(turnResult),
		call: ({ port, repo, handle }) =>
			port.fetchWorkerOutput({ sessionId: "sdk-1", repo, opRef: "gw-out-1", notBeforeMs: 1_000, relay: handle }),
		expected: expect.objectContaining({ status: "proven", text: "body" }),
	},
	{
		name: "queueEmpty",
		op: "queue.messages.list",
		relayReply: { ok: true, page: { items: [], complete: true } },
		cliReply: page({ items: [], complete: true }),
		call: ({ port, repo, handle }) => port.queueEmpty({ sessionId: "sdk-1", repo, relay: handle }),
		expected: true,
	},
	{
		name: "fetchLastAssistant",
		op: "session.last_assistant",
		relayReply: { ok: true, page: { items: ["last"], complete: true } },
		cliReply: page({ items: ["last"], complete: true }),
		call: ({ port, repo, handle }) => port.fetchLastAssistant({ sessionId: "sdk-1", repo, relay: handle }),
		expected: { text: "last", pages: 1, complete: true },
	},
	{
		name: "fetchAssistantSince",
		op: "transcript.list",
		relayReply: {
			ok: true,
			page: { items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "since" }], complete: true },
		},
		cliReply: page({
			items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "since" }],
			complete: true,
		}),
		call: ({ port, repo, handle }) =>
			port.fetchAssistantSince({ sessionId: "sdk-1", repo, notBeforeMs: 4_000, relay: handle }),
		expected: { text: "since", pages: 1, complete: true },
	},
];

for (const entry of cases) {
	test(`${entry.name}: a live relay answers and no CLI runs`, async () => {
		const f = await fixture({ mode: "answer", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		expect(await entry.call(f)).toEqual(entry.expected);
		expect(f.relay.requests.map((request) => request.operation)).toEqual([entry.op]);
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});

	test(`${entry.name}: a relay transport failure falls back to the CLI exactly once`, async () => {
		const f = await fixture({ mode: "tear", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		expect(await entry.call(f)).toEqual(entry.expected);
		expect(f.relay.requests.map((request) => request.operation)).toEqual([entry.op]);
		expect(f.cli).toHaveLength(1);
		expect(f.cli[0]).toEqual(expect.arrayContaining(["sdk", "session", "raw", "sdk-1", entry.op]));
		await f.handle.close();
	});

	test(`${entry.name}: a relay refusal is surfaced and the CLI is not called`, async () => {
		const f = await fixture({ mode: "refuse", relayReply: () => entry.relayReply, cliReply: () => entry.cliReply });
		const outcome = await entry.call(f).then(
			(value) => ({ value }),
			(error: unknown) => ({ error }),
		);
		if (entry.op === "turn.result") {
			// The worker-output contract reports refusals as data, never as a throw.
			expect(outcome).toEqual({ value: { status: "absent", code: "transport_error" } });
		} else if (entry.op === "queue.messages.list") {
			// A refused queue read is never proof the queue is empty.
			expect(outcome).toEqual({ value: false });
		} else {
			expect("error" in outcome && String(outcome.error)).toContain("invalid_params");
		}
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});
}

test("without a relay handle the CLI stays the transport and no relay is opened", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({ ok: true, result: { changed: true } }),
		cliReply: () => ok({ changed: true }),
	});
	const spawned = f.relay.streams.length;
	expect(await f.port.setModel({ sessionId: "sdk-1", repo: f.repo, selection: "gpt-x" })).toEqual({ changed: true });
	expect(f.relay.streams).toHaveLength(spawned);
	expect(f.relay.requests).toEqual([]);
	expect(f.cli[0]).toEqual(expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.set"]));
	await f.handle.close();
});

test("transcript.list pages over the relay with the host's continuationCursor as the top-level cursor", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: (request) =>
			request.cursor === undefined
				? {
						ok: true,
						page: {
							items: [{ role: "assistant", ts: new Date(5_000).toISOString(), body: "first" }],
							complete: false,
							continuationCursor: "c-1",
						},
					}
				: {
						ok: true,
						page: { items: [{ role: "assistant", ts: new Date(6_000).toISOString(), body: "second" }], complete: true },
					},
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	expect(
		await f.port.fetchAssistantSince({ sessionId: "sdk-1", repo: f.repo, notBeforeMs: 4_000, relay: f.handle }),
	).toEqual({
		text: "second",
		pages: 2,
		complete: true,
	});
	expect(f.relay.requests.map((request) => request.cursor)).toEqual([undefined, "c-1"]);
	const written = f.relay.streams[0]!.written.filter((frame) => frame.type === "query_request");
	expect(written[1]).toMatchObject({ query: "transcript.list", input: {}, cursor: "c-1" });
	await f.handle.close();
});

test("transcript.list over the relay rejects a repeated cursor and an incomplete page without one", async () => {
	for (const second of [{ complete: false, continuationCursor: "c-1" }, { complete: false }]) {
		const f = await fixture({
			mode: "answer",
			relayReply: (request) =>
				request.cursor === undefined
					? { ok: true, page: { items: [], complete: false, continuationCursor: "c-1" } }
					: { ok: true, page: { items: [], ...second } },
			cliReply: () => {
				throw new Error("CLI must not run");
			},
		});
		const error = await f.port
			.fetchAssistantSince({ sessionId: "sdk-1", repo: f.repo, notBeforeMs: 0, relay: f.handle })
			.catch((failure: unknown) => failure);
		expect(error).toBeInstanceOf(TranscriptIncompleteError);
		expect(f.cli).toEqual([]);
		await f.handle.close();
		database?.close();
		database = undefined;
		await rm(home, { recursive: true, force: true });
		home = "";
	}
});

test("session.last_assistant pages over the relay, joining chunks until complete", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: (request) =>
			request.cursor === undefined
				? { ok: true, page: { items: ["hel"], complete: false, continuationCursor: "p-2" } }
				: { ok: true, page: { items: ["lo"], complete: true } },
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	expect(await f.port.fetchLastAssistant({ sessionId: "sdk-1", repo: f.repo, relay: f.handle })).toEqual({
		text: "hello",
		pages: 2,
		complete: true,
	});
	expect(f.relay.requests.map((request) => request.cursor)).toEqual([undefined, "p-2"]);
	await f.handle.close();
});

test("session.last_assistant over the relay rejects an incomplete page without a continuation cursor", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({ ok: true, page: { items: ["partial"], complete: false } }),
		cliReply: () => {
			throw new Error("CLI must not run");
		},
	});
	const error = await f.port
		.fetchLastAssistant({ sessionId: "sdk-1", repo: f.repo, relay: f.handle })
		.catch((failure: unknown) => failure);
	expect(error).toBeInstanceOf(TranscriptIncompleteError);
	expect(f.cli).toEqual([]);
	await f.handle.close();
});

// These go through ScriptedRelayStream JSON serialization and TailRunner's
// real line decoder, not a pre-projected TailHandle.control fake.
const promptReceipt = { accepted: true, clientRef: "original", commandId: "command", turnId: "turn" };
const steerReceipt = {
	clientRef: "original", commandId: "command", turnId: "turn", status: "accepted", acceptedAt: 1_700_000_000_000,
};
for (const operation of ["turn.prompt", "turn.steer"] as const) {
	const receipt = operation === "turn.prompt" ? promptReceipt : steerReceipt;
	for (const [name, reply] of [
		["missing ok", { error: { code: "busy" } }],
		["missing ok with result", { result: receipt }],
		["nonboolean ok", { ok: "false", error: { code: "busy" } }],
		["truthy ok", { ok: 1, result: receipt }],
		["malformed error", { ok: true, result: receipt, error: "busy" }],
		["null error", { ok: true, result: receipt, error: null }],
		["contradictory error", { ok: true, result: receipt, error: { code: "busy" } }],
		["false with result", { ok: false, result: receipt, error: { code: "busy" } }],
		["unknown certainty", { ok: false, error: { code: "busy", outcomeCertainty: "unknown" } }],
		["malformed certainty", { ok: false, error: { code: "busy", outcomeCertainty: false } }],
		["refusal with foreign reference", { ok: false, error: { code: "busy", clientRef: "foreign" } }],
		["malformed success page", { ok: true, result: receipt, page: "partial" }],
		["malformed refusal page", { ok: false, error: { code: "busy" }, page: false }],
		["success truncation", { ok: true, result: receipt, truncated: true }],
		["refusal validity", { ok: false, error: { code: "busy" }, valid: false }],
		["error truncation", { ok: false, error: { code: "busy", truncated: true } }],
		["wrong operation", { ok: true, result: receipt, operation: "other.control" }],
	] as const) {
		test(`${operation}: raw ${name} remains uncertain without retry or definitive disposition`, async () => {
			const f = await fixture({
				mode: "answer", relayReply: () => reply,
				cliReply: () => { throw new Error("CLI must not run"); },
			});
			const target = { sessionId: "sdk-1", repo: f.repo, relay: f.handle, text: "work" };
			const failure = await (
				operation === "turn.prompt"
					? f.port.send({ ...target, opRef: "original" })
					: f.port.steer({ ...target, clientRef: "original" })
			).catch((error: unknown) => error);
			expect(failure).toMatchObject({ details: { outcomeCertainty: "unknown" } });
			expect(isSessionBusy(failure)).toBe(false);
			expect(isDefinitiveSteerRejection(failure)).toBe(false);
			expect(f.relay.requests).toEqual([{
				type: "control_request", operation, input: { text: "work", clientRef: "original" },
			}]);
			expect(f.sleeps).toEqual([]);
			expect(f.cli).toEqual([]);
			await f.handle.close();
		});
	}

	test(`${operation}: exact accepted receipt and legitimate routing metadata survive raw decoding`, async () => {
		const f = await fixture({
			mode: "answer", relayReply: () => ({ ok: true, operation, result: receipt }),
			cliReply: () => { throw new Error("CLI must not run"); },
		});
		const target = { sessionId: "sdk-1", repo: f.repo, relay: f.handle, text: "work" };
		if (operation === "turn.prompt") {
			expect(await f.port.send({ ...target, opRef: "original" })).toMatchObject({
				operationRef: "original", commandId: "command", turnId: "turn",
			});
		} else {
			expect(await f.port.steer({ ...target, clientRef: "original" })).toBeUndefined();
		}
		expect(f.relay.requests).toHaveLength(1);
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});

	test(`${operation}: exact refusal retains not-applied evidence`, async () => {
		const f = await fixture({
			mode: "answer",
			relayReply: () => ({ ok: false, error: { code: "busy", message: "occupied", outcomeCertainty: "not-applied" } }),
			cliReply: () => { throw new Error("CLI must not run"); },
		});
		const target = { sessionId: "sdk-1", repo: f.repo, relay: f.handle, text: "work" };
		const failure = await (
			operation === "turn.prompt"
				? f.port.send({ ...target, opRef: "original", busyWaitMs: 0 })
				: f.port.steer({ ...target, clientRef: "original" })
		).catch((error: unknown) => error);
		expect(failure).toMatchObject({ details: { code: "busy", outcomeCertainty: "not-applied" } });
		expect(isDefinitiveSteerRejection(failure)).toBe(operation === "turn.steer");
		expect(f.sleeps).toEqual([]);
		expect(f.cli).toEqual([]);
		await f.handle.close();
	});
}

test("raw query and ordinary control errors retain original certainty and evidence", async () => {
	const error = { code: "busy", outcomeCertainty: "unknown", clientRef: "original", detail: { valid: false } };
	const f = await fixture({
		mode: "answer", relayReply: () => ({ ok: false, error }),
		cliReply: () => { throw new Error("CLI must not run"); },
	});
	expect(await f.handle.query("turn.result", { clientRef: "original" })).toMatchObject({ ok: false, error });
	const failure = await f.port.setModel({
		sessionId: "sdk-1", repo: f.repo, relay: f.handle, selection: "model",
	}).catch((failure: unknown) => failure);
	expect(failure).toMatchObject({ details: error });
	const queryFailure = await f.port.fetchLastAssistant({
		sessionId: "sdk-1", repo: f.repo, relay: f.handle,
	}).catch((failure: unknown) => failure);
	expect(queryFailure).toMatchObject({ details: error });
	expect(f.relay.requests.map((request) => request.operation)).toEqual([
		"turn.result", "model.set", "session.last_assistant",
	]);
	expect(f.cli).toEqual([]);
	await f.handle.close();
});

test("a matching ID with the wrong raw response kind is not a control refusal or transport retry", async () => {
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({ type: "query_response", ok: false, error: { code: "busy" } }),
		cliReply: () => { throw new Error("CLI must not run"); },
	});
	const failure = await f.port.setModel({
		sessionId: "sdk-1", repo: f.repo, relay: f.handle, selection: "model",
	}).catch((error: unknown) => error);
	expect(String(failure)).toContain("relay response kind does not match");
	expect(isDefinitiveSteerRejection(failure)).toBe(false);
	expect(f.relay.requests).toHaveLength(1);
	expect(f.cli).toEqual([]);
	await f.handle.close();
});

test("original steer lookup accepts query metadata but not raw validity loss", async () => {
	let invalid = false;
	const f = await fixture({
		mode: "answer",
		relayReply: () => ({
			ok: true, query: "turn.steer_status", result: steerReceipt, ...(invalid ? { truncated: true } : {}),
		}),
		cliReply: () => { throw new Error("CLI must not run"); },
	});
	const target = { sessionId: "sdk-1", repo: f.repo, relay: f.handle, clientRef: "original" };
	expect(await f.port.lookupSteerStatus(target)).toMatchObject({ status: "accepted", clientRef: "original" });
	invalid = true;
	expect(await f.port.lookupSteerStatus(target)).toMatchObject({ status: "unavailable", code: "invalid_evidence" });
	expect(f.relay.requests.every((request) => request.type === "query_request")).toBe(true);
	expect(f.cli).toEqual([]);
	await f.handle.close();
});

for (const lookup of [false, true]) {
test(`raw unknown-certainty busy holds the original task control and blocks its successor (Q31=${lookup})`, async () => {
	const taskId = "b7654d21-6806-4fdc-89ef-e2f9c038e4f9";
	const name = `fm-${taskId}`;
	const sessionId = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
	const f = await fixture({
		mode: "answer",
		originKey: `work/task/${name}`,
		sessionId,
		relayReply: (request) => request.type === "query_request" ? ({
			ok: true,
			result: {
				clientRef: request.input.clientRef,
				status: "rejected",
				acceptedAt: 1_700_000_000_000,
				error: { code: "busy", message: "uncertain rejection", outcomeCertainty: "unknown" },
			},
		}) : ({ ok: false, error: { code: "busy", outcomeCertainty: "unknown" } }),
		cliReply: () => { throw new Error("CLI must not run"); },
	});
	await mkdir(f.repo, { recursive: true });
	const db = database!;
	const coordinator = { platform: "discord", kind: "channel", conversationId: "100", boundaryId: "1" } as const;
	const thread = { platform: "discord", kind: "thread", conversationId: "200", parentId: "100", boundaryId: "1" } as const;
	await createOwnedSessionFixture(db, f.authority, {
		sessionId: "persona-session", originKey: originKey(coordinator), epoch: 0, repo: f.repo,
	});
	// Only assignment/lifecycle is scripted. The original steering invocation
	// crosses BrokerSessionPort and the real stdio decoder on the owned session.
	class LookupPort extends ScriptedSessionPort {
		override async lookupSteerStatus(input: Parameters<BrokerSessionPort["lookupSteerStatus"]>[0]) {
			return lookup ? await f.port.lookupSteerStatus({ ...input, relay: f.handle }) : await super.lookupSteerStatus(input);
		}
	}
	const lifecycle = new LookupPort({
		onBind: () => sessionId,
		onSteer: (input) => f.port.steer({ ...input, relay: f.handle }),
	});
	const lanes = new LaneGovernor({ database: db, sessionPort: lifecycle, maxLanes: 4 });
	const manager = new WorkLaneManager({
		database: db, port: lifecycle, lanes, pollMs: 60_000, taskSurfaceAvailable: () => true,
	});
	const claimId = "29e0f8d7-b069-44bc-a747-5890b89ea174";
	const at = "2026-10-06T05:00:00.000Z";
	const context = {
		stableOrigin: coordinator,
		evidence: { principalId: "owner", origin: coordinator, eventId: "assignment", editId: null, evidenceAt: at, observedAt: at },
	};
	try {
		await manager.start({
			name, text: "Inspect evidence", cwd: f.repo, callerSessionId: "persona-session",
			task: { taskId, kind: "read_only", surface: { parentOrigin: coordinator } },
		}, context);
		await manager.threadClaim({ taskId, claimId });
		await manager.threadBind({ taskId, claimId, outcome: { kind: "bound", origin: thread } });
		const task = db.workTaskGet(taskId)!;
		for (const [eventId, text] of [["cli:first-control", "first-control"], ["cli:successor", "successor"]]) {
			await manager.steer({
				name, taskId, expectedOpRef: task.opRef, eventId, text,
			}, { ...context, evidence: { ...context.evidence, eventId } });
		}
		await manager.drainTaskControls(taskId);
		const controls = db.workControlList(taskId);
		expect(controls.map((control) => control.phase)).toEqual(["held", "pending"]);
		expect(controls[0]).toMatchObject({ reason: "steer_receipt_unresolved" });
		expect(controls[1]?.sendingAt).toBeNull();
		expect(f.relay.requests.filter((request) => request.type === "control_request")).toEqual([{
			type: "control_request", operation: "turn.steer",
			input: { text: "first-control", clientRef: controls[0]!.clientRef },
		}]);
		const queries = f.relay.requests.filter((request) => request.type === "query_request");
		if (lookup) {
			expect(queries.length).toBeGreaterThan(0);
			expect(queries.every((request) =>
				request.operation === "turn.steer_status" && request.input.clientRef === controls[0]!.clientRef,
			)).toBe(true);
			expect(controls[0]?.request.expectedOpRef).toBe(task.opRef);
			expect(db.workTaskGet(taskId)?.opRef).toBe(task.opRef);
		} else {
			expect(queries).toEqual([]);
		}
		expect(lifecycle.steers).toHaveLength(1);
		expect(f.sleeps).toEqual([]);
		expect(f.cli).toEqual([]);
	} finally {
		await manager.stop();
		await f.handle.close();
	}
});
}

for (const transport of ["cli", "relay"] as const) {
	test(`contradictory original output cannot create a task final or supplement through ${transport}`, async () => {
		const taskId = "b7654d21-6806-4fdc-89ef-e2f9c038e4f9";
		const name = `fm-${taskId}`;
		const sessionId = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
		let raw: Record<string, unknown> = {};
		let contradiction: Record<string, unknown> = { error: { code: "terminal_uncertain" } };
		const f = await fixture({
			mode: "answer", originKey: `work/task/${name}`, sessionId,
			relayReply: (request) => {
				expect(request.operation).toBe("turn.result");
				return raw;
			},
			cliReply: (args) => {
				expect(args[args.indexOf("--query") + 1]).toBe("turn.result");
				return { exitCode: 0, stderr: "", stdout: JSON.stringify(raw) };
			},
		});
		await mkdir(f.repo, { recursive: true });
		const db = database!;
		const coordinator = { platform: "discord", kind: "channel", conversationId: "100", boundaryId: "1" } as const;
		const thread = { platform: "discord", kind: "thread", conversationId: "200", parentId: "100", boundaryId: "1" } as const;
		await createOwnedSessionFixture(db, f.authority, {
			sessionId: "persona-session", originKey: originKey(coordinator), epoch: 0, repo: f.repo,
		});
		class OutputPort extends ScriptedSessionPort {
			override async fetchWorkerOutput(input: Parameters<BrokerSessionPort["fetchWorkerOutput"]>[0]) {
				const original = await super.fetchWorkerOutput(input);
				if (original.status !== "proven") throw new Error("fixture requires exact original terminal output");
				raw = {
					ok: true,
					result: {
						kind: "prompt", status: "terminal_ok", clientRef: input.opRef,
						commandId: original.provenance.commandId, turnId: original.provenance.turnId,
						terminalAt: original.provenance.terminalAt, receiptState: "present",
						content: { version: 1, type: "text", text: original.text, byteLength: original.provenance.byteLength, truncated: false },
					},
					...contradiction,
				};
				return await f.port.fetchWorkerOutput({ ...input, relay: transport === "relay" ? f.handle : undefined });
			}
		}
		const lifecycle = new OutputPort({ onBind: () => sessionId });
		const lanes = new LaneGovernor({ database: db, sessionPort: lifecycle, maxLanes: 4 });
		const manager = new WorkLaneManager({
			database: db, port: lifecycle, lanes, pollMs: 5, taskSurfaceAvailable: () => true,
		});
		const at = "2026-10-06T05:00:00.000Z";
		const context = {
			stableOrigin: coordinator,
			evidence: { principalId: "owner", origin: coordinator, eventId: "assignment", editId: null, evidenceAt: at, observedAt: at },
		};
		try {
			await manager.start({
				name, text: "Inspect evidence", cwd: f.repo, callerSessionId: "persona-session",
				task: { taskId, kind: "read_only", surface: { parentOrigin: coordinator } },
			}, context);
			const claimId = "29e0f8d7-b069-44bc-a747-5890b89ea174";
			await manager.threadClaim({ taskId, claimId });
			await manager.threadBind({ taskId, claimId, outcome: { kind: "bound", origin: thread } });
			const task = db.workTaskGet(taskId)!;
			lifecycle.complete(task.opRef, "Original retained answer");
			for (let poll = 0; poll < 200 && db.workAttemptGet(task.opRef)?.settledAt == null; poll++) await Bun.sleep(5);
			const before = db.workAttemptGet(task.opRef)!;
			expect(before.settledAt).not.toBeNull();
			expect(before.output.proof).toBeNull();
			expect(db.workTaskGet(taskId)?.obligationState).toBe("held");
			expect(db.workTaskSourceGet(`final-${taskId}-original`)).toBeUndefined();
			const held = db.workTaskGet(taskId);
			for (contradiction of [
				{ error: { code: "terminal_uncertain" } }, { truncated: true }, { valid: false },
				{ query: "runtime.jobs.list" }, { complete: false }, { outcomeCertainty: "unknown" },
			]) {
				expect(await manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "held" });
				expect(db.workTaskGet(taskId)).toEqual(held);
				expect(db.workAttemptGet(task.opRef)).toEqual(before);
				expect(db.workTaskSourceGet(`supplement-${taskId}-original`)).toBeUndefined();
			}
			contradiction = {};
			expect(await manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "reconciled" });
			expect(db.workTaskSourceGet(`supplement-${taskId}-original`)?.body).toBe("Original retained answer");
			expect(lifecycle.sends).toHaveLength(1);
			expect(lifecycle.resumes).toHaveLength(0);
			expect(lifecycle.steers).toHaveLength(0);
			expect(f.relay.requests.every((request) => request.type === "query_request")).toBe(true);
			if (transport === "relay") expect(f.cli).toEqual([]);
			else expect(f.relay.requests).toEqual([]);
		} finally {
			await manager.stop();
			await f.handle.close();
		}
	});
}

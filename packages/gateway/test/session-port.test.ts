import { Database } from "bun:sqlite";
import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CliRunner, GjcCliError } from "@gajae-gateway/subsession";
import { isDefinitiveSteerRejection } from "../src/orchestrator/persona-session";
import {
	BrokerSessionPort,
	isSessionBusy,
	ModelNotSelectedError,
	PromptNotSubmittedError,
	SessionRequestTimeoutError,
	type SessionSteerStatusResult,
} from "../src/orchestrator/session-port";
import { type TailHandle, TailRunner } from "../src/orchestrator/tail-runner";
import { BrokerAuthorityError, GatewayDatabase } from "../src/store/db";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	type ScriptedRelayReply,
	ScriptedSessionPort,
	scriptedRelay,
} from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;

test("opt-in fake ownership records only successful binds and refuses unknown saved sessions", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-fake-ownership-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const agentDir = join(home, "agent");
	const repo = join(home, "workspace");
	const fake = new ScriptedSessionPort({ onBind: ({ epoch }) => `owned-${epoch}` });
	const port = attachTestBrokerOwnership(database, fake, agentDir);
	expect(port).toBe(fake);
	fake.setSessionState("unknown-saved", { repo, live: false });
	await expect(port.resume({ sessionId: "unknown-saved", repo, originKey: "origin", epoch: 0 })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(fake.resumes).toHaveLength(0);
	await port.bind({ originKey: "origin", epoch: 0, repo });
	database.rebindEpoch("origin");
	await port.bind({ originKey: "origin", epoch: 1, repo });
	fake.setSessionState("owned-0", { live: false });
	await port.resume({ sessionId: "owned-0", repo, originKey: "origin", epoch: 0 });
	expect(database.getSessionRecord("origin")).toMatchObject({ sessionId: "owned-1", epoch: 1 });
	const authority = initializeTestBrokerAuthority(database, agentDir);
	expect(database.assertOwnedSession("owned-0", repo, authority)).toMatchObject({ originKey: "origin", epoch: 0 });
	const failing = attachTestBrokerOwnership(
		database,
		new ScriptedSessionPort({
			onBind: () => {
				throw new Error("fixture refused");
			},
		}),
		agentDir,
	);
	await expect(failing.bind({ originKey: "failed", epoch: 0, repo })).rejects.toThrow("fixture refused");
	expect(database.getSessionRecord("failed")).toBeUndefined();
});

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("a binding recorded under a symlinked workspace stays owned when the runtime asks with the resolved path", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-symlink-repo-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const agentDir = join(home, "agent");
	const real = join(home, "real-workspace");
	const link = join(home, "workspace");
	await mkdir(real);
	await symlink(real, link);
	const authority = initializeTestBrokerAuthority(database, agentDir);
	// Provenance written before the workspace was canonicalized names the link.
	await createOwnedSessionFixture(database, authority, {
		sessionId: "legacy-link",
		originKey: "origin",
		epoch: 0,
		repo: link,
	});

	expect(database.assertOwnedSession("legacy-link", real, authority)).toMatchObject({ originKey: "origin" });
	expect(database.assertOwnedSession("legacy-link", link, authority)).toMatchObject({ originKey: "origin" });

	// A different directory is still refused.
	const other = join(home, "other");
	await mkdir(other);
	const db = database;
	expect(() => db.assertOwnedSession("legacy-link", other, authority)).toThrow(BrokerAuthorityError);
});

test("failed-turn evidence comes from the owned shared session file without exposing provider text", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-failure-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const repo = join(home, "workspace");
	const agentDir = join(home, "agent");
	const authority = initializeTestBrokerAuthority(database, agentDir);
	const bucket = join(agentDir, "sessions", "bucket");
	await mkdir(repo);
	await mkdir(bucket, { recursive: true });
	const sessionId = "failed-session";
	await createOwnedSessionFixture(database, authority, { sessionId, originKey: "failure-evidence", epoch: 0, repo });
	const startedAtMs = Date.now();
	const rows = [
		{ type: "session", version: 5, id: sessionId, cwd: repo, timestamp: new Date(startedAtMs - 100).toISOString() },
		{
			type: "message",
			id: "user",
			parentId: null,
			timestamp: new Date(startedAtMs).toISOString(),
			message: { role: "user", timestamp: startedAtMs, content: [{ type: "text", text: "hello" }] },
		},
		{
			type: "message",
			id: "error",
			parentId: "user",
			timestamp: new Date(startedAtMs + 20).toISOString(),
			message: {
				role: "assistant",
				timestamp: startedAtMs + 1,
				content: [],
				stopReason: "error",
				errorStatus: 400,
				errorMessage: "400 Unknown parameter: 'input[1].status'.\nraw-http-request=/private/request.json",
			},
		},
	];
	await writeFile(join(bucket, `now_${sessionId}.jsonl`), `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`);
	const run: CliRunner = async () => {
		throw new Error("Failure evidence must not start an SDK operation");
	};
	const options = {
		database,
		authority,
		cli: run,
		instanceId: "evidence",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	};
	const port = new BrokerSessionPort(options);
	const input = { sessionId, repo, startedAtMs, terminalAtMs: startedAtMs + 30 };
	expect(await port.failedTurnEvidence(input)).toEqual({ reason: "unsupported_input_status" });
	await expect(port.failedTurnEvidence({ ...input, sessionId: "foreign-session" })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	await expect(port.failedTurnEvidence({ ...input, repo: join(home, "other-repo") })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
});

test("AC-K rendered SDK prompt is notice + blank line + task; broker SessionPort preserves op-ref, model, terminal status, and transcript", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("session.create"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
		if (args.includes("model.profile.set"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { changed: false, id: "gpt-heavy" } }),
				stderr: "",
			};
		if (args.includes("model.set"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { changed: true } }), stderr: "" };
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	// The turn is submitted and observed on the session's own relay: the prompt
	// goes down as turn.prompt, the terminal status comes back as turn.result.
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return {
				ok: true,
				result: { commandId: "cmd-1", turnId: "turn-1", accepted: true, clientRef: request.input.clientRef },
			};
		if (request.operation === "turn.result")
			return { ok: true, result: { kind: "prompt", status: "terminal_ok", clientRef: request.input.clientRef } };
		if (request.operation === "session.last_assistant")
			return { ok: true, page: { items: ["finished body"], complete: true } };
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const tailRunner = new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner,
	});
	port.setStallTimeoutMs(5_000);
	expect(tailRunner.stallTimeoutMs).toBe(5_000);
	const binding = await port.bind({
		originKey: "work/task/a",
		epoch: 0,
		repo: "/tmp/repo",
		codingRegister: true,
		model: { preset: "gpt-heavy" },
	});
	const profileReceipt = await port.setModel({
		sessionId: binding.sessionId,
		repo: "/tmp/repo",
		selection: { preset: "gpt-heavy" },
	});
	expect(profileReceipt).toEqual({ changed: false });
	const result = await port.request({
		sessionId: binding.sessionId,
		repo: "/tmp/repo",
		text: "implement it",
		systemPreamble: "trusted bootstrap",
		model: { preset: "coding" },
		opRef: "gw-work-1",
		pollMs: 0,
	});

	expect(binding.sessionId).toBe("sdk-1");
	expect(result.assistant.text).toBe("finished body");
	expect(calls[0]).toEqual(
		expect.arrayContaining(["sdk", "session", "raw", "global", "--op", "session.create", "--idempotency-key"]),
	);
	const create = calls.find((args) => args.includes("session.create"))!;
	const createInput = JSON.parse(create[create.indexOf("--json-input") + 1]!) as Record<string, unknown>;
	expect(createInput).toMatchObject({ cwd: "/tmp/repo", modelPreset: "gpt-heavy" });
	expect(binding.startupModelApplied).toBe(true);
	expect(result.receipt).toMatchObject({ operationRef: "gw-work-1", commandId: "cmd-1", turnId: "turn-1" });
	expect(relay.requests[0]).toEqual({
		type: "control_request",
		operation: "turn.prompt",
		input: { text: "trusted bootstrap\n\nimplement it", clientRef: "gw-work-1" },
	});
	expect(relay.requests[1]).toEqual({
		type: "query_request",
		operation: "turn.result",
		input: { kind: "prompt", clientRef: "gw-work-1" },
	});
	expect(calls.some((args) => args.includes("send") || args.includes("tail") || args.includes("status"))).toBe(false);
	expect(calls.find((args) => args.includes("model.profile.set"))).toEqual(
		expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.profile.set"]),
	);
	// The final read rides the request's own relay; no CLI query is spawned for it.
	expect(relay.requests[2]).toEqual({ type: "query_request", operation: "session.last_assistant", input: {} });
	expect(calls.some((args) => args.includes("session.last_assistant"))).toBe(false);
	// The relay is closed once the request settles.
	expect(relay.streams).toHaveLength(1);
	expect(relay.streams[0]!.closed).toBe(true);
});

for (const [label, model, expected] of [
	["default", undefined, {}],
	["explicit", "provider/model", { modelId: "provider/model" }],
	["preset", { preset: "selected-profile" }, { modelPreset: "selected-profile" }],
] as const) {
	test(`session creation preserves ${label} model semantics without post-create selection`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-default-bootstrap-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
		const calls: string[][] = [];
		const port = new BrokerSessionPort({
			database,
			authority,
			instanceId: "instance-default",
			tailRunner: new TailRunner({ stream: noRelay, repo: "/tmp/repo" }),
			cli: async (args) => {
				calls.push([...args]);
				if (args.includes("session.create"))
					return {
						exitCode: 0,
						stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-default" } }),
						stderr: "",
					};
				throw new Error(`unexpected bootstrap command ${args.join(" ")}`);
			},
		});
		const binding = await port.bind({ originKey: "default-origin", epoch: 0, repo: "/tmp/repo", model });
		const create = calls.find((args) => args.includes("session.create"))!;
		const input = JSON.parse(create[create.indexOf("--json-input") + 1]!);
		expect(input).toEqual({ cwd: "/tmp/repo", readinessTimeoutMs: 60_000, ...expected });
		expect(binding.startupModelApplied === true).toBe(model !== undefined);
		expect(calls.some((args) => args.includes("model.set") || args.includes("model.profile.set"))).toBe(false);
	});
}

test("broker SessionPort reuses the durable epoch binding and does not recreate a session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						session: {
							sessionId: "sdk-1",
							live: true,
							deleted: false,
							locator: { cwd: "/tmp/repo", worktreeRoot: "/tmp/repo" },
						},
					},
				}),
				stderr: "",
			};
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: "/tmp/repo" }),
	});
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
});

test("broker SessionPort rebinds a saved binding on an explicit session_unavailable envelope", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-public-error-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const calls: string[][] = [];
	const publicFailure = {
		ok: false,
		error: {
			code: "session_unavailable",
			message: "SDK session saved-1 is unavailable through the session Router.",
		},
	};
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect") && args.includes("saved-1"))
			return { exitCode: 1, stdout: JSON.stringify(publicFailure), stderr: "" };
		if (args.includes("session.create"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "fresh-1" } }), stderr: "" };
		if (args.includes("inspect") && args.includes("fresh-1"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId: "fresh-1", live: true } } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-1",
		repo,
		originKey: "public-session-gone",
		epoch: 0,
	});
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "public-error",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	await expect(port.bind({ originKey: "public-session-gone", epoch: 0, repo })).resolves.toMatchObject({
		sessionId: "fresh-1",
		epoch: 1,
	});
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
});

test("generic inspect errors preserve saved authority and do not trigger a replacement session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-generic-error-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return {
			exitCode: 1,
			stdout: JSON.stringify({
				ok: false,
				error: { code: "operation_failed", message: "The requested operation failed." },
			}),
			stderr: "",
		};
	};
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-1",
		repo,
		originKey: "generic-inspect-error",
		epoch: 0,
	});
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "generic-error",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	await expect(port.bind({ originKey: "generic-inspect-error", epoch: 0, repo })).resolves.toMatchObject({
		sessionId: "saved-1",
		epoch: 0,
	});
	expect(calls).toHaveLength(1);
	expect(database.getSessionRecord("generic-inspect-error")).toMatchObject({ sessionId: "saved-1", epoch: 0 });
});

test("structured nonzero session failures survive normalization at the global lifecycle route", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-envelope-error-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const publicFailure = {
		ok: false,
		error: {
			code: "session_unavailable",
			message: "SDK session saved-1 is unavailable through the session Router.",
		},
	};
	const calls: string[][] = [];
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: async (args) => {
			calls.push([...args]);
			return { exitCode: 1, stdout: JSON.stringify(publicFailure), stderr: "" };
		},
		instanceId: "envelope-error",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-1",
		repo,
		originKey: "global-close-error",
		epoch: 0,
	});
	await expect(port.close({ sessionId: "saved-1", repo })).rejects.toMatchObject({
		name: "GjcCliError",
		details: publicFailure.error,
	});
	expect(calls).toHaveLength(1);
	expect(calls[0]).toEqual(expect.arrayContaining(["raw", "global", "--op", "session.close"]));
});

test("broker SessionPort resumes saved dead authority through the SDK control before returning the same binding", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	let live = false;
	const repo = join(home, "workspace");
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { session: { sessionId: "saved-1", locator: { repo }, live, deleted: false } },
				}),
				stderr: "",
			};
		if (args.includes("session.resume")) {
			live = true;
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { resumed: true } }), stderr: "" };
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-1",
		repo,
		originKey: "discord/channel/c",
		epoch: 3,
	});
	await expect(port.resume({ sessionId: "saved-1", repo, originKey: "discord/channel/c", epoch: 3 })).resolves.toEqual({
		sessionId: "saved-1",
		repo,
		originKey: "discord/channel/c",
		epoch: 3,
	});
	expect(calls.filter((args) => args.includes("inspect"))).toHaveLength(2);
	expect(calls.find((args) => args.includes("session.resume"))).toEqual(
		expect.arrayContaining(["sdk", "session", "raw", "control", "saved-1", "--op", "session.resume"]),
	);
});

test("broker SessionPort preserves a structured client-ref conflict emitted with a non-zero CLI status", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const run: CliRunner = async (args) => {
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const relay = scriptedRelay(() => ({ ok: false, error: { code: "client_ref_conflict", message: "already used" } }));
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace") }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "work/conflict",
		epoch: 0,
	});
	await expect(
		port.send({ sessionId: "sdk-1", repo: join(home, "workspace"), text: "duplicate", opRef: "gw-work-1" }),
	).rejects.toMatchObject({
		name: "OpRefRejectedError",
		code: "client_ref_conflict",
	});
});

test("broker SessionPort retries a terminal-uncertain lifecycle create with the same idempotency key", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	let createCalls = 0;
	const createKeys: string[] = [];
	const sleeps: number[] = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("session.create")) throw new Error(`unexpected command ${args.join(" ")}`);
		createKeys.push(args[args.indexOf("--idempotency-key") + 1]!);
		if (createCalls++ === 0)
			return {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "terminal_uncertain", message: "startup pending" } }),
				stderr: "",
			};
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-after-retry" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace") }),
		sleep: async (milliseconds) => {
			sleeps.push(milliseconds);
		},
	});
	await expect(
		port.bind({ originKey: "loopback/loopback/retry", epoch: 0, repo: join(home, "workspace") }),
	).resolves.toMatchObject({
		sessionId: "sdk-after-retry",
	});
	expect(createCalls).toBe(2);
	expect(createKeys).toHaveLength(2);
	expect(createKeys[0]).toMatch(/^gw-bind-[a-f0-9]{32}$/);
	expect(createKeys[1]).toBe(createKeys[0]);
	expect(sleeps).toEqual([1_000]);
});

test("bind rebinds a persisted live-false session instead of handing a dead monitor endpoint to request", async () => {
	const { mkdtemp, rm } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { GatewayDatabase } = await import("../src/store/db");
	const { BrokerSessionPort } = await import("../src/orchestrator/session-port");
	const { TailRunner } = await import("../src/orchestrator/tail-runner");
	const home = await mkdtemp(join(tmpdir(), "gajaeway-bind-dead-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	try {
		const repo = join(home, "workspace");
		await createOwnedSessionFixture(database, authority, {
			sessionId: "dead-session",
			repo,
			originKey: "monitor/eventtype/x",
			epoch: 1,
		});
		const commands: string[][] = [];
		const cli = async (args: readonly string[]) => {
			commands.push([...args]);
			if (args.includes("inspect") && args.includes("dead-session"))
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: { session: { sessionId: "dead-session", repo, live: false, deleted: false } },
					}),
					stderr: "",
				};
			if (args.includes("session.create"))
				return {
					exitCode: 0,
					stdout: JSON.stringify({ ok: true, result: { sessionId: "fresh-session" } }),
					stderr: "",
				};
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: {} }), stderr: "" };
		};
		const port = new BrokerSessionPort({
			authority,
			database,
			cli,
			instanceId: "i",
			tailRunner: new TailRunner({ stream: noRelay, repo }),
		});
		const binding = await port.bind({ originKey: "monitor/eventtype/x", epoch: 1, repo });
		expect(binding.sessionId).toBe("fresh-session");
		expect(binding.epoch).toBe(2);
		expect(database.getSessionRecord("monitor/eventtype/x")).toMatchObject({ epoch: 2, sessionId: "fresh-session" });
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("a recovered answer is the full body, never the 500-character summary", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-body-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const body = `${"가".repeat(700)} 끝.`;
	const run: CliRunner = async (args) => {
		if (args.includes("transcript.list"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					page: {
						items: [
							// The host ships both: textSummary is body.slice(0, 500).
							{ role: "assistant", ts: new Date().toISOString(), textSummary: body.slice(0, 500), body },
						],
						complete: true,
					},
				}),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const tailRunner = new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({ database, authority, cli: run, instanceId: "instance-body", tailRunner });
	try {
		await createOwnedSessionFixture(database, authority, {
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			originKey: "work/body",
			epoch: 0,
		});
		const recovered = await port.fetchAssistantSince({
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			notBeforeMs: Date.now() - 60_000,
		});
		// Preferring the summary cut every recovered reply mid-sentence at 500.
		expect(recovered?.text).toBe(body);
		expect(recovered?.text.length).toBeGreaterThan(500);
		expect(recovered?.text.endsWith("끝.")).toBe(true);
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("fetchAssistantSince follows transcript continuation pages and returns the newest turn-scoped assistant", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-pages-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const floor = Date.now();
	const cursors: Array<string | undefined> = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("transcript.list")) throw new Error(`unexpected command ${args.join(" ")}`);
		const cursorIndex = args.indexOf("--cursor");
		const cursor = cursorIndex < 0 ? undefined : args[cursorIndex + 1];
		cursors.push(cursor);
		return {
			exitCode: 0,
			stdout: JSON.stringify(
				cursor === undefined
					? {
							page: {
								items: [{ role: "assistant", ts: new Date(floor - 60_000).toISOString(), body: "old answer" }],
								complete: false,
								continuationCursor: "page-2",
							},
						}
					: {
							page: {
								items: [{ role: "assistant", ts: new Date(floor + 1_000).toISOString(), body: "current answer" }],
								complete: true,
							},
						},
			),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-pages",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
	});
	try {
		await createOwnedSessionFixture(database, authority, {
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			originKey: "work/pages",
			epoch: 0,
		});
		const recovered = await port.fetchAssistantSince({
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			notBeforeMs: floor,
		});
		expect(cursors).toEqual([undefined, "page-2"]);
		expect(recovered).toEqual({ text: "current answer", pages: 2, complete: true });
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("close uses the global lifecycle route: the per-session control route prohibits session.close for the daemon CLI", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("session.close") && args.includes("control"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: false,
					error: {
						code: "adapter_operation_prohibited",
						message: "session.close is unavailable through the SDK session CLI.",
					},
				}),
				stderr: "",
			};
		if (args.includes("session.close"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, operation: "session.close", result: { sessionId: "sdk-1" } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: "/tmp/repo",
		originKey: "work/close",
		epoch: 0,
	});
	await port.close({ sessionId: "sdk-1", repo: "/tmp/repo" });
	expect(calls).toHaveLength(1);
	const args = calls[0]!;
	expect(args.slice(0, 4)).toEqual(["sdk", "session", "raw", "global"]);
	expect(args).toContain("session.close");
	expect(args[args.indexOf("--idempotency-key") + 1]).toMatch(/^gw-close-[0-9a-f-]{36}$/);
	expect(JSON.parse(args[args.indexOf("--json-input") + 1]!)).toEqual({ sessionId: "sdk-1" });
});

const closeSessionId = "2adfca12-b752-45ae-835a-4f419125338a";
// Exact v0.18.7 f2bba356: broker/lifecycle.ts ordinary waitForClose result,
// lifecycle/service.ts:1244 success wrapper, cli/session-cli.ts:2358-2371,
// 2548-2551 raw global serialization. No closed boolean exists on this path.
const ordinaryClose = {
	ok: true,
	operation: "session.close",
	result: { sessionId: closeSessionId },
};

async function closeFixture(cli: CliRunner) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-close-contract-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const db = database;
	const authority = initializeTestBrokerAuthority(db, join(home, "agent"));
	const target = { sessionId: closeSessionId, repo: join(home, "workspace") };
	await createOwnedSessionFixture(db, authority, { ...target, originKey: "work/closure", epoch: 0 });
	const port = new BrokerSessionPort({
		database: db,
		authority,
		cli,
		instanceId: "closure-contract",
		now: () => 7,
		tailRunner: new TailRunner({ stream: noRelay, repo: target.repo }),
	});
	return { db, authority, target, port };
}

for (const note of [undefined, "", "Host required SIGTERM"]) {
	test(`ordinary source-backed close accepts optional note ${JSON.stringify(note)} and retains binding`, async () => {
		const f = await closeFixture(async () => ({
			exitCode: 0,
			stdout: JSON.stringify({
				...ordinaryClose,
				result: { sessionId: closeSessionId, ...(note === undefined ? {} : { note }) },
			}),
			stderr: "",
		}));
		await f.port.close(f.target);
		expect(f.db.getSessionRecord("work/closure")).toMatchObject({ sessionId: closeSessionId, epoch: 0 });
		expect(f.db.assertOwnedSession(closeSessionId, f.target.repo, f.authority)).toMatchObject({
			originKey: "work/closure",
			epoch: 0,
		});
	});
}

for (const [name, body] of Object.entries({
	missingResult: { ok: true, operation: "session.close" },
	falseResult: { ...ordinaryClose, result: false },
	arrayResult: { ...ordinaryClose, result: [ordinaryClose.result] },
	wrongSession: { ...ordinaryClose, result: { sessionId: "other" } },
	wrongOperation: { ...ordinaryClose, operation: "session.delete" },
	missingOperation: { ok: true, result: ordinaryClose.result },
	falseOk: { ...ordinaryClose, ok: false },
	truthyOk: { ...ordinaryClose, ok: "true" },
	nullEnvelope: null,
	invalidNote: { ...ordinaryClose, result: { sessionId: closeSessionId, note: null } },
	closedFalse: { ...ordinaryClose, result: { sessionId: closeSessionId, closed: false } },
	inventedClosedTrue: { ...ordinaryClose, result: { sessionId: closeSessionId, closed: true } },
	unrelatedDto: { ...ordinaryClose, result: { accepted: true } },
	contradictoryError: { ...ordinaryClose, error: { code: "terminal_uncertain" } },
	unknownCertainty: { ...ordinaryClose, certainty: "unknown" },
	truncated: { ...ordinaryClose, truncated: true },
	replay: { ...ordinaryClose, replayed: true },
	resultReplay: { ...ordinaryClose, result: { sessionId: closeSessionId, replayed: true } },
	retired: { ...ordinaryClose, result: { sessionId: closeSessionId, retired: true, heartbeatSilenceMs: 5000 } },
	spawnChild: { ...ordinaryClose, result: { sessionId: closeSessionId, code: "spawn_child_closed" } },
	reused: { ...ordinaryClose, result: { sessionId: closeSessionId, reused: true } },
})) {
	test(`close holds ${name} without deleting original binding`, async () => {
		const f = await closeFixture(async () => ({ exitCode: 0, stdout: JSON.stringify(body), stderr: "" }));
		await expect(f.port.close(f.target)).rejects.toBeInstanceOf(GjcCliError);
		expect(f.db.getSessionRecord("work/closure")).toMatchObject({ sessionId: closeSessionId, epoch: 0 });
		expect(f.db.assertOwnedSession(closeSessionId, f.target.repo, f.authority).epoch).toBe(0);
	});
}

test("close uses distinct request identities at the same clock and refuses a historical replay", async () => {
	const keys: string[] = [];
	const f = await closeFixture(async (args) => {
		keys.push(args[args.indexOf("--idempotency-key") + 1]!);
		return {
			exitCode: 0,
			stdout: JSON.stringify(keys.length === 1 ? ordinaryClose : { ...ordinaryClose, replayed: true }),
			stderr: "",
		};
	});
	await f.port.close(f.target);
	await expect(f.port.close(f.target)).rejects.toBeInstanceOf(GjcCliError);
	expect(new Set(keys).size).toBe(2);
});

test("close rejects ownership changed during await even though historical ownership remains", async () => {
	let acknowledge!: () => void;
	const pending = new Promise<void>((resolve) => {
		acknowledge = resolve;
	});
	const f = await closeFixture(async () => {
		await pending;
		return { exitCode: 0, stdout: JSON.stringify(ordinaryClose), stderr: "" };
	});
	const closing = f.port.close(f.target);
	f.db.rebindEpoch("work/closure");
	acknowledge();
	await expect(closing).rejects.toBeInstanceOf(BrokerAuthorityError);
	expect(f.db.assertOwnedSession(closeSessionId, f.target.repo, f.authority).epoch).toBe(0);
	expect(f.db.getSessionRecord("work/closure")?.epoch).toBe(1);
});

test("close refuses historical-only ownership before transport", async () => {
	let calls = 0;
	const f = await closeFixture(async () => {
		calls++;
		return { exitCode: 0, stdout: JSON.stringify(ordinaryClose), stderr: "" };
	});
	f.db.rebindEpoch("work/closure");
	await expect(f.port.close(f.target)).rejects.toBeInstanceOf(BrokerAuthorityError);
	expect(calls).toBe(0);
});

test("close propagates transport failure without erasing original ownership", async () => {
	const failure = new Error("transport lost after dispatch");
	const f = await closeFixture(async () => {
		throw failure;
	});
	await expect(f.port.close(f.target)).rejects.toBe(failure);
	expect(f.db.getSessionRecord("work/closure")?.sessionId).toBe(closeSessionId);
});

for (const response of [
	{ exitCode: 0, stdout: '{"ok":true,"operation":"session.close","result":', stderr: "" },
	{ exitCode: 1, stdout: JSON.stringify(ordinaryClose), stderr: "transport failed" },
]) {
	test(`close rejects incomplete transport receipt ${response.exitCode}`, async () => {
		const f = await closeFixture(async () => response);
		await expect(f.port.close(f.target)).rejects.toBeInstanceOf(GjcCliError);
		expect(f.db.getSessionRecord("work/closure")?.sessionId).toBe(closeSessionId);
	});
}

test("close retains original request correlation when caller mutates input during await", async () => {
	let acknowledge!: () => void;
	const pending = new Promise<void>((resolve) => {
		acknowledge = resolve;
	});
	const f = await closeFixture(async () => {
		await pending;
		return {
			exitCode: 0,
			stdout: JSON.stringify({ ...ordinaryClose, result: { sessionId: "replacement" } }),
			stderr: "",
		};
	});
	const closing = f.port.close(f.target);
	f.target.sessionId = "replacement";
	acknowledge();
	await expect(closing).rejects.toBeInstanceOf(GjcCliError);
	expect(f.db.getSessionRecord("work/closure")?.sessionId).toBe(closeSessionId);
});

test("close rejects broker authority changed during transport", async () => {
	let changeAuthority!: () => void;
	const f = await closeFixture(async () => {
		changeAuthority();
		return { exitCode: 0, stdout: JSON.stringify(ordinaryClose), stderr: "" };
	});
	const assertion = spyOn(f.db, "assertBrokerAuthority");
	changeAuthority = () => {
		assertion.mockImplementation(() => {
			throw new BrokerAuthorityError("authority_mismatch");
		});
	};
	try {
		await expect(f.port.close(f.target)).rejects.toBeInstanceOf(BrokerAuthorityError);
		expect(f.db.getSessionRecord("work/closure")?.sessionId).toBe(closeSessionId);
	} finally {
		assertion.mockRestore();
	}
});

test("foreign live and saved sessions and wrong-repo owned UUIDs never reach any SDK surface", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-ownership-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const ownedId = "11111111-2222-3333-4444-555555555555";
	await createOwnedSessionFixture(database, authority, { sessionId: ownedId, originKey: "owned", epoch: 0, repo });
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: {} }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "ownership",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	for (const target of [
		{ sessionId: "foreign-live", repo },
		{ sessionId: "foreign-saved", repo },
		{ sessionId: ownedId, repo: join(home, "other-repo") },
	]) {
		const operations: Array<() => Promise<unknown>> = [
			() => port.inspect(target),
			() => port.liveness(target),
			() => port.queueEmpty(target),
			() => port.resume({ ...target, originKey: "foreign", epoch: 0 }),
			() => port.send({ ...target, text: "must not send", opRef: "gw-foreign" }),
			() => port.steer({ ...target, text: "must not steer", clientRef: "gw-steer" }),
			() => port.lookupSteerStatus({ ...target, clientRef: "gw-steer" }),
			() => port.setModel({ ...target, selection: "model" }),
			() => port.setModel({ ...target, selection: { preset: "profile" } }),
			() => port.setServiceTier({ ...target, tier: "default" }),
			() => port.status({ ...target, opRef: "gw-foreign" }),
			() => port.failedTurnEvidence({ ...target, startedAtMs: 0, terminalAtMs: 1 }),
			() => port.fetchWorkerOutput({ ...target, opRef: "gw-foreign", notBeforeMs: 0 }),
			() => port.fetchLastAssistant(target),
			() => port.fetchAssistantSince({ ...target, notBeforeMs: 0 }),
			() => port.attachTail({ ...target, brokerGeneration: 0 }),
			() => port.runCompaction({ ...target, originKey: "foreign" }),
			() => port.close(target),
			() => port.request({ ...target, text: "must not replay", opRef: "gw-foreign" }),
		];
		for (const operation of operations) await expect(operation()).rejects.toBeInstanceOf(BrokerAuthorityError);
	}
	expect(calls).toEqual([]);
});

test("legacy private bindings cannot initialize a global port or be resumed and rebound", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-legacy-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	database.putSessionAtEpoch("legacy", "private-session", 0);
	const authority = { canonicalAgentDir: join(home, "global"), identity: `gjc:${join(home, "global")}` };
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw new Error("private ID must not be interpreted globally");
	};
	expect(
		() =>
			new BrokerSessionPort({
				database: database!,
				authority,
				cli: run,
				instanceId: "legacy",
				tailRunner: new TailRunner({ stream: noRelay, repo: home }),
			}),
	).toThrow(BrokerAuthorityError);
	expect(() => database!.assertBrokerAuthority(authority, { initializeEmpty: true })).toThrow(BrokerAuthorityError);
	expect(database.getSessionRecord("legacy")).toMatchObject({ sessionId: "private-session", epoch: 0 });
	expect(calls).toEqual([]);
});

test("bind rejects an unowned persisted ID before inspect and without epoch rotation", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-unowned-binding-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const record = database.getSessionRecord("origin")!;
	// Corrupt the read boundary deliberately: no production adoption API is used.
	const getSessionRecord = database.getSessionRecord.bind(database);
	database.getSessionRecord = () => ({ ...record, sessionId: "private-session" });
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw new Error("unowned binding reached SDK");
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "binding",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	try {
		await expect(port.bind({ originKey: "origin", epoch: 0, repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
		expect(calls).toEqual([]);
	} finally {
		database.getSessionRecord = getSessionRecord;
	}
	expect(database.getSessionRecord("origin")).toEqual(record);
});

test("historical owned bindings remain readable after epoch retirement but fail after authority cutover", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-history-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, {
		sessionId: "retired-owned",
		originKey: "origin",
		epoch: 0,
		repo,
	});
	database.close();
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	database.assertBrokerAuthority(authority);
	database.rebindEpoch("origin");
	await createOwnedSessionFixture(database, authority, {
		sessionId: "current-owned",
		originKey: "origin",
		epoch: 1,
		repo,
	});
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return {
			exitCode: 0,
			stdout: JSON.stringify({ ok: true, page: { items: ["historical answer"], complete: true } }),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "history",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	expect((await port.fetchLastAssistant({ sessionId: "retired-owned", repo })).text).toBe("historical answer");
	expect(database.assertOwnedSession("retired-owned", repo, authority)).toMatchObject({
		originKey: "origin",
		epoch: 0,
	});
	const targetAuthority = {
		canonicalAgentDir: join(home, "other-agent"),
		identity: `gjc:${join(home, "other-agent")}`,
	};
	database.cutoverBrokerAuthority({ expectedAuthority: authority, targetAuthority, evidence: "test fixture cutover" });
	await expect(port.bind({ originKey: "origin", epoch: 2, repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
	await expect(port.fetchLastAssistant({ sessionId: "retired-owned", repo })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(calls).toHaveLength(1);
});

test("authority failures propagate through recovery catches without rebind or replay", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-authority-error-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const refusal = new BrokerAuthorityError("authority_mismatch");
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw refusal;
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "failure",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	const target = { sessionId: "owned", repo };
	for (const operation of [
		() => port.bind({ originKey: "origin", epoch: 0, repo }),
		() => port.bind({ originKey: "new-origin", epoch: 0, repo }),
		() => port.inspect(target),
		() => port.liveness(target),
		() => port.runCompaction({ ...target, originKey: "origin" }),
		() => port.fetchWorkerOutput({ ...target, opRef: "gw-owned", notBeforeMs: 0 }),
	])
		await expect(operation()).rejects.toBe(refusal);
	expect(database.getSessionRecord("origin")).toMatchObject({ sessionId: "owned", epoch: 0 });
	expect(database.getSessionRecord("new-origin")).toBeUndefined();
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
	expect(calls.some((args) => args.includes("session.resume") || args.includes("send"))).toBe(false);
});

test("owned SDK reads and controls ignore unrelated corrupt lane history without hiding census failures", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-isolated-history-"));
	const path = join(home, "gateway.db");
	database = await GatewayDatabase.open(path);
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const raw = new Database(path);
	try {
		raw
			.query(
				"INSERT INTO lane_jobs(job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				"lanejob-unrelated",
				"work-unrelated",
				"main",
				"/unrelated",
				"awaiting_operator",
				"{",
				"2026-09-08T00:00:00.000Z",
				"2026-09-08T00:00:00.000Z",
			);
	} finally {
		raw.close();
	}
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				page: { items: ["owned answer"], complete: true },
				result: { changed: true },
			}),
			stderr: "",
		};
	};
	const options = {
		database,
		authority,
		cli: run,
		instanceId: "isolated-history",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	};
	const port = new BrokerSessionPort(options);
	expect((await port.fetchLastAssistant({ sessionId: "owned", repo })).text).toBe("owned answer");
	expect(await port.setModel({ sessionId: "owned", repo, selection: "model" })).toEqual({ changed: true });
	expect(calls).toHaveLength(2);
	expect(() => database!.inspectBrokerAuthority()).toThrow();
	expect(database.laneJobJson("lanejob-unrelated")).toBe("{");
	expect(() =>
		database!.cutoverBrokerAuthority({
			expectedAuthority: authority,
			targetAuthority: { ...authority, identity: "different" },
			evidence: "must validate history",
			disposition: "quarantine",
		}),
	).toThrow();
	await expect(port.fetchLastAssistant({ sessionId: "foreign", repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
	await expect(port.setModel({ sessionId: "foreign", repo, selection: "model" })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(() => new BrokerSessionPort({ ...options, authority: { ...authority, identity: "wrong" } })).toThrow(
		"authority_mismatch",
	);
	expect(calls).toHaveLength(2);
});
for (const fixture of [
	{ reply: { ok: false, error: { code: "busy" } }, refused: false },
	{
		reply: { ok: false, error: { code: "busy", message: "Host is busy", outcomeCertainty: "not-applied" } },
		refused: true,
	},
	{ reply: { ok: false, error: { code: "session_unavailable" } }, refused: false },
	{ reply: { ok: true, result: { accepted: false, status: "rejected", error: { code: "busy" } } }, refused: false },
	{
		reply: {
			ok: true,
			result: { accepted: false, status: "rejected", clientRef: "expected-ref", error: { code: "busy" } },
		},
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: { accepted: false, status: "rejected", clientRef: "wrong-ref", error: { code: "busy" } },
		},
		refused: false,
	},
	{ reply: { ok: true, result: { accepted: true, clientRef: "wrong-ref" } }, refused: false },
	{ reply: { ok: true, result: { accepted: true } }, refused: false },
	{ reply: { ok: true, result: { accepted: true, status: "rejected", clientRef: "expected-ref" } }, refused: false },
	{
		reply: { ok: true, result: { accepted: true, status: "accepted", clientRef: "expected-ref", ok: false } },
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: {
				accepted: false,
				status: "rejected",
				clientRef: "expected-ref",
				error: { code: "busy", outcomeCertainty: "unknown" },
			},
		},
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: { accepted: true, clientRef: "expected-ref", error: { code: "busy" } },
		},
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: {
				accepted: false,
				status: "rejected",
				clientRef: "expected-ref",
				error: { code: "busy" },
				truncated: true,
			},
		},
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: {
				accepted: false,
				status: "rejected",
				clientRef: "expected-ref",
				error: { code: "busy" },
				terminalAt: "later",
			},
		},
		refused: false,
	},
	{
		reply: {
			ok: true,
			result: { accepted: true, clientRef: "expected-ref", commandId: "command" },
		},
		refused: false,
	},
	{ reply: { ok: true, result: {} }, refused: false },
] as const)
	test(`steer preserves authoritative rejection versus ambiguity: ${JSON.stringify(fixture)}`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
		await createOwnedSessionFixture(database, authority, {
			sessionId: "sdk-1",
			repo: "/tmp/repo",
			originKey: "steer-receipt",
			epoch: 0,
		});
		const run: CliRunner = async () => {
			throw new Error("steer must not spawn a CLI");
		};
		const relay = scriptedRelay((request) => {
			expect(request).toEqual({
				type: "control_request",
				operation: "turn.steer",
				input: { text: "input", clientRef: "expected-ref" },
			});
			return fixture.reply as ReturnType<Parameters<typeof scriptedRelay>[0]>;
		});
		const port = new BrokerSessionPort({
			database,
			authority,
			cli: run,
			instanceId: "instance-1",
			tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
		});
		let failure: unknown;
		try {
			await port.steer({ sessionId: "sdk-1", repo: "/tmp/repo", text: "input", clientRef: "expected-ref" });
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeInstanceOf(GjcCliError);
		expect(((failure as GjcCliError).details as { refused?: boolean } | undefined)?.refused === true).toBe(
			fixture.refused,
		);
	});

async function steerStatusFixture(respond: Parameters<typeof scriptedRelay>[0]) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steer-status-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const target = { sessionId: "steer-owned", repo: join(home, "workspace"), clientRef: "original-steer" };
	await createOwnedSessionFixture(database, authority, { ...target, originKey: "steer-status", epoch: 0 });
	const relay = scriptedRelay(respond);
	const cliCalls: string[][] = [];
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: async (args) => {
			cliCalls.push([...args]);
			throw new Error("steer status must not fall back to CLI or send");
		},
		instanceId: "steer-status",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: target.repo, requestTimeoutMs: 50 }),
	});
	return { port, target, relay, cliCalls, authority, db: database };
}

const originalSteerReceipt = {
	clientRef: "original-steer",
	commandId: "original-command",
	turnId: "original-turn",
	status: "accepted",
	acceptedAt: 1_700_000_000_000,
};

test("Q31 nested rejection preserves unknown certainty as unavailable, never a definitive refusal", async () => {
	let error: unknown = { code: "busy", message: "refused" };
	const { port, target, relay, cliCalls } = await steerStatusFixture(() => ({
		ok: true,
		result: { ...originalSteerReceipt, status: "rejected", error },
	}));
	expect(await port.lookupSteerStatus(target)).toEqual({
		status: "rejected",
		clientRef: target.clientRef,
		acceptedAt: originalSteerReceipt.acceptedAt,
		errorCode: "busy",
	});
	const invalid = [
		{ code: "busy", outcomeCertainty: "unknown" },
		{ code: "busy", message: "uncertain", outcomeCertainty: "unknown" },
		{ code: "busy", message: "contradiction", outcomeCertainty: "applied" },
		{ code: "busy", message: "unsupported", outcomeCertainty: "not-applied" },
		{ code: "busy", message: "contradiction", accepted: true },
		{ code: "busy", message: "contradiction", ok: true },
		{ code: "busy", message: "partial", truncated: true },
		{ code: "busy", message: "partial", complete: false },
		{ code: "busy", message: "extra proof", proof: "settled" },
		{ code: "busy", message: 1 },
		{ code: "busy" },
	];
	for (error of invalid) {
		expect(await port.lookupSteerStatus(target)).toEqual({
			status: "unavailable",
			clientRef: target.clientRef,
			code: "invalid_evidence",
		});
	}
	expect(relay.requests).toHaveLength(invalid.length + 1);
	expect(
		relay.requests.every((request) => request.type === "query_request" && request.operation === "turn.steer_status"),
	).toBe(true);
	expect(cliCalls).toEqual([]);
});

for (const fixture of [
	{
		name: "accepted",
		result: originalSteerReceipt,
		expected: { status: "accepted", clientRef: "original-steer", acceptedAt: originalSteerReceipt.acceptedAt },
	},
	{
		name: "rejected",
		result: {
			...originalSteerReceipt,
			status: "rejected",
			terminalAt: originalSteerReceipt.acceptedAt + 10,
			error: { code: "steer_refused", message: "private runtime diagnostic" },
		},
		expected: {
			status: "rejected",
			clientRef: "original-steer",
			acceptedAt: originalSteerReceipt.acceptedAt,
			terminalAt: originalSteerReceipt.acceptedAt + 10,
			errorCode: "steer_refused",
		},
	},
	{
		name: "restart uncertainty",
		result: {
			...originalSteerReceipt,
			status: "uncertain",
			terminalAt: originalSteerReceipt.acceptedAt + 20,
			error: { code: "process_restart_uncertain", message: "private runtime diagnostic" },
		},
		expected: {
			status: "uncertain",
			clientRef: "original-steer",
			acceptedAt: originalSteerReceipt.acceptedAt,
			terminalAt: originalSteerReceipt.acceptedAt + 20,
			errorCode: "process_restart_uncertain",
		},
	},
	{
		name: "dispatch uncertainty",
		result: {
			...originalSteerReceipt,
			status: "uncertain",
			error: { code: "delivery_uncertain", message: "private runtime diagnostic" },
		},
		expected: {
			status: "uncertain",
			clientRef: "original-steer",
			acceptedAt: originalSteerReceipt.acceptedAt,
			errorCode: "delivery_uncertain",
		},
	},
	{
		name: "unknown original reference",
		result: { clientRef: "original-steer", status: "unknown" },
		expected: { status: "unknown", clientRef: "original-steer" },
	},
	{
		// Exact-release lookup with an object selector can omit clientRef for absence.
		name: "unknown without echoed selector",
		result: { status: "unknown" },
		expected: { status: "unknown", clientRef: "original-steer" },
	},
] satisfies readonly { name: string; result: unknown; expected: SessionSteerStatusResult }[]) {
	test(`steer status reads original receipt without controls: ${fixture.name}`, async () => {
		const { port, target, relay, cliCalls } = await steerStatusFixture(() => ({ ok: true, result: fixture.result }));
		expect(await port.lookupSteerStatus(target)).toEqual(fixture.expected);
		expect(relay.requests).toEqual([
			{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
		]);
		expect(cliCalls).toEqual([]);
		expect(relay.streams).toHaveLength(1);
		expect(relay.streams[0]!.sessionId).toBe(target.sessionId);
		expect(relay.streams[0]!.closed).toBe(true);
	});
}

for (const [name, result, code] of [
	["wrong reference", { ...originalSteerReceipt, clientRef: "other" }, "identity_mismatch"],
	["missing reference", { status: "accepted", acceptedAt: 1 }, "identity_mismatch"],
	["wrong unknown reference", { status: "unknown", clientRef: "other" }, "identity_mismatch"],
	["prompt terminal", { ...originalSteerReceipt, status: "terminal_ok" }, "invalid_evidence"],
	["missing status", { clientRef: "original-steer", accepted: true }, "invalid_evidence"],
	["contradictory acceptance", { ...originalSteerReceipt, accepted: false }, "invalid_evidence"],
	["contradictory rejection", { ...originalSteerReceipt, status: "rejected", accepted: true }, "invalid_evidence"],
	["contradictory uncertainty", { ...originalSteerReceipt, status: "uncertain", accepted: true }, "invalid_evidence"],
	["nested refusal", { ...originalSteerReceipt, ok: false }, "invalid_evidence"],
	["unknown with acceptance", { status: "unknown", accepted: true }, "invalid_evidence"],
	["foreign session field", { ...originalSteerReceipt, sessionId: "other-session" }, "invalid_evidence"],
	["prompt kind", { ...originalSteerReceipt, kind: "prompt" }, "invalid_evidence"],
	["torn SDK identity", { ...originalSteerReceipt, turnId: null }, "invalid_evidence"],
	["oversized SDK identity", { ...originalSteerReceipt, turnId: "x".repeat(129) }, "invalid_evidence"],
	["missing timestamp", { ...originalSteerReceipt, acceptedAt: undefined }, "invalid_evidence"],
	["string timestamp", { ...originalSteerReceipt, acceptedAt: "1700000000000" }, "invalid_evidence"],
	["negative timestamp", { ...originalSteerReceipt, acceptedAt: -1 }, "invalid_evidence"],
	["fractional timestamp", { ...originalSteerReceipt, acceptedAt: 1.5 }, "invalid_evidence"],
	["unsafe timestamp", { ...originalSteerReceipt, acceptedAt: Number.MAX_SAFE_INTEGER + 1 }, "invalid_evidence"],
	["out of date range", { ...originalSteerReceipt, acceptedAt: 8_640_000_000_000_001 }, "invalid_evidence"],
	["nonfinite timestamp", { ...originalSteerReceipt, acceptedAt: Number.NaN }, "invalid_evidence"],
	[
		"accepted terminal timestamp",
		{ ...originalSteerReceipt, terminalAt: originalSteerReceipt.acceptedAt },
		"invalid_evidence",
	],
	["backwards terminal timestamp", { ...originalSteerReceipt, status: "rejected", terminalAt: 1 }, "invalid_evidence"],
	[
		"malformed terminal timestamp",
		{ ...originalSteerReceipt, status: "uncertain", terminalAt: "later" },
		"invalid_evidence",
	],
	["accepted error", { ...originalSteerReceipt, error: { code: "steer_refused" } }, "invalid_evidence"],
	["malformed error", { ...originalSteerReceipt, status: "rejected", error: "refused" }, "invalid_evidence"],
	[
		"unsafe error code",
		{ ...originalSteerReceipt, status: "rejected", error: { code: "private\ntext" } },
		"invalid_evidence",
	],
	[
		"oversized error code",
		{ ...originalSteerReceipt, status: "uncertain", error: { code: "x".repeat(65) } },
		"invalid_evidence",
	],
	["missing result", undefined, "invalid_evidence"],
	["array result", [], "invalid_evidence"],
] as const) {
	test(`steer status holds malformed evidence: ${name}`, async () => {
		const { port, target, relay, cliCalls } = await steerStatusFixture(() => ({ ok: true, result }));
		expect(await port.lookupSteerStatus(target)).toEqual({ status: "unavailable", clientRef: target.clientRef, code });
		expect(relay.requests).toEqual([
			{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
		]);
		expect(relay.streams[0]!.closed).toBe(true);
		expect(cliCalls).toEqual([]);
	});
}

test("steer status validates canonical clientRef before opening a relay and accepts the 128-character boundary", async () => {
	const { port, target, relay, cliCalls } = await steerStatusFixture((request) => ({
		ok: true,
		result: { ...originalSteerReceipt, clientRef: request.input.clientRef },
	}));
	for (const clientRef of ["", " ", "\t\n", " original-steer", "original-steer ", "x".repeat(129)]) {
		await expect(port.lookupSteerStatus({ ...target, clientRef })).rejects.toThrow("steer clientRef");
	}
	expect(relay.streams).toHaveLength(0);
	const clientRef = "x".repeat(128);
	expect(await port.lookupSteerStatus({ ...target, clientRef })).toEqual({
		status: "accepted",
		clientRef,
		acceptedAt: originalSteerReceipt.acceptedAt,
	});
	expect(relay.requests).toEqual([{ type: "query_request", operation: "turn.steer_status", input: { clientRef } }]);
	expect(cliCalls).toEqual([]);
});

for (const code of ["unavailable", "invalid_request", "session_unavailable", "steer_refused"]) {
	test(`steer status query refusal is not original steering rejection: ${code}`, async () => {
		const { port, target, relay, cliCalls } = await steerStatusFixture(() => ({
			ok: false,
			error: { code, message: "private runtime diagnostic" },
		}));
		expect(await port.lookupSteerStatus(target)).toEqual({
			status: "unavailable",
			clientRef: target.clientRef,
			code: code === "unavailable" ? "query_unavailable" : "query_refused",
			errorCode: code,
		});
		expect(relay.requests).toEqual([
			{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
		]);
		expect(relay.streams[0]!.closed).toBe(true);
		expect(cliCalls).toEqual([]);
	});
}

test("steer status preserves caller relay lifetime and refuses another owned session's relay", async () => {
	const { port, target, relay, cliCalls, db, authority } = await steerStatusFixture(() => ({
		ok: true,
		result: originalSteerReceipt,
	}));
	const handle = await port.attachTail({ ...target, brokerGeneration: 0 });
	try {
		expect((await port.lookupSteerStatus({ ...target, relay: handle })).status).toBe("accepted");
		expect(relay.streams[0]!.closed).toBe(false);
		await createOwnedSessionFixture(db, authority, {
			sessionId: "other-owned",
			repo: target.repo,
			originKey: "other-steer-status",
			epoch: 0,
		});
		expect(await port.lookupSteerStatus({ ...target, sessionId: "other-owned", relay: handle })).toEqual({
			status: "unavailable",
			clientRef: target.clientRef,
			code: "identity_mismatch",
		});
		expect(relay.requests).toEqual([
			{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
		]);
		expect(relay.streams[0]!.closed).toBe(false);
		expect(cliCalls).toEqual([]);
	} finally {
		await handle.close();
	}
});

for (const supplied of [false, true]) {
	test(`steer status transport loss stays unavailable without retry (supplied relay: ${supplied})`, async () => {
		const { port, target, relay, cliCalls } = await steerStatusFixture(() => new Promise(() => {}));
		const handle = supplied ? await port.attachTail({ ...target, brokerGeneration: 0 }) : undefined;
		try {
			expect(await port.lookupSteerStatus({ ...target, relay: handle })).toEqual({
				status: "unavailable",
				clientRef: target.clientRef,
				code: "transport_error",
			});
			expect(relay.requests).toEqual([
				{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
			]);
			expect(relay.streams[0]!.closed).toBe(!supplied);
			expect(cliCalls).toEqual([]);
		} finally {
			await handle?.close();
		}
	});
}

test("steer status attach failure stays unavailable without fallback execution", async () => {
	const { target, db, authority, cliCalls } = await steerStatusFixture(() => ({
		ok: true,
		result: originalSteerReceipt,
	}));
	const port = new BrokerSessionPort({
		database: db,
		authority,
		instanceId: "attach-failure",
		cli: async (args) => {
			cliCalls.push([...args]);
			throw new Error("no fallback");
		},
		tailRunner: new TailRunner({ stream: noRelay, repo: target.repo }),
	});
	expect(await port.lookupSteerStatus(target)).toEqual({
		status: "unavailable",
		clientRef: target.clientRef,
		code: "transport_error",
	});
	expect(cliCalls).toEqual([]);
});

for (const reply of ["accepted", "refused"] as const) {
	test(`steer status fences authority invalidation during ${reply} query await`, async () => {
		let invalidate!: () => void;
		const { port, target, relay, cliCalls, db, authority } = await steerStatusFixture(async () => {
			await Promise.resolve();
			invalidate();
			return reply === "accepted"
				? { ok: true, result: originalSteerReceipt }
				: { ok: false, error: { code: "unavailable" } };
		});
		invalidate = () => {
			const canonicalAgentDir = join(home, "other-agent");
			db.cutoverBrokerAuthority({
				expectedAuthority: authority,
				targetAuthority: { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` },
				evidence: "steer status await fixture",
			});
		};
		await expect(port.lookupSteerStatus(target)).rejects.toBeInstanceOf(BrokerAuthorityError);
		expect(relay.requests).toEqual([
			{ type: "query_request", operation: "turn.steer_status", input: { clientRef: target.clientRef } },
		]);
		expect(relay.streams[0]!.closed).toBe(true);
		expect(cliCalls).toEqual([]);
	});
}

test("a relay that dies before the steer reply is ambiguity, never a definitive rejection", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: "/tmp/repo",
		originKey: "steer-torn",
		epoch: 0,
	});
	const relay = scriptedRelay(() => new Promise(() => {}));
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: async () => {
			throw new Error("steer must not spawn a CLI");
		},
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), requestTimeoutMs: 50 }),
	});
	const attempt = port.steer({ sessionId: "sdk-1", repo: "/tmp/repo", text: "input", clientRef: "expected-ref" });
	await Bun.sleep(5);
	relay.streams[0]!.close();
	const failure = await attempt.catch((error: unknown) => error);
	expect(failure).toBeInstanceOf(Error);
	expect((failure as { details?: { refused?: boolean } }).details?.refused).toBeUndefined();
	expect(isDefinitiveSteerRejection(failure)).toBe(false);
});

async function promptReceiptFixture(respond: Parameters<typeof scriptedRelay>[0], cli?: CliRunner) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-prompt-receipt-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const target = { sessionId: "prompt-owned", repo: join(home, "workspace"), opRef: "original-prompt", text: "work" };
	await createOwnedSessionFixture(database, authority, { ...target, originKey: "prompt-receipt", epoch: 0 });
	const relay = scriptedRelay(respond);
	const port = new BrokerSessionPort({
		database,
		authority,
		cli:
			cli ??
			(async () => {
				throw new Error("prompt must not fall back to CLI");
			}),
		instanceId: "prompt-receipt",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: target.repo, requestTimeoutMs: 50 }),
		now: () => 1_800_000_000_000,
		sleep: async () => {
			throw new Error("uncertain prompt must not retry");
		},
	});
	return { port, target, relay, authority, db: database };
}

// Exact v0.18.7 prompt acknowledgement: status/acceptedAt belong to reconciliation,
// not this DTO. Invocation IDs are learned here, not manufactured by the gateway.
const originalPromptReceipt = {
	accepted: true,
	clientRef: "original-prompt",
	commandId: "original-command",
	turnId: "original-turn",
};

for (const supplied of [false, true]) {
	test(`send retains canonical prompt identity without requiring status or SDK time (supplied=${supplied})`, async () => {
		const { port, target, relay } = await promptReceiptFixture(() => ({ ok: true, result: originalPromptReceipt }));
		const handle = supplied ? await port.attachTail({ ...target, brokerGeneration: 0 }) : undefined;
		try {
			expect(await port.send({ ...target, relay: handle })).toEqual({
				sessionId: target.sessionId,
				operationRef: originalPromptReceipt.clientRef,
				commandId: originalPromptReceipt.commandId,
				turnId: originalPromptReceipt.turnId,
				acceptedAt: new Date(1_800_000_000_000).toISOString(),
				taskKey: "gateway",
			});
			expect(relay.requests).toEqual([
				{ type: "control_request", operation: "turn.prompt", input: { text: target.text, clientRef: target.opRef } },
			]);
			expect(relay.streams[0]!.closed).toBe(!supplied);
		} finally {
			await handle?.close();
		}
	});
}

for (const [name, result] of [
	["empty", {}],
	["missing result", undefined],
	["null", null],
	["array", [originalPromptReceipt]],
	["missing clientRef", { accepted: true, commandId: "c", turnId: "t" }],
	["wrong clientRef", { ...originalPromptReceipt, clientRef: "another-prompt" }],
	["missing acceptance", { clientRef: "original-prompt", commandId: "c", turnId: "t" }],
	["false acceptance", { ...originalPromptReceipt, accepted: false }],
	["truthy acceptance", { ...originalPromptReceipt, accepted: "true" }],
	["missing commandId", { accepted: true, clientRef: "original-prompt", turnId: "t" }],
	["missing turnId", { accepted: true, clientRef: "original-prompt", commandId: "c" }],
	["empty commandId", { ...originalPromptReceipt, commandId: "" }],
	["blank turnId", { ...originalPromptReceipt, turnId: " " }],
	["numeric commandId", { ...originalPromptReceipt, commandId: 42 }],
	["numeric turnId", { ...originalPromptReceipt, turnId: 42 }],
	["unknown status", { ...originalPromptReceipt, status: "unknown" }],
	["rejected status", { ...originalPromptReceipt, status: "rejected" }],
	["status instead of acceptance", { clientRef: "original-prompt", commandId: "c", turnId: "t", status: "accepted" }],
	["false inner ok", { ...originalPromptReceipt, ok: false }],
	["inner error", { ...originalPromptReceipt, error: { code: "busy" } }],
	["foreign session", { ...originalPromptReceipt, sessionId: "foreign" }],
	["undocumented wrapper", { receipt: originalPromptReceipt }],
] as const) {
	test(`send holds ${name} acknowledgement without accepted promotion or replay`, async () => {
		const { port, target, relay } = await promptReceiptFixture(() => ({ ok: true, result }));
		let accepted = 0;
		const failure = await port.send(target).then(
			() => {
				accepted++;
			},
			(error: unknown) => error,
		);
		expect(accepted).toBe(0);
		expect(failure).toBeInstanceOf(GjcCliError);
		expect(failure).toMatchObject({ details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" } });
		expect(isSessionBusy(failure)).toBe(false);
		expect(failure).not.toBeInstanceOf(ModelNotSelectedError);
		expect(isDefinitiveSteerRejection(failure)).toBe(false);
		expect(relay.requests).toHaveLength(1);
		expect(relay.streams[0]!.closed).toBe(true);
	});
}

for (const reply of [
	{ ok: true, result: originalPromptReceipt, error: { code: "busy" } },
	{ ok: false, result: originalPromptReceipt, error: { code: "busy" } },
	{ ok: false, result: {}, error: { code: "model_not_selected" } },
	{ ok: false, error: {} },
	{ ok: true, result: originalPromptReceipt, page: {} },
]) {
	test(`send rejects contradictory or missing acknowledgement envelope ${JSON.stringify(reply)}`, async () => {
		const { port, target, relay } = await promptReceiptFixture(() => reply as ScriptedRelayReply);
		const failure = await port.send(target).catch((error: unknown) => error);
		expect(failure).toMatchObject({ details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" } });
		expect(isSessionBusy(failure)).toBe(false);
		expect(failure).not.toBeInstanceOf(ModelNotSelectedError);
		expect(relay.requests).toHaveLength(1);
	});
}

for (const reply of [
	{ result: originalPromptReceipt },
	{ ok: "true", result: originalPromptReceipt },
	{ ok: false, accepted: true, error: { code: "busy" } },
	{ ok: false, error: { code: "busy", outcomeCertainty: "unknown" } },
	{ ok: false, error: { code: "busy", accepted: true } },
	{ ok: true, result: originalPromptReceipt, error: "busy" },
	{ ok: false, error: { code: "busy" }, page: false },
	{ ok: true, result: originalPromptReceipt, truncated: true },
	null,
]) {
	test(`send holds malformed supplied-handle replies ${JSON.stringify(reply)}`, async () => {
		const { port, target } = await promptReceiptFixture(() => {
			throw new Error("unexpected stream control");
		});
		let controls = 0;
		const handle = {
			sessionId: target.sessionId,
			control: async () => {
				controls++;
				return reply;
			},
		} as unknown as TailHandle;
		const failure = await port.send({ ...target, relay: handle }).catch((error: unknown) => error);
		expect(failure).toMatchObject({ details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" } });
		expect(isSessionBusy(failure)).toBe(false);
		expect(controls).toBe(1);
	});
}

for (const when of ["before", "after"] as const) {
	test(`send fences a foreign relay session ${when} control await`, async () => {
		const { port, target } = await promptReceiptFixture(() => {
			throw new Error("unexpected stream control");
		});
		let controls = 0;
		let relaySessionId = when === "before" ? "foreign" : target.sessionId;
		const handle = {
			get sessionId() {
				return relaySessionId;
			},
			control: async () => {
				controls++;
				await Promise.resolve();
				relaySessionId = "foreign";
				return { ok: true, result: originalPromptReceipt };
			},
		} as unknown as TailHandle;
		await expect(port.send({ ...target, relay: handle })).rejects.toMatchObject({
			details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" },
		});
		expect(controls).toBe(when === "before" ? 0 : 1);
	});
}

for (const kind of ["authority", "ownership", "epoch"] as const) {
	for (const ok of [true, false]) {
		const scenario =
			kind === "epoch"
				? "synthetic epoch evidence"
				: kind === "ownership"
					? "ownership retirement via authority cutover"
					: "authority";
		test(`send fences ${scenario} invalidation during control await (ok=${ok})`, async () => {
			let entered!: () => void;
			let release!: (reply: ScriptedRelayReply) => void;
			const controlEntered = new Promise<void>((resolve) => {
				entered = resolve;
			});
			const response = new Promise<ScriptedRelayReply>((resolve) => {
				release = resolve;
			});
			const reply: ScriptedRelayReply = ok
				? { ok: true, result: originalPromptReceipt }
				: { ok: false, error: { code: "busy" } };
			const { port, target, relay, db, authority } = await promptReceiptFixture(() => {
				entered();
				return response;
			});
			const assertOwnedSession = db.assertOwnedSession.bind(db);
			let ownershipQuery: ReturnType<typeof spyOn<GatewayDatabase, "assertOwnedSession">> | undefined;
			const sending = port.send(target).catch((error: unknown) => error);
			try {
				// Keep mutations and assertions on the awaited test path, not the
				// relay's fire-and-forget response callback.
				await Promise.race([
					controlEntered,
					sending.then(() => {
						throw new Error("send settled before entering prompt control");
					}),
				]);
				if (kind === "epoch") {
					// Synthetic defensive-branch evidence only: production provenance
					// is immutable, so an in-place binding epoch change is unreachable.
					ownershipQuery = spyOn(db, "assertOwnedSession").mockImplementation((sessionId, repo, owner) => {
						const owned = assertOwnedSession(sessionId, repo, owner);
						return sessionId === target.sessionId ? { ...owned, epoch: owned.epoch + 1 } : owned;
					});
				} else {
					const canonicalAgentDir = join(home, "other-agent");
					const targetAuthority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
					db.cutoverBrokerAuthority({
						expectedAuthority: authority,
						targetAuthority,
						evidence: "prompt await fixture",
					});
					if (kind === "ownership") {
						// Supported retirement keeps historical provenance but removes
						// the old session from the new authority's ownership.
						expect(db.getSessionRecord("prompt-receipt")).toMatchObject({ sessionId: "", epoch: 1 });
						expect(() => db.assertOwnedSession(target.sessionId, target.repo, targetAuthority)).toThrow(
							BrokerAuthorityError,
						);
					}
				}
				release(reply);
				const failure = await sending;
				if (kind === "epoch") {
					expect(failure).toMatchObject({ details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" } });
					expect(ownershipQuery).toHaveBeenCalledTimes(1);
					expect(assertOwnedSession(target.sessionId, target.repo, authority).epoch).toBe(0);
				} else expect(failure).toBeInstanceOf(BrokerAuthorityError);
				expect(isSessionBusy(failure)).toBe(false);
				expect(relay.requests).toEqual([
					{ type: "control_request", operation: "turn.prompt", input: { text: target.text, clientRef: target.opRef } },
				]);
				expect(relay.streams[0]!.closed).toBe(true);
			} finally {
				release(reply);
				await sending;
				ownershipQuery?.mockRestore();
			}
		});
	}
}

test("send holds a torn prompt reply without a second control", async () => {
	const { port, target, relay } = await promptReceiptFixture(() => new Promise(() => {}));
	const failure = await port.send(target).catch((error: unknown) => error);
	expect(failure).toBeInstanceOf(Error);
	expect(isSessionBusy(failure)).toBe(false);
	expect(failure).not.toBeInstanceOf(ModelNotSelectedError);
	expect(relay.requests).toHaveLength(1);
	expect(relay.streams[0]!.closed).toBe(true);
});

test("model preflight failure proves only this call sent no prompt, not absence of model side effects", async () => {
	const calls: string[][] = [];
	let modelApplied = false;
	const { port, target, relay } = await promptReceiptFixture(
		() => {
			throw new Error("preflight failure must not submit a prompt");
		},
		async (args) => {
			calls.push([...args]);
			expect(args).toContain("model.profile.set");
			modelApplied = true; // The effect landed, but its receipt did not.
			throw new Error(`model acknowledgement lost ${"x".repeat(2_000)}`);
		},
	);
	const failure = await port.send({ ...target, model: { preset: "selected-model" } }).catch((error: unknown) => error);
	expect(failure).toBeInstanceOf(PromptNotSubmittedError);
	expect(failure).toMatchObject({ opRef: target.opRef, phase: "model_preflight" });
	expect(String((failure as Error).cause).length).toBeLessThanOrEqual(512);
	expect((failure as { details?: unknown }).details).toBeUndefined();
	expect(modelApplied).toBe(true);
	expect(calls).toHaveLength(1);
	expect(relay.requests).toHaveLength(0);
	expect(relay.streams).toHaveLength(0);
});

test("model preflight proof does not erase an earlier ambiguous send under the same reference", async () => {
	const { port, target, relay } = await promptReceiptFixture(
		() => ({ ok: true, result: {} }),
		async () => {
			throw new Error("model receipt lost");
		},
	);
	await expect(port.send(target)).rejects.toMatchObject({
		details: { code: "prompt_receipt_uncertain", outcomeCertainty: "unknown" },
	});
	await expect(port.send({ ...target, model: { preset: "selected-model" } })).rejects.toMatchObject({
		opRef: target.opRef,
		phase: "model_preflight",
	});
	expect(relay.requests).toEqual([
		{ type: "control_request", operation: "turn.prompt", input: { text: target.text, clientRef: target.opRef } },
	]);
});

test("send waits for model preflight acknowledgement before its only prompt control", async () => {
	const events: string[] = [];
	let release!: () => void;
	const preflight = new Promise<void>((resolve) => {
		release = resolve;
	});
	const { port, target, relay } = await promptReceiptFixture(
		() => {
			events.push("prompt");
			return { ok: true, result: originalPromptReceipt };
		},
		async () => {
			events.push("model");
			await preflight;
			events.push("model-receipt");
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: true }), stderr: "" };
		},
	);
	const sending = port.send({ ...target, model: { preset: "selected-model" } });
	expect(events).toEqual(["model"]);
	expect(relay.requests).toHaveLength(0);
	release();
	expect((await sending).operationRef).toBe(target.opRef);
	expect(events).toEqual(["model", "model-receipt", "prompt"]);
	expect(relay.requests).toHaveLength(1);
});

test("send preserves explicit client reference conflict without another control", async () => {
	const { port, target, relay } = await promptReceiptFixture(() => ({
		ok: false,
		error: { code: "client_ref_conflict", message: "Client reference conflicts with an existing control" },
	}));
	await expect(port.send(target)).rejects.toMatchObject({ opRef: target.opRef, code: "client_ref_conflict" });
	expect(relay.requests).toHaveLength(1);
});

test("send distinguishes an explicit model refusal from a transport error with the same code", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo,
		originKey: "model-refusal",
		epoch: 0,
	});
	const refusalRelay = scriptedRelay((request) => {
		if (request.operation !== "turn.prompt") throw new Error(`unexpected request ${request.operation}`);
		return { ok: false, error: { code: "model_not_selected", message: "The requested model is unavailable." } };
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: async () => {
			throw new Error("unexpected CLI command");
		},
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: refusalRelay.spawn, repo }),
	});
	const refusal = await port
		.send({ sessionId: "sdk-1", repo, text: "prompt", opRef: "gw-p-model-refusal" })
		.catch((error: unknown) => error);
	expect(refusal).toBeInstanceOf(ModelNotSelectedError);
	expect(refusal).toMatchObject({ opRef: "gw-p-model-refusal", code: "model_not_selected" });
	expect(refusalRelay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(1);

	const transportError = new GjcCliError("relay failed", 0, "", { code: "model_not_selected" });
	const transportRelay = {
		sessionId: "sdk-1",
		control: async () => {
			throw transportError;
		},
	} as unknown as TailHandle;
	await expect(
		port.send({ sessionId: "sdk-1", repo, text: "prompt", opRef: "gw-p-model-transport", relay: transportRelay }),
	).rejects.toBe(transportError);
});

test("send waits out a `busy` refusal and resends under the same op-ref once the turn is free", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));

	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "busy-wait",
		epoch: 0,
	});
	const repo = join(home, "workspace");
	const sends: string[] = [];
	const sleeps: number[] = [];
	let clock = 0;
	const run: CliRunner = async (args) => {
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const relay = scriptedRelay((request) => {
		if (request.operation !== "turn.prompt") throw new Error(`unexpected request ${request.operation}`);
		sends.push(String(request.input.clientRef));
		if (sends.length < 3)
			return { ok: false, error: { code: "busy", message: "turn.prompt is unavailable while the agent is busy" } };
		return { ok: true, result: { commandId: "c", turnId: "t", accepted: true, clientRef: request.input.clientRef } };
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		now: () => clock,
		sleep: async (ms) => {
			sleeps.push(ms);
			clock += ms;
		},
	});
	const receipt = await port.send({ sessionId: "sdk-1", repo, text: "hi", opRef: "gw-p-busy1" });
	expect(receipt.operationRef).toBe("gw-p-busy1");
	expect(sends).toEqual(["gw-p-busy1", "gw-p-busy1", "gw-p-busy1"]);
	expect(sleeps).toEqual([2_000, 2_000]);
});

test("send surfaces `busy` once the bounded wait is exhausted, never having sent", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));

	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "busy-exhaust",
		epoch: 0,
	});
	const repo = join(home, "workspace");
	let clock = 0;
	let sends = 0;
	const run: CliRunner = async () => {
		throw new Error("unexpected command");
	};
	const relay = scriptedRelay(() => {
		sends++;
		return { ok: false, error: { code: "busy", message: "busy" } };
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		now: () => clock,
		sleep: async (ms) => {
			clock += ms;
		},
	});
	await expect(
		port.send({ sessionId: "sdk-1", repo, text: "hi", opRef: "gw-p-busy2", busyWaitMs: 5_000 }),
	).rejects.toThrow(/busy/);
	// Attempts at 0s, 2s, 4s, 6s; the 6s refusal lands past the 5s deadline and surfaces.
	expect(sends).toBe(4);
});

test("request keeps observing an accepted op on the CLI when its relay tears mid-turn, never abandoning it", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "sdk-1", repo, originKey: "torn", epoch: 0 });
	const cliStatus: string[] = [];
	let cliReports = 0;
	const run: CliRunner = async (args) => {
		if (args.includes("status")) {
			cliStatus.push(args[args.indexOf("status") + 2] ?? "");
			cliReports += 1;
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						operationRef: "gw-torn-1",
						status: { status: cliReports < 2 ? "in_flight" : "terminal_ok", clientRef: "gw-torn-1" },
						summary: { completed: cliReports >= 2 },
					},
				}),
				stderr: "",
			};
		}
		if (args.includes("session.last_assistant"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ type: "query_response", ok: true, page: { items: ["survived"], complete: true } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	let relayQueries = 0;
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return { ok: true, result: { commandId: "c", turnId: "t", accepted: true, clientRef: request.input.clientRef } };
		relayQueries += 1;
		// The relay tears on the first status read: the answer never comes.
		relay.streams[0]!.close();
		return new Promise(() => {});
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 200 }),
	});
	const result = await port.request({ sessionId: "sdk-1", repo, text: "go", opRef: "gw-torn-1", pollMs: 0 });
	expect(result.status.status.status).toBe("terminal_ok");
	expect(result.assistant.text).toBe("survived");
	expect(relayQueries).toBe(1);
	// The SAME clientRef was observed on the CLI after the tear; nothing was re-sent.
	expect(cliStatus.every((ref) => ref === "gw-torn-1")).toBe(true);
	expect(cliReports).toBe(2);
	expect(relay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(1);
});

// Issue #9: the bounded request wait was a fixed wall-clock cap. Long monitor
// turns (canonicalize, townhall) legitimately run 25-40 minutes while emitting
// tool activity the whole way, and were killed as SessionRequestTimeoutError
// at the 1800 s mark with the work still landing. The wait is an inactivity
// lease: frames attributed to the turn refresh it; silence still ends it.
for (const progressing of [true, false]) {
	test(`request wait is an activity lease: a turn that keeps emitting ${progressing ? "survives past" : "is not spared when silent for"} waitTimeoutMs`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
		const repo = join(home, "workspace");
		await createOwnedSessionFixture(database, authority, { sessionId: "sdk-1", repo, originKey: "lease", epoch: 0 });
		const run: CliRunner = async (args) => {
			throw new Error(`unexpected command ${args.join(" ")}`);
		};
		let clock = 0;
		let polls = 0;
		const ids = { commandId: "cmd-lease", turnId: "turn-lease" };
		const relay = scriptedRelay((request) => {
			if (request.operation === "turn.prompt")
				return { ok: true, result: { ...ids, accepted: true, clientRef: request.input.clientRef } };
			if (request.operation === "session.last_assistant")
				return { ok: true, page: { items: ["landed"], complete: true } };
			polls += 1;
			// Terminal only on the 6th poll (clock 5000 ms); the lease is 2500 ms.
			return {
				ok: true,
				result: {
					kind: "prompt",
					status: polls >= 6 ? "terminal_ok" : "in_flight",
					clientRef: request.input.clientRef,
				},
			};
		});
		const port = new BrokerSessionPort({
			database,
			authority,
			cli: run,
			instanceId: "instance-1",
			tailRunner: new TailRunner({ stream: relay.spawn, repo, now: () => clock }),
			now: () => clock,
			sleep: async (ms) => {
				clock += ms;
				if (progressing)
					relay.streams[0]!.host({ type: "event", kind: "tool_execution_update", ...ids, payload: { event: {} } });
				// Let the pushed frame reach the handle before the next deadline check.
				await Bun.sleep(1);
			},
		});
		const attempt = port.request({
			sessionId: "sdk-1",
			repo,
			text: "long work",
			opRef: "gw-lease-1",
			pollMs: 1000,
			waitTimeoutMs: 2500,
		});
		if (progressing) {
			const result = await attempt;
			expect(result.status.status.status).toBe("terminal_ok");
			expect(result.assistant.text).toBe("landed");
			expect(clock).toBe(5000);
		} else {
			const failure = await attempt.catch((error: unknown) => error);
			expect(failure).toBeInstanceOf(SessionRequestTimeoutError);
			expect((failure as SessionRequestTimeoutError).lastStatus.status.status).toBe("in_flight");
			expect(clock).toBe(3000);
		}
	});
}

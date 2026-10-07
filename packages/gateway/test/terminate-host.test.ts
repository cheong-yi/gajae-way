import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort, parseRunningJobs } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { noRelay } from "./session-port.fake";

/**
 * The safety property of ending a retired session's host: the gateway may only
 * signal a pid that (a) the broker reports for THAT session and (b) `ps` shows
 * to be a `sdk session-host-internal`. A real child process stands in for the
 * host so the SIGTERM path is exercised for real, not mocked.
 */
async function harness(pidFor: (sessionId: string) => number | undefined, live = true) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-terminate-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const run: CliRunner = async (args) => {
		if (args.includes("inspect")) {
			const sessionId = args[args.indexOf("inspect") + 1]!;
			const pid = pidFor(sessionId);
			if (pid === undefined)
				return {
					exitCode: 1,
					stdout: JSON.stringify({ ok: false, error: { code: "session_unavailable", message: "gone" } }),
					stderr: "",
				};
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId, live, pid } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const repo = join(home, "workspace");
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "terminate",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
	});
	// The port refuses any session it does not own (#assertOwned): register the
	// ids these tests will name, exactly as bind() would have.
	for (const sessionId of ["s-1", "s-2", "s-3", "s-4", "s-5"]) {
		const owned = database.recordOwnedBinding({
			authority,
			sessionId,
			originKey: `loopback/loopback/${sessionId}`,
			epoch: 0,
			repo,
		});
		if (!owned) throw new Error(`could not register ownership of ${sessionId}`);
	}
	return {
		port,
		repo,
		async close() {
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

/**
 * A long-lived child whose argv looks like a gjc session host; it exits on
 * SIGTERM. A shell no-op loop carries the tag in its own argv, so
 * `ps -o command=` shows it (`sleep` rejects extra arguments and exits at once).
 */
function fakeHost(argvTag: string) {
	return Bun.spawn(["sh", "-c", "while :; do sleep 1; done", argvTag], { stdout: "ignore", stderr: "ignore" });
}

test("a live session host reported by the broker is SIGTERMed and exits", async () => {
	const child = fakeHost("sdk session-host-internal");
	const h = await harness(() => child.pid);
	try {
		const outcome = await h.port.terminateHost({ sessionId: "s-1", repo: h.repo });
		expect(outcome).toEqual({ outcome: "terminated", pid: child.pid });
		expect(await child.exited).not.toBe(0); // signalled, not a clean exit
	} finally {
		child.kill();
		await h.close();
	}
});

test("a pid that is not a session host is never signalled - broker, daemon, or anything else", async () => {
	for (const tag of ["sdk broker-internal", "sdk daemon-internal", "some-other-program"]) {
		const child = fakeHost(tag);
		const h = await harness(() => child.pid);
		try {
			const outcome = await h.port.terminateHost({ sessionId: "s-2", repo: h.repo });
			expect(outcome.outcome).toBe("not_a_host");
			// Still running: nothing was sent.
			expect(child.killed).toBe(false);
			expect(await Promise.race([child.exited, Bun.sleep(150).then(() => "alive")])).toBe("alive");
		} finally {
			child.kill();
			await h.close();
		}
	}
});

test("a session the broker no longer knows, or reports not live, is already gone", async () => {
	const gone = await harness(() => undefined);
	try {
		expect(await gone.port.terminateHost({ sessionId: "s-3", repo: gone.repo })).toEqual({ outcome: "already_gone" });
	} finally {
		await gone.close();
	}
	const child = fakeHost("sdk session-host-internal");
	const notLive = await harness(() => child.pid, false);
	try {
		expect(await notLive.port.terminateHost({ sessionId: "s-4", repo: notLive.repo })).toEqual({
			outcome: "already_gone",
		});
		expect(child.killed).toBe(false);
	} finally {
		child.kill();
		await notLive.close();
	}
});

test("a pid that already exited is already gone, not an error", async () => {
	const child = fakeHost("sdk session-host-internal");
	const pid = child.pid;
	child.kill();
	await child.exited;
	const h = await harness(() => pid);
	try {
		expect(await h.port.terminateHost({ sessionId: "s-5", repo: h.repo })).toEqual({ outcome: "already_gone" });
	} finally {
		await h.close();
	}
});

// #41: the host's running background jobs are read before it is ended. Shape
// captured from a live gjc 0.17.2 `runtime.jobs.list` query response.
test("runtime.jobs.list: running jobs are parsed; an unreadable answer throws instead of reading as none", () => {
	const result = (envelope: unknown): CliResult => ({ exitCode: 0, stderr: "", stdout: JSON.stringify(envelope) });
	const page = (running: unknown[]) =>
		result({
			type: "query_response",
			ok: true,
			page: {
				items: [{ running, recent: [], delivery: { queued: 0, delivering: false, pendingJobIds: [] } }],
				complete: true,
			},
		});
	expect(parseRunningJobs(page([]))).toEqual([]);
	expect(
		parseRunningJobs(page([{ id: "0-Review", type: "task", status: "running", label: "review", startTime: 1 }])),
	).toEqual([{ id: "0-Review", type: "task", label: "review" }]);
	expect(() => parseRunningJobs(result({ ok: false, error: { code: "resource_gone" } }))).toThrow();
	expect(() => parseRunningJobs(result({ ok: true, page: { items: [null], complete: true } }))).toThrow();
});

test("actual runningJobs requires successful complete singleton public query evidence", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-jobs-proof-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = { canonicalAgentDir: home, identity: `gjc:${home}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const repo = join(home, "workspace");
	const sessionId = "jobs-owned";
	database.recordOwnedBinding({ authority, sessionId, originKey: "jobs/proof", epoch: 0, repo });
	const snapshot = { running: [], recent: [], delivery: { queued: 0, delivering: false, pendingJobIds: [] } };
	const clean = { type: "query_response", query: "runtime.jobs.list", ok: true, page: { items: [snapshot], complete: true } };
	let reply: CliResult = { exitCode: 0, stderr: "", stdout: JSON.stringify(clean) };
	const calls: string[][] = [];
	const port = new BrokerSessionPort({
		database, authority, instanceId: "jobs-proof",
		cli: async (args) => { calls.push([...args]); return reply; },
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	const query = () => port.runningJobs({ sessionId, repo });
	try {
		expect(await query()).toEqual([]);
		for (const key of ["type", "kind"]) {
			reply = { ...reply, stdout: JSON.stringify({ ...clean, page: { ...clean.page, items: [{
				...snapshot, running: [{ id: "active", [key]: "task", label: "still running", status: "running" }],
			}] } }) };
			expect(await query()).toEqual([{ id: "active", type: "task", label: "still running" }]);
		}
		const invalid = [
			{ ...clean, error: { code: "terminal_uncertain" } },
			{ ...clean, query: "queue.messages.list" },
			{ ...clean, operation: "session.close" },
			{ ...clean, type: "control_response" },
			{ ...clean, truncated: true },
			{ ...clean, valid: false },
			{ ...clean, result: {} },
			{ ...clean, page: { ...clean.page, complete: false } },
			{ ...clean, page: { ...clean.page, complete: undefined } },
			{ ...clean, page: { ...clean.page, continuationCursor: "more" } },
			{ ...clean, page: { ...clean.page, preview: true } },
			{ ...clean, page: { ...clean.page, truncated: true } },
			{ ...clean, page: { ...clean.page, items: [] } },
			{ ...clean, page: { ...clean.page, items: [snapshot, { running: [{ id: "live", type: "task", label: "live" }] }] } },
			{ ...clean, page: { ...clean.page, items: [{ ...snapshot, complete: false }] } },
			...[null, {}, { id: "live", type: "task", label: 1 }, { id: "live", type: "task", label: "live", truncated: true }].map(
				(job) => ({ ...clean, page: { ...clean.page, items: [{ ...snapshot, running: [job] }] } }),
			),
		];
		for (const envelope of invalid) {
			reply = { exitCode: 0, stderr: "", stdout: JSON.stringify(envelope) };
			await expect(query()).rejects.toThrow();
		}
		reply = { exitCode: 1, stderr: "", stdout: JSON.stringify(clean) };
		await expect(query()).rejects.toThrow();
		expect(calls).toHaveLength(4 + invalid.length);
		expect(calls.every((args) => args.join(" ") === `sdk session raw query ${sessionId} --query runtime.jobs.list --json-input {}`)).toBe(true);
		database.assertOwnedSession(sessionId, repo, authority);
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type OriginRef,
	originKey,
	type WorkApproveResult,
	type WorkFirstmateAssignmentParams,
	type WorkFirstmateRetireResult,
} from "@gajae-gateway/protocol";
import { GajaewayClient } from "../../sdk/src/client";
import type { GatewayConfig } from "../src/config";
import { paneClosed, readChildPane, readLanePane } from "../src/orchestrator/firstmate-pane";
import * as ghChecks from "../src/orchestrator/gh-checks";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

async function until(predicate: () => boolean) {
	for (let index = 0; index < 2000 && !predicate(); index++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

type Send = (where: OriginRef, messageId: string, text: string, authorId: string) => Promise<{ engaged: boolean }>;

/**
 * A closed pane refuses the line before the inbox, binds nothing and starts no turn. The barrier
 * is a completed turn in a fresh unrelated thread, sent after the refused line.
 */
function noPersona(db: Store, port: ScriptedSessionPort, client: () => GajaewayClient, child: OriginRef) {
	const childKey = originKey(child);
	const childBinds = () => port.binds.filter((bind) => bind.originKey === childKey).length;
	const send: Send = (where, messageId, text, authorId) =>
		client().request<{ engaged: boolean }>("chat.send", {
			origin: where,
			messageId,
			text,
			engagement: { mentioned: where !== child, group: true, authorId },
		});
	return async (messageId: string, text: string) => {
		const attemptsBefore = port.sendAttempts.length;
		const bindsBefore = childBinds();
		expect((await send(child, messageId, text, "human")).engaged).toBe(false);
		const barrier: OriginRef = { ...child, conversationId: `barrier-${messageId}` };
		const barrierId = `${messageId}-barrier`;
		expect((await send(barrier, barrierId, "Status?", "observer")).engaged).toBe(true);
		await until(() => db.inbox.messageDisposition(originKey(barrier), barrierId)?.message.turn_state === "done");
		const barrierSession = db.bindings.getSessionRecord(originKey(barrier))?.sessionId;
		expect(barrierSession).toBeTruthy();
		expect(port.sendAttempts.slice(attemptsBefore).every((input) => input.sessionId === barrierSession)).toBe(true);
		expect(db.inbox.messageDisposition(childKey, messageId)).toBeUndefined();
		expect(childBinds()).toBe(bindsBefore);
		expect(db.bindings.getSessionRecord(childKey)?.sessionId).toBeFalsy();
	};
}

/** Every Discord-bound frame the gateway pushes to the in-process adapter connection. */
function recordAdapterEvents(adapter: GajaewayClient) {
	const events: { event: string; origin?: string; text?: string }[] = [];
	for (const event of ["chat.message", "chat.progress", "monitor.event", "discord.thread.open"])
		adapter.on(event, (value) => {
			const payload = value as { origin?: OriginRef; text?: string };
			events.push({
				event,
				...(payload.origin ? { origin: originKey(payload.origin) } : {}),
				...(payload.text ? { text: payload.text } : {}),
			});
		});
	return events;
}

/**
 * Discord is reached only through frames to the in-process adapter: the pane opens it answered,
 * and ordinary chat frames. No other frame kind went out (nothing was fetched: see noNetwork).
 */
function expectAdapterOnly(events: readonly { event: string }[], paneOpens: number) {
	expect(events.filter((frame) => frame.event === "discord.thread.open")).toHaveLength(paneOpens);
	expect(events.every((frame) => ["discord.thread.open", "chat.message", "chat.progress"].includes(frame.event))).toBe(
		true,
	);
}

const SENTINEL = "owner-work.txt";
const SENTINEL_BODY = "uncommitted owner work\n";

/** A spawned command that could discard checkout work: git reset/merge/rm and kin, or a plain rm. */
function destructive(command: readonly string[]): boolean {
	const [executable = "", ...rest] = command;
	const base = executable.split("/").at(-1);
	if (base === "rm") return true;
	if (base !== "git") return false;
	if (rest.includes("worktree") && rest.includes("remove")) return true;
	return rest.some((arg) => ["reset", "merge", "rm", "clean", "restore", "stash", "checkout", "switch"].includes(arg));
}

/**
 * Watches one task checkout and the process's outside reach: every spawned argv and every fetch.
 * The gateway's only Discord surface in these fixtures is the in-process adapter connection.
 */
function watchCheckout(path: string, head: () => string) {
	const spawn = spyOn(Bun, "spawn");
	const spawnSync = spyOn(Bun, "spawnSync");
	const fetch = spyOn(globalThis, "fetch");
	const argv = (call: unknown[]): string[] => {
		const first = call[0] as string[] | { cmd: string[] };
		return [...(Array.isArray(first) ? first : first.cmd)].map(String);
	};
	const spawned = () => [...spawn.mock.calls, ...spawnSync.mock.calls].map((call) => argv(call as unknown[]));
	return {
		/** Positive control: the recorder sees the gateway's own dirty-checkout probe of this path. */
		sawDirtyProbe: () =>
			expect(
				spawn.mock.calls.some((call) => {
					const command = argv(call as unknown[]);
					return command.includes(path) && command.includes("--untracked-files=all");
				}),
			).toBe(true),
		write: () => Bun.write(join(path, SENTINEL), SENTINEL_BODY),
		/** The sentinel is still there with its bytes, HEAD is where it was, and nothing destructive ran. */
		intact: async (expectedHead: string) => {
			expect(await Bun.file(join(path, SENTINEL)).exists()).toBe(true);
			expect(await Bun.file(join(path, SENTINEL)).text()).toBe(SENTINEL_BODY);
			expect(head()).toBe(expectedHead);
			expect(spawned().filter(destructive)).toEqual([]);
		},
		noNetwork: () => expect(fetch).not.toHaveBeenCalled(),
		restore: () => {
			spawn.mockRestore();
			spawnSync.mockRestore();
			fetch.mockRestore();
		},
	};
}

test("pane wire records gateway creation, kicks off the child persona once, and waits for a running child turn before close", async () => {
	const directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-pane-wire-")));
	const repo = join(directory, "repo");
	await mkdir(repo);
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repo, ...args], {
			env: {
				...process.env,
				GIT_AUTHOR_NAME: "fixture",
				GIT_AUTHOR_EMAIL: "fixture@example.test",
				GIT_COMMITTER_NAME: "fixture",
				GIT_COMMITTER_EMAIL: "fixture@example.test",
			},
			stdout: "pipe",
			stderr: "pipe",
		});
		if (result.exitCode) throw new Error(result.stderr.toString());
		return result.stdout.toString().trim();
	};
	git("init", "-q");
	git("commit", "--allow-empty", "-qm", "fixture");
	const path = join(directory, "assigned");
	git("worktree", "add", "-qb", "firstmate/pane", path);
	const assignment: WorkFirstmateAssignmentParams = {
		taskId: "pane-task",
		name: "pane",
		ownerId: "owner",
		messageId: "assignment",
		thread: { platform: "discord", kind: "thread", conversationId: "parent-thread", parentId: "cockpit" },
		mandate: { id: "mandate", text: "Fix the scoped parser", checkRetries: false, commit: false, push: false },
		worktree: {
			path,
			commonDir: await realpath(git("-C", path, "rev-parse", "--path-format=absolute", "--git-common-dir")),
			gitDir: await realpath(git("-C", path, "rev-parse", "--absolute-git-dir")),
			branch: "firstmate/pane",
		},
	};
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "discord:cockpit": { engagement: "open", audience: "all" } },
	};
	const db = await Store.open(config.dbPath);
	const child: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "child-pane",
		parentId: "cockpit",
		threadVisibility: "public",
	};
	let childBinds = 0;
	let holdChildTurn = false;
	let heldChildOpRef: string | undefined;
	const directive = `[POST:${originKey(child)}] Owner pane note\n[WAKE:${originKey(child)}] Information only\n[WATCH:${originKey(child)}] 30m wait for human child input`;
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			if (input.sessionId === db.bindings.getSessionRecord("work/task/pane")?.sessionId) return;
			if (holdChildTurn && input.sessionId === db.bindings.getSessionRecord(originKey(child))?.sessionId) {
				holdChildTurn = false;
				heldChildOpRef = input.opRef;
				return;
			}
			scripted.complete(input.opRef, input.text.includes("Run pane directives") ? directive : "[SILENT]");
		},
		onBind: (input) => {
			if (input.originKey === originKey(child)) {
				expect(readChildPane(db, child)).toBeDefined();
				childBinds++;
			}
			return db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID();
		},
	});
	attachTestBrokerOwnership(db, port, join(directory, "agent"));
	const checks = spyOn(ghChecks, "ghCheckStatusClient").mockReturnValue({ getCheckStatus: async () => null });
	let server: GatewayServer | undefined;
	let adapter: GajaewayClient | undefined;
	let client: GajaewayClient | undefined;
	try {
		server = await startUnixServer({ config, database: db, sessionPort: port });
		adapter = await GajaewayClient.connectSocket(config.socketPath, { clientName: "adapter-discord" });
		let opens = 0;
		adapter.on("discord.thread.open", (value) => {
			const request = value as { requestId: string; parent: OriginRef };
			opens++;
			expect(request.parent).toEqual(assignment.thread);
			void adapter!.request("discord.thread.opened", { requestId: request.requestId, threadId: child.conversationId });
		});
		client = await GajaewayClient.connectSocket(config.socketPath);
		const start = await client.workStart({ firstmate: assignment });
		if (!start.started) throw new Error(start.reason);
		expect(opens).toBe(1);
		expect(readLanePane(db, "pane")).toMatchObject({
			taskId: "pane-task",
			childOrigin: child,
			createdByGateway: db.instanceId,
		});
		const childSends = () => {
			const sessionId = db.bindings.getSessionRecord(originKey(child))?.sessionId;
			return sessionId ? port.sendAttempts.filter((input) => input.sessionId === sessionId) : [];
		};
		// The recorded pane is kicked off through chat.send's own inbound path: exactly one
		// gateway-authored message, a fixed sentence naming the lane and never carrying task text.
		await until(() => childBinds === 1);
		expect(childBinds).toBe(1);
		await until(() => childSends().length === 1);
		const kickoffText = "The pane for FirstMate lane `pane` is open.";
		const kickoffRows = db.inbox.recentInbound(originKey(child), 50, new Date(0).toISOString());
		expect(kickoffRows).toHaveLength(1);
		expect(kickoffRows[0]?.body).toBe(kickoffText);
		expect(db.inbox.messageDisposition(originKey(child), "firstmate-pane-kickoff:pane")).toBeDefined();
		const personaDirective = (where: OriginRef, messageId: string) =>
			client!.request("chat.send", {
				origin: where,
				messageId,
				text: "Run pane directives",
				engagement: { mentioned: true, group: true, authorId: "owner" },
			});
		await personaDirective(assignment.thread, "owner-directives");
		await until(() => db.inbox.watchRows().length === 1);
		await until(() =>
			db.outbox
				.rows()
				.some((row) => row.origin_key === originKey(child) && JSON.parse(row.payload_json).text === "Owner pane note"),
		);
		expect(
			db.outbox
				.rows()
				.filter((row) => row.origin_key === originKey(child))
				.every((row) => JSON.parse(row.payload_json).noChildFallback === true),
		).toBe(true);
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);
		// The owner's WAKE follows the gateway's kickoff turn on the same ordinary persona.
		await until(() => childSends().length === 2);
		expect(childBinds).toBe(1);
		const ordinaryThread = { ...child, conversationId: "ordinary-thread" };
		await personaDirective(ordinaryThread, "unowned-directives");
		await until(
			() => db.inbox.messageDisposition(originKey(ordinaryThread), "unowned-directives")?.message.turn_state === "done",
		);
		expect(db.inbox.watchRows()).toHaveLength(1);
		expect(
			db.outbox
				.rows()
				.some(
					(row) =>
						row.origin_key === originKey(ordinaryThread) && JSON.parse(row.payload_json).text.includes("not_a_channel"),
				),
		).toBe(true);
		const send = (messageId: string, text = "Do not change the public API") =>
			client!.request<{ engaged: boolean }>("chat.send", {
				origin: child,
				messageId,
				text,
				engagement: { mentioned: false, group: true, authorId: "human" },
			});
		// An unmentioned human line in the pane reaches its own persona through ordinary admission.
		expect((await send("human")).engaged).toBe(true);
		await until(() => childSends().length === 3);
		expect(childBinds).toBe(1);
		const preamble = childSends()[2]!.systemPreamble ?? "";
		expect(preamble).toContain("FirstMate crewmate pane");
		expect(preamble).toContain("task pane-task, lane `pane`");
		expect(preamble).toContain(`[WAKE:${originKey(assignment.thread)}]`);
		expect(preamble).toContain("never run `work start --firstmate-correction`, `work approve`");
		// The gateway no longer turns the line into a work steer; the persona drives the lane.
		expect(port.steers).toHaveLength(0);
		expect(db.inbox.watchRows()[0]).toMatchObject({ state: "fired", triggered_message_id: "human" });
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);
		const binding = db.bindings.getSessionRecord(originKey(child));
		expect(binding?.sessionId).toBeTruthy();
		await client.close();
		await adapter.close();
		await server.stop();
		server = await startUnixServer({ config, database: db, sessionPort: port });
		client = await GajaewayClient.connectSocket(config.socketPath);
		expect(await client.workStart({ firstmate: assignment })).toEqual(start);
		expect(opens).toBe(1);
		// Repeat open never duplicates the kickoff inbound.
		expect(
			db.inbox.recentInbound(originKey(child), 50, new Date(0).toISOString()).filter((row) => row.body === kickoffText),
		).toHaveLength(1);
		expect((await send("after-restart", "Status?")).engaged).toBe(true);
		await until(() => childSends().length === 4);
		expect(db.bindings.getSessionRecord(originKey(child))).toEqual(binding);
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);
		const childSessionId = db.bindings.getSessionRecord(originKey(child))?.sessionId;
		expect(childSessionId).toBeTruthy();
		const childTurns = () => port.sendAttempts.filter((input) => input.sessionId === childSessionId).length;
		// Settle the lane attempt: release requires a settled outcome.
		port.complete(start.opRef, "final local result");
		await until(() => Boolean(db.work.workAttemptGet(start.opRef)?.settledAt));
		await port.runExclusive(start.sessionKey, async () => {});
		const attempt = db.work.workAttemptGet(start.opRef)!;
		// The settled outcome is one native persona report to the task's parent thread, not a second post.
		expect(attempt.decision).toBe("reported");
		expect(db.outbox.get(attempt.deliveryId)).toBeUndefined();
		expect(db.inbox.messageDisposition(originKey(assignment.thread), attempt.reportId)?.message).toMatchObject({
			source: "lane_report",
			body: expect.stringContaining(`inspect: work inspect pane --task pane-task --op ${start.opRef}`),
		});
		await until(() => db.inbox.reportConsumed(originKey(assignment.thread), attempt.reportId));
		const observed = await client.workInspect({
			name: assignment.name,
			taskId: assignment.taskId,
			opRef: start.opRef,
		});
		expect(observed.evidence.completeness).toBe("complete");
		const evidence = observed.evidence;
		if (
			evidence.completeness !== "complete" ||
			evidence.text === undefined ||
			evidence.source === undefined ||
			evidence.provenance === undefined
		)
			throw new Error("expected complete attributed lane evidence");
		if (!observed.identity) throw new Error("expected lane identity");
		const { sessionKey: _sessionKey, ...identity } = observed.identity;
		const decisionBinding = {
			name: assignment.name,
			identity,
			ownerId: assignment.ownerId,
			messageId: "approval-message",
			thread: assignment.thread,
			mandateId: assignment.mandate.id,
			scope: "Accept only the scoped local result",
		};
		const approved = await client.workApprove({
			...decisionBinding,
			decision: "accept_outcome",
			evidence: {
				completeness: evidence.completeness,
				text: evidence.text,
				source: evidence.source,
				provenance: evidence.provenance,
				reasons: observed.reasons,
			},
		});
		expect(approved.recorded).toBe(true);
		if (!approved.recorded) throw new Error(approved.reason);
		// Active-turn safety: retiring waits for the child's running turn instead of killing it.
		holdChildTurn = true;
		expect((await send("hold-the-turn-open")).engaged).toBe(true);
		await until(() => heldChildOpRef !== undefined);
		let retireReturned = false;
		const retiring = client
			.workRetire({
				firstmate: {
					...decisionBinding,
					messageId: "release-message",
					approvalId: approved.approval.approvalId,
					action: "release_session_preserve_work",
				},
			})
			.then((result) => {
				retireReturned = true;
				return result;
			});
		await Bun.sleep(100);
		expect(retireReturned).toBe(false);
		expect(port.closes.filter((close) => close.sessionId === childSessionId)).toHaveLength(0);
		port.complete(heldChildOpRef!, "[SILENT]");
		expect(await retiring).toMatchObject({ retired: true });
		expect(port.closes.filter((close) => close.sessionId === childSessionId)).toHaveLength(1);
		expect(paneClosed(db, child)).toBe(true);
		expect((await send("after-retire")).engaged).toBe(false);
		expect(db.bindings.getSessionRecord(originKey(child))?.sessionId).toBeFalsy();
		expect(childTurns()).toBe(5);
		expect(port.steers).toHaveLength(0);
	} finally {
		await client?.close();
		await adapter?.close();
		await server?.stop();
		checks.mockRestore();
		db.close();
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);

test("FirstMate journey without Discord: assign, child message, close, follow-up, approval, closeout", async () => {
	const directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-journey-")));
	const repo = join(directory, "repo");
	await mkdir(repo);
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repo, ...args], {
			env: {
				...process.env,
				GIT_AUTHOR_NAME: "fixture",
				GIT_AUTHOR_EMAIL: "fixture@example.test",
				GIT_COMMITTER_NAME: "fixture",
				GIT_COMMITTER_EMAIL: "fixture@example.test",
			},
			stdout: "pipe",
			stderr: "pipe",
		});
		if (result.exitCode) throw new Error(result.stderr.toString());
		return result.stdout.toString().trim();
	};
	git("init", "-q");
	git("commit", "--allow-empty", "-qm", "fixture");
	const path = join(directory, "assigned");
	git("worktree", "add", "-qb", "firstmate/journey", path);
	const thread: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "parent-thread",
		parentId: "cockpit",
	};
	const assignment: WorkFirstmateAssignmentParams = {
		taskId: "journey-task",
		name: "journey",
		ownerId: "owner",
		messageId: "assignment",
		thread,
		mandate: { id: "mandate", text: "Fix the scoped parser", checkRetries: false, commit: false, push: false },
		worktree: {
			path,
			commonDir: await realpath(git("-C", path, "rev-parse", "--path-format=absolute", "--git-common-dir")),
			gitDir: await realpath(git("-C", path, "rev-parse", "--absolute-git-dir")),
			branch: "firstmate/journey",
		},
	};
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "discord:cockpit": { engagement: "open", audience: "all" } },
	};
	const db = await Store.open(config.dbPath);
	const child: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "child-journey",
		parentId: "cockpit",
		threadVisibility: "public",
	};
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			// The lane turn settles when the test says so; every persona turn stays silent.
			if (input.sessionId === db.bindings.getSessionRecord("work/task/journey")?.sessionId) return;
			scripted.complete(input.opRef, "[SILENT]");
		},
		onBind: (input) => db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID(),
	});
	attachTestBrokerOwnership(db, port, join(directory, "agent"));
	const checks = spyOn(ghChecks, "ghCheckStatusClient").mockReturnValue({ getCheckStatus: async () => null });
	const head = () => git("-C", path, "rev-parse", "HEAD");
	const checkout = watchCheckout(path, head);
	let server: GatewayServer | undefined;
	let adapter: GajaewayClient | undefined;
	let client: GajaewayClient | undefined;
	try {
		server = await startUnixServer({ config, database: db, sessionPort: port });
		// The fake adapter only answers the gateway's pane-open request; nothing reaches Discord.
		adapter = await GajaewayClient.connectSocket(config.socketPath, { clientName: "adapter-discord" });
		const adapterEvents = recordAdapterEvents(adapter);
		adapter.on("discord.thread.open", (value) => {
			const request = value as { requestId: string };
			void adapter!.request("discord.thread.opened", { requestId: request.requestId, threadId: child.conversationId });
		});
		client = await GajaewayClient.connectSocket(config.socketPath);
		const send = (where: OriginRef, messageId: string, text: string, authorId: string) =>
			client!.request<{ engaged: boolean }>("chat.send", {
				origin: where,
				messageId,
				text,
				engagement: { mentioned: where === thread, group: true, authorId },
			});
		const childSessionId = () => db.bindings.getSessionRecord(originKey(child))?.sessionId;
		const childTurns = () => port.sendAttempts.filter((input) => input.sessionId === childSessionId()).length;
		const expectNoPersona = noPersona(db, port, () => client!, child);

		// 1. Assign: the lane starts and the gateway records the open child pane.
		const start = await client.workStart({ firstmate: assignment });
		if (!start.started) throw new Error(start.reason);
		expect(start.sessionKey).toBe("work/task/journey");
		expect(readLanePane(db, "journey")).toMatchObject({ taskId: "journey-task", childOrigin: child });
		expect(paneClosed(db, child)).toBe(false);
		await until(() => childTurns() === 1);
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);

		// 2. A message into the open child reaches its persona.
		expect((await send(child, "child-note", "Do not change the public API", "human")).engaged).toBe(true);
		await until(() => childTurns() === 2);
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);
		const openChildSession = childSessionId();
		expect(openChildSession).toBeTruthy();

		// Settle the lane attempt and confirm its parent delivery.
		port.complete(start.opRef, "final local result");
		await until(() => Boolean(db.work.workAttemptGet(start.opRef)?.settledAt));
		await port.runExclusive(start.sessionKey, async () => {});
		const attempt = db.work.workAttemptGet(start.opRef)!;
		await until(() => db.outbox.get(attempt.deliveryId) !== undefined);
		db.transaction((tx) => db.outbox.confirm(tx, attempt.deliveryId));
		// The settled attempt is the one open approval hold.
		expect(db.work.firstmate.approvalHolds("journey")).toMatchObject([
			{ state: "open", taskId: "journey-task", opRef: start.opRef, ordinal: 0, sessionId: start.sessionId },
		]);

		// A start for another task on the live checkout is refused and touches nothing.
		const assignedHead = head();
		await checkout.write();
		await expect(
			client.workStart({
				firstmate: {
					...assignment,
					taskId: "rival-task",
					name: "rival",
					messageId: "rival-assignment",
					thread: { ...thread, conversationId: "rival-thread" },
				},
			}),
		).rejects.toMatchObject({ detail: { reason: "immutable_assignment_conflict" } });
		expect(db.work.firstmate.assignment("rival")).toBeUndefined();
		await checkout.intact(assignedHead);

		// 3a. Abandoning a dirty checkout is refused: nothing closes, nothing is discarded.
		await expect(client.request("work.retire", { abandon: "journey" })).rejects.toMatchObject({
			message: expect.stringContaining("dirty checkout"),
		});
		expect(db.work.firstmate.abandoned("journey")).toBe(false);
		expect(paneClosed(db, child)).toBe(false);
		expect(childSessionId()).toBe(openChildSession);
		expect(db.bindings.getSessionRecord(start.sessionKey)?.sessionId).toBe(start.sessionId);
		checkout.sawDirtyProbe();
		await checkout.intact(assignedHead);
		// The owner keeps the work by committing it; the sentinel stays on disk.
		git("-C", path, "add", SENTINEL);
		git("-C", path, "commit", "-qm", "owner work");
		const keptHead = head();
		expect(keptHead).not.toBe(assignedHead);

		// 3. Close: abandonment closes the pane and the child persona's session.
		expect(await client.request<{ abandoned: boolean; name: string }>("work.retire", { abandon: "journey" })).toEqual({
			abandoned: true,
			name: "journey",
		});
		expect(paneClosed(db, child)).toBe(true);
		expect(childSessionId()).toBeFalsy();
		expect(port.closes.filter((close) => close.sessionId === openChildSession)).toHaveLength(1);
		await checkout.intact(keptHead);
		const framesAtClose = adapterEvents.length;

		// 4. A follow-up after close starts no persona.
		await expectNoPersona("follow-up", "One more thing");

		// 4b. A follow-up correction of the abandoned task refuses before any session binds.
		const bindsBeforeCorrection = port.binds.length;
		await expect(
			client.workStart({
				firstmateCorrection: {
					name: "journey",
					taskId: "journey-task",
					ownerId: "owner",
					messageId: "follow-up-correction",
					thread,
					mandateId: "mandate",
					priorOpRef: start.opRef,
					text: "Also handle empty input.",
				},
			}),
		).rejects.toMatchObject({ detail: { reason: "task_abandoned" } });
		expect(port.binds).toHaveLength(bindsBeforeCorrection);
		expect(db.work.firstmate.attemptsForTask("journey-task")).toHaveLength(1);
		expect(db.bindings.getSessionRecord(start.sessionKey)?.sessionId).toBe("");

		// 5. An abandoned task stays closed. The owner's words do not grow an approval.
		const scope = "Accept only the scoped local result";
		expect((await send(thread, "approval-message", scope, "owner")).engaged).toBe(true);
		const approved = await client.request<WorkApproveResult>("work.approve.decision", {
			name: "journey",
			messageId: "approval-message",
			decision: "accept_outcome",
		});
		expect(approved).toMatchObject({ recorded: false, held: true, reason: "approval_abandoned" });
		expect(db.work.firstmate.approvalForMessage("owner", "approval-message")).toBeUndefined();
		expect(db.work.firstmate.finishHolds("journey")).toEqual([]);

		// 6. Closeout of an abandoned task has no finish hold to answer and is refused.
		await until(
			() => db.inbox.messageDisposition(originKey(thread), "approval-message")?.message.turn_state === "done",
		);
		expect((await send(thread, "closeout-message", "Close it out", "owner")).engaged).toBe(true);
		const closeout = await client.request<WorkFirstmateRetireResult>("work.retire.decision", {
			name: "journey",
			messageId: "closeout-message",
		});
		expect(closeout).toMatchObject({ retired: false, held: true, reason: "release_finish_hold_missing" });
		expect(db.work.firstmate.releaseForMessage("owner", "closeout-message")).toBeUndefined();
		await expectNoPersona("after-closeout", "Still there?");
		expect(port.steers).toHaveLength(0);
		await checkout.intact(keptHead);
		expectAdapterOnly(adapterEvents, 1);
		// Nothing reached the closed pane after its close.
		expect(adapterEvents.slice(framesAtClose).filter((frame) => frame.origin === originKey(child))).toEqual([]);
		checkout.noNetwork();
	} finally {
		await client?.close();
		await adapter?.close();
		await server?.stop();
		checks.mockRestore();
		checkout.restore();
		db.close();
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);

test("FirstMate lane closed without abandonment: approval recorded, closeout closes pane and lane and retires the release", async () => {
	const directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-closed-lane-")));
	const repo = join(directory, "repo");
	await mkdir(repo);
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repo, ...args], {
			env: {
				...process.env,
				GIT_AUTHOR_NAME: "fixture",
				GIT_AUTHOR_EMAIL: "fixture@example.test",
				GIT_COMMITTER_NAME: "fixture",
				GIT_COMMITTER_EMAIL: "fixture@example.test",
			},
			stdout: "pipe",
			stderr: "pipe",
		});
		if (result.exitCode) throw new Error(result.stderr.toString());
		return result.stdout.toString().trim();
	};
	git("init", "-q");
	git("commit", "--allow-empty", "-qm", "fixture");
	const path = join(directory, "assigned");
	git("worktree", "add", "-qb", "firstmate/closed", path);
	const thread: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "parent-thread",
		parentId: "cockpit",
	};
	// Another thread under the same channel: the owner's words there answer no hold.
	const foreign: OriginRef = { ...thread, conversationId: "foreign-thread" };
	const assignment: WorkFirstmateAssignmentParams = {
		taskId: "closed-task",
		name: "closed",
		ownerId: "owner",
		messageId: "assignment",
		thread,
		mandate: { id: "mandate", text: "Fix the scoped parser", checkRetries: false, commit: false, push: false },
		worktree: {
			path,
			commonDir: await realpath(git("-C", path, "rev-parse", "--path-format=absolute", "--git-common-dir")),
			gitDir: await realpath(git("-C", path, "rev-parse", "--absolute-git-dir")),
			branch: "firstmate/closed",
		},
	};
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "discord:cockpit": { engagement: "open", audience: "all" } },
	};
	const db = await Store.open(config.dbPath);
	const child: OriginRef = {
		platform: "discord",
		kind: "thread",
		conversationId: "child-closed",
		parentId: "cockpit",
		threadVisibility: "public",
	};
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			if (input.sessionId === db.bindings.getSessionRecord("work/task/closed")?.sessionId) return;
			scripted.complete(input.opRef, "[SILENT]");
		},
		onBind: (input) => db.bindings.getSessionRecord(input.originKey)?.sessionId || crypto.randomUUID(),
	});
	attachTestBrokerOwnership(db, port, join(directory, "agent"));
	const checks = spyOn(ghChecks, "ghCheckStatusClient").mockReturnValue({ getCheckStatus: async () => null });
	const head = () => git("-C", path, "rev-parse", "HEAD");
	const checkout = watchCheckout(path, head);
	let server: GatewayServer | undefined;
	let adapter: GajaewayClient | undefined;
	let client: GajaewayClient | undefined;
	try {
		server = await startUnixServer({ config, database: db, sessionPort: port });
		adapter = await GajaewayClient.connectSocket(config.socketPath, { clientName: "adapter-discord" });
		const adapterEvents = recordAdapterEvents(adapter);
		let paneOpens = 0;
		adapter.on("discord.thread.open", (value) => {
			const request = value as { requestId: string };
			// The first pane is the closed lane's; a later assignment gets its own pane.
			const threadId = paneOpens++ === 0 ? child.conversationId : `child-next-${paneOpens}`;
			void adapter!.request("discord.thread.opened", { requestId: request.requestId, threadId });
		});
		client = await GajaewayClient.connectSocket(config.socketPath);
		const send = (where: OriginRef, messageId: string, text: string, authorId: string) =>
			client!.request<{ engaged: boolean }>("chat.send", {
				origin: where,
				messageId,
				text,
				engagement: { mentioned: where === thread || where === foreign, group: true, authorId },
			});
		const turnDone = (where: OriginRef, messageId: string) =>
			until(() => db.inbox.messageDisposition(originKey(where), messageId)?.message.turn_state === "done");
		const childSessionId = () => db.bindings.getSessionRecord(originKey(child))?.sessionId;
		const childTurns = () => port.sendAttempts.filter((input) => input.sessionId === childSessionId()).length;
		const owner = db.work.firstmate;
		const expectNoPersona = noPersona(db, port, () => client!, child);

		const start = await client.workStart({ firstmate: assignment });
		if (!start.started) throw new Error(start.reason);
		expect(readLanePane(db, "closed")).toMatchObject({ taskId: "closed-task", childOrigin: child });
		await until(() => childTurns() === 1);
		await until(() => db.inbox.pendingOldest(originKey(child)) === undefined);
		const openChildSession = childSessionId();
		expect(openChildSession).toBeTruthy();

		port.complete(start.opRef, "final local result");
		await until(() => Boolean(db.work.workAttemptGet(start.opRef)?.settledAt));
		await port.runExclusive(start.sessionKey, async () => {});
		const attempt = db.work.workAttemptGet(start.opRef)!;
		await until(() => db.outbox.get(attempt.deliveryId) !== undefined);
		db.transaction((tx) => db.outbox.confirm(tx, attempt.deliveryId));

		// The settled lane stays bound and its pane stays open until the owner's closeout.
		const laneSession = db.bindings.getSessionRecord(start.sessionKey)!;
		expect(laneSession.sessionId).toBe(start.sessionId);
		expect(paneClosed(db, child)).toBe(false);

		// The settled attempt is the lane's one open approval hold; nothing is finishable yet.
		expect(owner.approvalHolds("closed")).toMatchObject([
			{
				state: "open",
				taskId: "closed-task",
				opRef: start.opRef,
				ordinal: 0,
				sessionId: laneSession.sessionId,
				epoch: laneSession.epoch,
			},
		]);
		expect(owner.finishHolds("closed")).toEqual([]);
		const approve = (messageId: string) =>
			client!.request<WorkApproveResult>("work.approve.decision", {
				name: "closed",
				messageId,
				decision: "accept_outcome",
			});
		const closeoutBy = (messageId: string) =>
			client!.request<WorkFirstmateRetireResult>("work.retire.decision", { name: "closed", messageId });

		// A closeout before any approval answers no finish hold.
		expect((await send(thread, "early-closeout", "Close it out now", "owner")).engaged).toBe(true);
		await turnDone(thread, "early-closeout");
		expect(await closeoutBy("early-closeout")).toMatchObject({
			retired: false,
			held: true,
			reason: "release_finish_hold_missing",
		});
		expect(owner.releaseForMessage("owner", "early-closeout")).toBeUndefined();

		// Only the owner's message in the assignment thread approves.
		const scope = "Accept only the scoped local result";
		expect((await send(foreign, "foreign-approval", scope, "owner")).engaged).toBe(true);
		await turnDone(foreign, "foreign-approval");
		expect(await approve("foreign-approval")).toMatchObject({ recorded: false, reason: "approval_origin_mismatch" });
		expect((await send(thread, "stranger-approval", scope, "stranger")).engaged).toBe(true);
		await turnDone(thread, "stranger-approval");
		expect(await approve("stranger-approval")).toMatchObject({
			recorded: false,
			reason: "approval_authority_mismatch",
		});

		// The wire refuses to pick between two open approval holds. Production never opens a second
		// one (a later settled attempt supersedes the earlier hold), so this plants damaged durable
		// state to reach the refusal; it proves nothing about how holds are produced.
		expect((await send(thread, "approval-message", scope, "owner")).engaged).toBe(true);
		await turnDone(thread, "approval-message");
		const [hold] = owner.approvalHolds("closed");
		const planted = { ...hold!, key: `${hold!.key.slice(0, -10)}${"9".padStart(10, "0")}`, ordinal: 9 };
		db.core.metaSet(planted.key, JSON.stringify(planted));
		expect(await approve("approval-message")).toMatchObject({ recorded: false, reason: "approval_hold_ambiguous" });
		expect(owner.approvalForMessage("owner", "approval-message")).toBeUndefined();
		db.core.metaSet(planted.key, JSON.stringify({ ...planted, state: "superseded" }));

		// The one open hold is approved by the owner's words; the stored body is the scope.
		const approved = await approve("approval-message");
		expect(approved).toMatchObject({
			recorded: true,
			approval: { messageId: "approval-message", scope, identity: { opRef: start.opRef, epoch: laneSession.epoch } },
		});
		if (!approved.recorded) throw new Error(approved.reason);
		expect(approved.approval.scope).toBe(scope);
		expect(await approve("approval-message")).toEqual(approved);
		expect(owner.approvalHolds("closed")[0]).toMatchObject({
			state: "resolved",
			opRef: start.opRef,
			approvalId: approved.approval.approvalId,
			messageId: "approval-message",
		});
		expect(owner.finishHolds("closed")).toMatchObject([
			{ state: "open", approvalId: approved.approval.approvalId, approvalMessageId: "approval-message" },
		]);

		// Closeout reads a later owner message in the assignment thread, never the approval itself
		// or a message from another thread.
		expect(await closeoutBy("approval-message")).toMatchObject({ retired: false, reason: "release_message_reused" });
		// The owner's earlier message meets an open finish hold now, yet it predates the approval.
		expect(await closeoutBy("early-closeout")).toMatchObject({ retired: false, reason: "release_message_not_later" });
		expect((await send(foreign, "foreign-closeout", "Close it out", "owner")).engaged).toBe(true);
		await turnDone(foreign, "foreign-closeout");
		expect(await closeoutBy("foreign-closeout")).toMatchObject({ retired: false, reason: "release_origin_mismatch" });
		expect(owner.releaseForMessage("owner", "foreign-closeout")).toBeUndefined();
		expect(owner.finishHolds("closed")[0]?.state).toBe("open");

		const closeoutBody = "Close it out; keep the branch as it is.";
		expect((await send(thread, "closeout-message", closeoutBody, "owner")).engaged).toBe(true);
		await turnDone(thread, "closeout-message");

		// A preserving release of a dirty checkout is refused: nothing is recorded, closed or discarded.
		const assignedHead = head();
		await checkout.write();
		const dirty = await closeoutBy("closeout-message");
		expect(dirty).toMatchObject({ retired: false, held: true, reason: expect.stringContaining("dirty checkout") });
		expect(owner.releaseForMessage("owner", "closeout-message")).toBeUndefined();
		expect(owner.finishHolds("closed")[0]?.state).toBe("open");
		expect(paneClosed(db, child)).toBe(false);
		expect(childSessionId()).toBe(openChildSession);
		expect(db.bindings.getSessionRecord(start.sessionKey)).toMatchObject({
			sessionId: laneSession.sessionId,
			epoch: laneSession.epoch,
		});
		expect(port.closes).toEqual([]);
		checkout.sawDirtyProbe();
		await checkout.intact(assignedHead);
		// The owner keeps the work by committing it; the sentinel stays on disk.
		git("-C", path, "add", SENTINEL);
		git("-C", path, "commit", "-qm", "owner work");
		const keptHead = head();
		expect(keptHead).not.toBe(assignedHead);

		// The closeout is the product close: it closes the pane before unbinding its persona, closes
		// the lane's session and empties its binding at a rotated epoch. No abandonment is recorded.
		const closeout = await closeoutBy("closeout-message");
		expect(closeout).toMatchObject({
			retired: true,
			release: { preserved: true, sessionKey: start.sessionKey, body: closeoutBody },
		});
		if (!closeout.retired) throw new Error(closeout.reason);
		expect(paneClosed(db, child)).toBe(true);
		expect(childSessionId()).toBeFalsy();
		expect(port.closes.filter((close) => close.sessionId === openChildSession)).toHaveLength(1);
		expect(port.closes.filter((close) => close.sessionId === laneSession.sessionId)).toHaveLength(1);
		expect(db.bindings.getSessionRecord(start.sessionKey)).toMatchObject({
			sessionId: "",
			epoch: laneSession.epoch + 1,
		});
		expect(owner.abandoned("closed")).toBe(false);
		await checkout.intact(keptHead);
		const framesAtClose = adapterEvents.length;
		// A message after pane close starts no persona.
		await expectNoPersona("after-close", "One more thing");
		const release = owner.releaseForMessage("owner", "closeout-message");
		expect(release?.body).toBe(closeoutBody);
		expect(owner.releaseReceipt(release!.releaseId)).toMatchObject({ preserved: true, body: closeoutBody });
		expect(owner.finishHolds("closed")[0]).toMatchObject({
			state: "resolved",
			releaseId: release!.releaseId,
			messageId: "closeout-message",
		});
		expect(db.bindings.getSessionRecord(start.sessionKey)?.sessionId).toBe("");
		expect(await closeoutBy("closeout-message")).toEqual(closeout);
		// No open finish hold remains: a further owner message finishes nothing.
		expect((await send(thread, "late-closeout", "Close it again", "owner")).engaged).toBe(true);
		await turnDone(thread, "late-closeout");
		expect(await closeoutBy("late-closeout")).toMatchObject({
			retired: false,
			reason: "release_finish_hold_missing",
		});

		// A follow-up correction of the released task refuses before any session binds.
		const bindsBeforeCorrection = port.binds.length;
		await expect(
			client.workStart({
				firstmateCorrection: {
					name: "closed",
					taskId: "closed-task",
					ownerId: "owner",
					messageId: "follow-up-correction",
					thread,
					mandateId: "mandate",
					priorOpRef: start.opRef,
					text: "Also handle empty input.",
				},
			}),
		).rejects.toMatchObject({ detail: { reason: "task_released" } });
		expect(port.binds).toHaveLength(bindsBeforeCorrection);
		expect(owner.attemptsForTask("closed-task")).toHaveLength(1);

		// Still no persona after the repeated and late closeouts.
		await expectNoPersona("after-closeout", "Still there?");
		expect(port.steers).toHaveLength(0);
		await checkout.intact(keptHead);

		expect((await send(thread, "merge-class", "Merge once green and reviewed.", "owner")).engaged).toBe(true);
		await turnDone(thread, "merge-class");
		const authorized = await client.request("work.merge.authorize", { name: "closed", messageId: "merge-class" });
		expect(authorized).toEqual({ recorded: true, messageId: "merge-class" });

		// The clean released checkout is free for a new assignment; while that task is live,
		// another task still conflicts.
		expect(git("-C", path, "status", "--porcelain", "--untracked-files=all")).toBe("");
		const next: WorkFirstmateAssignmentParams = {
			...assignment,
			taskId: "next-task",
			name: "next",
			messageId: "next-assignment",
		};
		const nextStart = await client.workStart({ firstmate: next });
		if (!nextStart.started) throw new Error(nextStart.reason);
		expect(owner.assignment("next")?.worktree.path).toBe(path);
		// The released task's review and merge never read the checkout the live next task owns.
		const caller = db.bindings.getSessionRecord(originKey(thread))?.sessionId;
		expect(caller).toBeTruthy();
		const review = { name: "closed", sessionId: caller, headSha: keptHead, verdict: "fail" };
		expect(await client.request("work.review", review)).toMatchObject({ reason: "review_checkout_reassigned" });
		expect(owner.reviews("closed")).toEqual([]);
		const merge = await client.request("work.merge", { name: "closed", sessionId: caller });
		expect(merge).toMatchObject({ merged: false, held: true, reason: "merge_checkout_reassigned" });
		expect(owner.merges("closed")).toEqual([]);
		await expect(
			client.workStart({
				firstmate: {
					...assignment,
					taskId: "third-task",
					name: "third",
					messageId: "third-assignment",
					thread: { ...thread, conversationId: "third-thread" },
				},
			}),
		).rejects.toMatchObject({ detail: { reason: "immutable_assignment_conflict" } });
		expect(owner.assignment("third")).toBeUndefined();
		await checkout.intact(keptHead);
		// Two pane opens (closed, next), both answered in process; the refused third opened none.
		expectAdapterOnly(adapterEvents, 2);
		expect(adapterEvents.slice(framesAtClose).filter((frame) => frame.origin === originKey(child))).toEqual([]);
		checkout.noNetwork();
	} finally {
		await client?.close();
		await adapter?.close();
		await server?.stop();
		checks.mockRestore();
		checkout.restore();
		db.close();
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);

import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey, type WorkFirstmateAssignmentParams } from "@gajae-gateway/protocol";
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

test("pane wire records gateway creation, kicks off the child persona once, and keeps that persona closed after retirement", async () => {
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
		// Settle the lane attempt: release and abandonment both require a settled outcome.
		port.complete(start.opRef, "final local result");
		await until(() => Boolean(db.work.workAttemptGet(start.opRef)?.settledAt));
		await port.runExclusive(start.sessionKey, async () => {});
		const attempt = db.work.workAttemptGet(start.opRef)!;
		// Preserving release waits for the outcome's confirmed parent delivery.
		await until(() => db.outbox.get(attempt.deliveryId) !== undefined);
		db.transaction((tx) => db.outbox.confirm(tx, attempt.deliveryId));
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
		// Retirement closed the child's ordinary persona session and unbound it.
		expect(port.closes.filter((close) => close.sessionId === childSessionId)).toHaveLength(1);
		expect(db.bindings.getSessionRecord(originKey(child))?.sessionId).toBeFalsy();
		expect(paneClosed(db, child)).toBe(true);
		// A message after close does not start another persona.
		expect((await send("after-retire")).engaged).toBe(false);
		expect(db.bindings.getSessionRecord(originKey(child))?.sessionId).toBeFalsy();
		expect(childTurns()).toBe(5);
		// Abandonment of an already closed pane succeeds and still does not rebind.
		expect(await client.request("work.retire", { abandon: assignment.name })).toEqual({
			abandoned: true,
			name: assignment.name,
		});
		expect((await send("after-abandon")).engaged).toBe(false);
		expect(db.bindings.getSessionRecord(originKey(child))?.sessionId).toBeFalsy();
		expect(port.closes.filter((close) => close.sessionId === childSessionId)).toHaveLength(1);
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

import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey, type OriginRef, type WorkFirstmateAssignmentParams } from "@gajae-gateway/protocol";
import { GajaewayClient } from "../../sdk/src/client";
import type { GatewayConfig } from "../src/config";
import { readChildPane, readLanePane } from "../src/orchestrator/firstmate-pane";
import * as ghChecks from "../src/orchestrator/gh-checks";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

async function until(predicate: () => boolean) {
	for (let index = 0; index < 2000 && !predicate(); index++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

test("pane wire persists gateway creation before actor bind, rejects bots, steers humans once and reloads on restart", async () => {
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
	const directive = `[POST:${originKey(child)}] Owner pane note\n[WAKE:${originKey(child)}] Information only\n[WATCH:${originKey(child)}] 30m wait for human child input`;
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => {
			if (input.sessionId === db.bindings.getSessionRecord("work/task/pane")?.sessionId) return;
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
		expect(childBinds).toBe(1);
		expect(readLanePane(db, "pane")).toMatchObject({
			taskId: "pane-task",
			childOrigin: child,
			createdByGateway: db.instanceId,
		});
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
		const send = (messageId: string, authorIsBot = false) =>
			client!.request<{ engaged: boolean }>("chat.send", {
				origin: child,
				messageId,
				text: "Do not change the public API",
				engagement: { mentioned: true, group: true, authorId: authorIsBot ? "other-bot" : "human", authorIsBot },
			});
		expect((await send("bot", true)).engaged).toBe(false);
		expect(port.steers).toHaveLength(0);
		expect(db.inbox.watchRows()[0]?.state).toBe("pending");
		expect((await send("human")).engaged).toBe(true);
		expect((await send("human")).engaged).toBe(true);
		expect(port.steers).toHaveLength(1);
		expect(db.inbox.watchRows()[0]).toMatchObject({ state: "fired", triggered_message_id: "human" });
		expect(db.inbox.pendingOldest(originKey(child))).toBeUndefined();
		const binding = db.bindings.getSessionRecord(originKey(child));
		await client.close();
		await adapter.close();
		await server.stop();
		server = await startUnixServer({ config, database: db, sessionPort: port });
		await until(() => childBinds === 2);
		expect(db.bindings.getSessionRecord(originKey(child))).toEqual(binding);
		client = await GajaewayClient.connectSocket(config.socketPath);
		expect(await client.workStart({ firstmate: assignment })).toEqual(start);
		expect(opens).toBe(1);
		db.core.metaSet(`firstmate-pane/child/${originKey(child)}`, "corrupt");
		expect((await send("after-corruption")).engaged).toBe(false);
		expect(port.steers).toHaveLength(1);
		expect(db.work.workAttemptGet(start.opRef)?.settledAt).toBeNull();
		port.complete(start.opRef, "final local result");
		await until(() => Boolean(db.work.workAttemptGet(start.opRef)?.settledAt));
	} finally {
		await client?.close();
		await adapter?.close();
		await server?.stop();
		checks.mockRestore();
		db.close();
		await rm(directory, { recursive: true, force: true });
	}
}, 20_000);

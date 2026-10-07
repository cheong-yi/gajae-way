import { Database } from "bun:sqlite";
import { afterEach, expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	LOOPBACK_ORIGIN,
	originKey,
	PROFILE_VERSION,
	validateWorkTaskDispositionBasis,
	validateWorkTaskDispositionBasisResult,
	validateWorkTaskDispositionResult,
	type WorkTaskDispositionBasis,
	type WorkTaskDispositionParams,
} from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase, workTaskDispositionId, workTaskSourceDeliveryId } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const close of cleanup.splice(0).reverse()) await close();
});
const taskId = "b7654d21-6806-4fdc-89ef-e2f9c038e4f9";
const claimId = "29e0f8d7-b069-44bc-a747-5890b89ea174";
const parent = { platform: "discord", kind: "channel", conversationId: "100", boundaryId: "1" } as const;
const thread = { ...parent, kind: "thread", conversationId: "200", parentId: "100" } as const;
const floor = Date.parse("2026-10-06T05:00:00.000Z");
const snowflake = (at: number, suffix = 0) => (((BigInt(at) - 1420070400000n) << 22n) + BigInt(suffix)).toString();

type WireFrame = { type: string; id?: string; result?: any; error?: any; payload?: any };
async function connect(socketPath: string) {
	const frames: WireFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				for (;;) {
					const end = buffered.indexOf("\n");
					if (end < 0) break;
					const line = buffered.slice(0, end);
					buffered = buffered.slice(end + 1);
					if (line) frames.push(JSON.parse(line));
				}
			},
		},
	});
	const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
	send({ v: PROFILE_VERSION, type: "hello", payload: { supportedVersions: [PROFILE_VERSION] } });
	await until(() => frames.length > 0);
	return {
		frames,
		close: () => socket.end(),
		async request(verb: string, params?: unknown) {
			const id = crypto.randomUUID();
			send({ v: PROFILE_VERSION, type: "request", id, verb, params });
			await until(() => frames.some((frame) => frame.id === id));
			return frames.find((frame) => frame.id === id)!;
		},
	};
}
async function until(predicate: () => boolean) {
	for (let index = 0; index < 600 && !predicate(); index++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}
async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "firstmate-server-"));
	await mkdir(join(directory, "workspace"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		boundaries: { "discord:1": { engagement: "mention-open", audience: "human-only" } },
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "300", peerId: "42" } },
	};
	const db = await GatewayDatabase.open(config.dbPath);
	let onSteer: ScriptedSessionPort["onSteer"];
	const port = new ScriptedSessionPort({
		sessionIdForBind: () => crypto.randomUUID(),
		onSteer: (input, port) => onSteer?.(input, port),
	});
	attachTestBrokerOwnership(db, port, join(directory, "agent"));
	let server: GatewayServer = await startUnixServer({ config, database: db, sessionPort: port });
	let client = await connect(config.socketPath);
	cleanup.push(async () => {
		client.close();
		await server.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const assignment = (id = taskId) => ({
		name: `fm-${id}`,
		text: "Inspect original evidence without edits",
		cwd: directory,
		task: { taskId: id, kind: "read_only", surface: { parentOrigin: parent } },
	});
	const request = (verb: string, params?: unknown) => client.request(verb, params);
	return {
		db,
		port,
		directory,
		config,
		request,
		assignment,
		setOnSteer(handler: ScriptedSessionPort["onSteer"]) {
			onSteer = handler;
		},
		async restart(workspace?: string) {
			client.close();
			await server.stop();
			server = await startUnixServer({
				config,
				database: db,
				sessionPort: port,
				...(workspace ? { workspace } : {}),
			});
			client = await connect(config.socketPath);
		},
		async admit() {
			return request("work.start", assignment());
		},
		async bind() {
			expect((await request("work.thread.claim", { taskId, claimId })).error).toBeUndefined();
			return request("work.thread.bind", { taskId, claimId, outcome: { kind: "bound", origin: thread } });
		},
		marker() {
			return db.workTaskSourceGet(`activation-${taskId}`)!.deliveryId!;
		},
		async confirm(messageIds = [snowflake(floor)]) {
			return request("delivery.confirm", {
				deliveryId: db.workTaskSourceGet(`activation-${taskId}`)!.deliveryId,
				platformReceipt: { origin: thread, messageIds },
			});
		},
	};
}
function event(overrides: Record<string, unknown> = {}) {
	return {
		origin: thread,
		messageId: snowflake(floor + 100),
		text: "Inspect the original failure",
		originSource: { platformCreatedAt: floor + 100 },
		engagement: { authorId: "42", mentioned: false, group: true },
		...overrides,
	};
}

function dispositionRequest(
	f: Awaited<ReturnType<typeof fixture>>,
	target: WorkTaskDispositionParams["target"],
	eventId = "cli:administrative-original",
): WorkTaskDispositionParams {
	const task = f.db.workTaskGet(taskId)!;
	return {
		taskId,
		jobId: task.jobId,
		expectedOpRef: task.opRef,
		sessionId: task.sessionId,
		epoch: task.epoch,
		cwd: task.request.cwd,
		requestHash: task.requestHash,
		target,
		eventId,
		expectedTaskVersion: task.version,
		outcome: "abandoned",
		reason: "Original evidence remains irrecoverable; administrative decision only",
		evidence: { availability: "unavailable", detail: "Operator checked original evidence", evidenceAt: null },
	};
}
async function dispositionCli(f: Awaited<ReturnType<typeof fixture>>, request: unknown, hint?: string) {
	const env = { ...process.env };
	delete env.GJC_SESSION_ID;
	if (hint !== undefined) env.GJC_SESSION_ID = hint;
	const child = Bun.spawn(
		[
			process.execPath,
			join(import.meta.dir, "../../cli/src/main.ts"),
			"--socket",
			f.config.socketPath,
			"work",
			"disposition",
			"--request",
			JSON.stringify(request),
		],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	const [code, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	return { code, stdout, stderr };
}
async function standaloneBasisCli(
	f: Awaited<ReturnType<typeof fixture>>,
	hint?: string,
	args: string[] = ["--basis", taskId],
) {
	const env = { ...process.env };
	delete env.GJC_SESSION_ID;
	if (hint !== undefined) env.GJC_SESSION_ID = hint;
	const child = Bun.spawn(
		[
			process.execPath,
			join(import.meta.dir, "../../cli/src/main.ts"),
			"--socket",
			f.config.socketPath,
			"work",
			"disposition",
			...args,
		],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	const [code, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	return { code, stdout, stderr };
}
async function negativeBasisCli(f: Awaited<ReturnType<typeof fixture>>, hint?: string) {
	const response = await standaloneBasisCli(f, hint);
	expect(response.code).toBe(0);
	expect(response.stderr).toBe("");
	const result = validateWorkTaskDispositionBasisResult(JSON.parse(response.stdout));
	if (result.kind !== "validation_unavailable") throw new Error("qualified negative basis required");
	return result;
}
async function corruptAndRecover(f: Awaited<ReturnType<typeof fixture>>, opRef: string) {
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_attempt_runtime SET record_json = '{private-broken-runtime' WHERE op_ref = ?").run(opRef);
	} finally {
		raw.close();
	}
	expect(() => f.db.workAttemptGet(opRef)).toThrow();
	expect(() => f.db.workTaskGet(taskId)).toThrow();
	// Production recovery, not a store-seeded negative source or manufactured task hold.
	await f.restart();
	await until(() =>
		f.db.withTransaction(() => f.db.workTaskLinkedUnavailableSourcesInTransaction(taskId).sources.length > 0),
	);
	return negativeBasisCli(f);
}
async function statusBasisCli(f: Awaited<ReturnType<typeof fixture>>, opRef: string) {
	const env = { ...process.env };
	delete env.GJC_SESSION_ID;
	const child = Bun.spawn(
		[
			process.execPath,
			join(import.meta.dir, "../../cli/src/main.ts"),
			"--socket",
			f.config.socketPath,
			"work",
			"status",
			`fm-${taskId}`,
			"--task-id",
			taskId,
			"--expected-op-ref",
			opRef,
		],
		{ env, stdout: "pipe", stderr: "pipe" },
	);
	const [code, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	expect(code).toBe(0);
	expect(stderr).toBe("");
	return validateWorkTaskDispositionBasis(JSON.parse(stdout).task.dispositionBasis);
}
function publicDisposition(
	basis: WorkTaskDispositionBasis,
	kind: "control" | "report",
	eventId = "cli:administrative-original",
): WorkTaskDispositionParams {
	const { controls, controlsCompleteness, report, ...fences } = basis;
	const target = kind === "control" ? controls[0] : report;
	if (!target) throw new Error("public held target unavailable");
	return {
		...fences,
		target,
		eventId,
		outcome: "abandoned",
		reason: "Original evidence remains irrecoverable; administrative decision only",
		evidence: { availability: "unavailable", detail: "Operator checked original evidence", evidenceAt: null },
	};
}
async function heldControl(f: Awaited<ReturnType<typeof fixture>>) {
	await f.admit();
	await f.bind();
	await f.confirm();
	f.setOnSteer(async () => {
		throw new Error("synthetic lost original steer receipt");
	});
	const task = f.db.workTaskGet(taskId)!;
	const response = await f.request("work.steer", {
		name: task.laneName,
		taskId,
		expectedOpRef: task.opRef,
		eventId: "cli:held-original-control",
		text: "Inspect original evidence",
	});
	expect(response.error).toBeUndefined();
	const control = f.db.workControlGet(response.result.controlId)!;
	expect(control.phase).toBe("held");
	return control;
}

test("actual CLI standalone negative basis preserves nullable original report and audits without SDK truth", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	const original = f.db.workTaskGet(taskId)!;
	expect(original.obligationState).toBe("awaiting_final");
	expect(original.terminalReportId).toBeNull();
	const positive = await standaloneBasisCli(f, " \t ");
	expect(positive.code).toBe(0);
	expect(validateWorkTaskDispositionBasisResult(JSON.parse(positive.stdout))).toMatchObject({
		execution: "none",
		kind: "original",
		basis: { taskId, report: null },
	});
	const selected = await corruptAndRecover(f, original.opRef);
	expect(selected.basis.report).toEqual({ kind: "report", reportId: null });
	const fact = f.db.workTaskSourceGet(selected.qualification.sourceId)!;
	expect(selected.qualification).toMatchObject({ scope: "read_only", firstObservedAt: fact.evidence.observedAt });
	expect(selected.qualification.sourceId).toBe(`unavailability-${selected.qualification.corruptionFingerprint}`);
	expect(await negativeBasisCli(f)).toEqual(selected);
	expect((await f.request("work.status", { name: original.laneName, taskId })).error).toBeDefined();
	const request = {
		...publicDisposition(selected.basis, "report", "cli:negative-null-report"),
		validationUnavailable: selected.qualification,
	};
	const raw = new Database(f.config.dbPath);
	try {
		const frozen = () =>
			["work_attempt_runtime", "lane_jobs", "work_controls", "broker_owned_bindings", "broker_quarantine"].map(
				(table) => raw.query(`SELECT * FROM ${table}`).all(),
			);
		const before = frozen();
		const taskBefore = raw
			.query<{ record_json: string }, [string]>("SELECT record_json FROM work_tasks WHERE task_id = ?")
			.get(taskId)!;
		const reads = f.port.workerOutputReads.length;
		const response = await dispositionCli(f, request);
		expect(response.code).toBe(0);
		const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
		expect(result).toMatchObject({
			execution: "none",
			disposition: "recorded",
			record: {
				principalId: "local-ipc:owner",
				origin: LOOPBACK_ORIGIN,
				request: { validationUnavailable: selected.qualification },
				retained: { obligationState: "awaiting_final", reportId: null, holdReason: null, controlPhase: null },
			},
		});
		expect(result.deliveryId).not.toBeNull();
		expect(frozen()).toEqual(before);
		expect(
			JSON.parse(
				raw
					.query<{ record_json: string }, [string]>("SELECT record_json FROM work_tasks WHERE task_id = ?")
					.get(taskId)!.record_json,
			),
		).toEqual({ ...JSON.parse(taskBefore.record_json), version: selected.basis.expectedTaskVersion + 1 });
		expect(f.db.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
		expect(JSON.parse(f.db.deliveryGet(result.deliveryId!)!.payload_json)).toMatchObject({
			final: false,
			origin: thread,
			workTask: { taskId, opRef: original.opRef, sourceId: result.sourceId, mappedOnly: true },
		});
		expect(validateWorkTaskDispositionResult(JSON.parse((await dispositionCli(f, request)).stdout))).toEqual({
			...result,
			disposition: "duplicate",
		});
		expect((await dispositionCli(f, { ...request, eventId: "cli:stale-negative-basis" })).code).toBe(1);
		expect((await dispositionCli(f, { ...request, reason: "Conflicting retry" })).code).toBe(1);
		expect(frozen()).toEqual(before);
		expect(f.port.workerOutputReads).toHaveLength(reads);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.steers).toHaveLength(0);
		expect(() => f.db.workTaskGet(taskId)).toThrow();
		// Both retained negative presentations settle through the actual adapter route,
		// while normal task reads and every lifecycle authority remain unavailable.
		const taskAfterDisposition = raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId);
		for (const deliveryId of [fact.deliveryId!, result.deliveryId!]) {
			const messageIds = [snowflake(floor + 200), snowflake(floor + 201)];
			const receiptId = `receipt-${createHash("sha256").update(deliveryId).digest("hex")}`;
			const confirm = () =>
				f.request("delivery.confirm", {
					deliveryId,
					platformReceipt: { origin: thread, messageIds },
				});
			const unconfirmed = f.db.deliveryGet(deliveryId);
			for (const change of [
				(value: any) => {
					value.workTask.sourceId = `activation-${taskId}`;
				},
				(value: any) => {
					value.workTask.opRef = "gw-wrong-original";
				},
				(value: any) => {
					value.text += " altered";
				},
				(value: any) => {
					value.origin.conversationId = "999";
				},
			]) {
				const altered = JSON.parse(unconfirmed!.payload_json);
				change(altered);
				raw
					.query("UPDATE deliveries SET payload_json = ? WHERE delivery_id = ?")
					.run(JSON.stringify(altered), deliveryId);
				try {
					expect((await confirm()).error).toBeDefined();
					expect(f.db.deliveryGet(deliveryId)?.state).toBe(unconfirmed!.state);
					expect(f.db.workTaskSourceGet(receiptId)).toBeUndefined();
				} finally {
					raw
						.query("UPDATE deliveries SET payload_json = ? WHERE delivery_id = ?")
						.run(unconfirmed!.payload_json, deliveryId);
				}
			}
			for (const platformReceipt of [
				undefined,
				{ origin: { ...thread, conversationId: "999" }, messageIds },
				{ origin: thread, messageIds: [messageIds[0], messageIds[0]] },
			]) {
				expect((await f.request("delivery.confirm", { deliveryId, platformReceipt })).error).toBeDefined();
				expect(f.db.deliveryGet(deliveryId)).toEqual(unconfirmed);
				expect(f.db.workTaskSourceGet(receiptId)).toBeUndefined();
			}
			expect((await confirm()).error).toBeUndefined();
			expect(f.db.deliveryGet(deliveryId)?.state).toBe("confirmed");
			const retained = f.db.workTaskSourceGet(receiptId)!;
			expect(JSON.parse(retained.body)).toEqual({
				deliveryId,
				firstMessageId: messageIds[0],
				messageCount: 2,
				messageIdsSha256: createHash("sha256").update(JSON.stringify(messageIds)).digest("hex"),
			});
			expect((await confirm()).error).toBeUndefined();
			expect(f.db.workTaskSourceGet(receiptId)).toEqual(retained);
			expect(
				(
					await f.request("delivery.confirm", {
						deliveryId,
						platformReceipt: { origin: thread, messageIds: [messageIds[0]] },
					})
				).error,
			).toBeDefined();
			expect(f.db.workTaskSourceGet(receiptId)).toEqual(retained);
			expect(frozen()).toEqual(before);
			expect(raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId)).toEqual(taskAfterDisposition);
			expect(() => f.db.workTaskGet(taskId)).toThrow();
			await f.restart();
			expect((await confirm()).error).toBeUndefined();
			expect(f.db.workTaskSourceGet(receiptId)).toEqual(retained);
			expect(() => f.db.workTaskGet(taskId)).toThrow();
		}
		// Quarantine is not bypassed to manufacture a runtime-repair test.
		expect(() =>
			raw.query("UPDATE work_attempt_runtime SET record_json = '{}' WHERE op_ref = ?").run(original.opRef),
		).toThrow("broker authority: quarantined");
		const refreshed = await negativeBasisCli(f);
		expect(refreshed).toEqual({
			...selected,
			basis: { ...selected.basis, expectedTaskVersion: selected.basis.expectedTaskVersion + 1 },
		});
		const healthy = await f.request("work.start", {
			name: "healthy-beside-negative",
			text: "Inspect independently",
			cwd: f.directory,
		});
		expect(healthy.error).toBeUndefined();
		f.port.complete(healthy.result.opRef, "Independent original result");
		await until(() => Boolean(f.db.workAttemptGet(healthy.result.opRef)?.settledAt));
		expect(f.db.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
		expect(() => f.db.workTaskGet(taskId)).toThrow();
	} finally {
		raw.close();
	}
}, 30_000);

test("actual CLI negative held-control disposition retains source, queue and authenticated persona attribution", async () => {
	const f = await fixture();
	const control = await heldControl(f);
	const task = f.db.workTaskGet(taskId)!;
	const originalObservation = f.db.workTaskSourceGet(`disposition-${control.controlId}-held`);
	const selected = await corruptAndRecover(f, task.opRef);
	expect(selected.basis.controls).toEqual([
		{
			kind: "control",
			controlId: control.controlId,
			eventId: control.request.evidence.eventId,
			clientRef: control.clientRef,
		},
	]);
	const persona = await f.port.bind({ originKey: originKey(parent), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(parent), JSON.stringify(parent));
	expect(await negativeBasisCli(f, ` \t${persona.sessionId}\n`)).toEqual(selected);
	const request = {
		...publicDisposition(selected.basis, "control", "cli:negative-persona"),
		validationUnavailable: selected.qualification,
	};
	const response = await dispositionCli(f, request, ` ${persona.sessionId} `);
	expect(response.code).toBe(0);
	const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
	expect(result.record).toMatchObject({
		principalId: `local-ipc:persona:${persona.sessionId}`,
		origin: parent,
		request: { callerSessionId: persona.sessionId, validationUnavailable: selected.qualification },
		retained: { controlPhase: "held" },
	});
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.db.workTaskSourceGet(`disposition-${control.controlId}-held`)).toEqual(originalObservation);
	expect(f.db.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
	expect((await dispositionCli(f, request, persona.sessionId)).code).toBe(0);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(1);
}, 30_000);

test("negative basis and audit reject worker, mapped and unresolved hints before reading administrative facts", async () => {
	const f = await fixture();
	await heldControl(f);
	const task = f.db.workTaskGet(taskId)!;
	const selected = await corruptAndRecover(f, task.opRef);
	const request = {
		...publicDisposition(selected.basis, "control", "cli:negative-denied"),
		validationUnavailable: selected.qualification,
	};
	const mapped = await f.port.bind({ originKey: originKey(thread), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(thread), JSON.stringify(thread));
	const unresolved = await f.port.bind({
		originKey: originKey(parent),
		epoch: 0,
		repo: join(f.directory, "workspace"),
	});
	const basis = spyOn(f.db, "workTaskDispositionBasis");
	const scope = spyOn(f.db, "workTaskQualifiedAdmissionScopeInTransaction");
	const control = spyOn(f.db, "workControlGet");
	const lookup = spyOn(f.port, "lookupSteerStatus");
	try {
		for (const hint of [task.sessionId!, mapped.sessionId, unresolved.sessionId, crypto.randomUUID(), "invalid-uuid"]) {
			expect((await standaloneBasisCli(f, hint)).code).toBe(1);
			expect((await dispositionCli(f, request, hint)).code).toBe(1);
		}
		for (const args of [
			["--basis", "not-a-uuid"],
			["--basis", taskId, "--request", "{}"],
		])
			expect((await standaloneBasisCli(f, undefined, args)).code).toBe(1);
		expect(basis).not.toHaveBeenCalled();
		expect(scope).not.toHaveBeenCalled();
		expect(control).not.toHaveBeenCalled();
		expect(lookup).not.toHaveBeenCalled();
		expect(f.db.workTaskSourceGet(workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN))).toBeUndefined();
	} finally {
		basis.mockRestore();
		scope.mockRestore();
		control.mockRestore();
		lookup.mockRestore();
	}
}, 30_000);

test("negative basis excludes pending controls and withholds a held target whose retained route proof is damaged", async () => {
	const f = await fixture();
	const first = await heldControl(f);
	const original = f.db.workTaskGet(taskId)!;
	for (let index = 0; index < 20; index++) {
		const response = await f.request("work.steer", {
			name: original.laneName,
			taskId,
			expectedOpRef: original.opRef,
			eventId: `cli:negative-queued-${index}`,
			text: "Queued original instruction",
		});
		expect(response.error).toBeUndefined();
		expect(response.result.delivery).toBe("pending");
	}
	const selected = await corruptAndRecover(f, original.opRef);
	expect(selected.basis.controlsCompleteness).toBe("complete");
	expect(selected.basis.controls).toEqual([
		{
			kind: "control",
			controlId: first.controlId,
			eventId: first.request.evidence.eventId,
			clientRef: first.clientRef,
		},
	]);
	const request = {
		...publicDisposition(selected.basis, "control", "cli:negative-missing-route"),
		validationUnavailable: selected.qualification,
	};
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?").run(`route-${first.controlId}`);
		const withheld = await negativeBasisCli(f);
		expect(withheld.basis.controls).toEqual([]);
		expect(withheld.basis.controlsCompleteness).toBe("partial");
		expect((await dispositionCli(f, request)).code).toBe(1);
		expect(f.db.workTaskSourceGet(workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN))).toBeUndefined();
		expect(f.db.workControlGet(first.controlId)).toEqual(first);
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		raw.close();
	}
}, 30_000);

test("negative basis bounds more than 20 held controls", async () => {
	const f = await fixture();
	const first = await heldControl(f);
	const original = f.db.workTaskGet(taskId)!;
	f.db.withTransaction(() => {
		for (let index = 0; index < 20; index++) {
			const at = new Date(floor + index).toISOString();
			const request = {
				taskId,
				expectedOpRef: original.opRef,
				kind: "cancel_request" as const,
				scope: "read_only" as const,
				body: "Retain unresolved local cancellation debt",
				evidence: {
					principalId: "owner:42",
					origin: thread,
					eventId: `negative-cap-${index}`,
					editId: null,
					evidenceAt: at,
					observedAt: at,
				},
			};
			const sourceId = `negative-cap-instruction-${index}`;
			f.db.workTaskSourceAppendInTransaction({
				sourceId,
				taskId,
				kind: "instruction",
				body: request.body,
				evidence: request.evidence,
				supersedes: null,
				completeness: "complete",
				controlId: null,
				reportId: null,
			});
			f.db.workControlAdmitInTransaction(request, undefined, {
				taskId,
				opRef: original.opRef,
				thread,
				sourceId,
			});
		}
	});
	const held = f.db.workControlList(taskId);
	expect(held).toHaveLength(21);
	expect(held.every((control) => control.phase === "held")).toBe(true);

	const selected = await corruptAndRecover(f, original.opRef);
	expect(selected.basis.controls).toHaveLength(20);
	expect(selected.basis.controlsCompleteness).toBe("partial");
	expect(selected.basis.controls[0]).toEqual({
		kind: "control",
		controlId: first.controlId,
		eventId: first.request.evidence.eventId,
		clientRef: first.clientRef,
	});
	expect(f.db.workControlList(taskId)).toEqual(held);
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
}, 30_000);

for (const operation of ["basis", "disposition"] as const)
	test(`negative ${operation} revalidates caller identity after acquiring the task lock`, async () => {
		const f = await fixture();
		await heldControl(f);
		const original = f.db.workTaskGet(taskId)!;
		const selected = await corruptAndRecover(f, original.opRef);
		const persona = await f.port.bind({ originKey: originKey(parent), epoch: 0, repo: join(f.directory, "workspace") });
		f.db.updateActivity(originKey(parent), JSON.stringify(parent));
		const request = {
			...publicDisposition(selected.basis, "control", `cli:negative-caller-${operation}`),
			validationUnavailable: selected.qualification,
		};
		const exclusive = f.port.runExclusive.bind(f.port);
		const basis = spyOn(f.db, "workTaskDispositionBasis");
		const scope = spyOn(f.db, "workTaskQualifiedAdmissionScopeInTransaction");
		let drifted = false;
		const gate = spyOn(f.port, "runExclusive").mockImplementation(
			async <T>(key: string, work: () => Promise<T>): Promise<T> => {
				if (!drifted && key === `work/task/${original.laneName}`) {
					drifted = true;
					f.db.updateActivity(originKey(parent), JSON.stringify({ ...parent, conversationId: "999" }));
				}
				return exclusive(key, work);
			},
		);
		try {
			const response =
				operation === "basis"
					? await standaloneBasisCli(f, persona.sessionId)
					: await dispositionCli(f, request, persona.sessionId);
			expect(response.code).toBe(1);
			expect(drifted).toBe(true);
			expect(basis).not.toHaveBeenCalled();
			expect(scope).not.toHaveBeenCalled();
			expect(f.db.workTaskSourceGet(workTaskDispositionId(taskId, request.eventId, parent))).toBeUndefined();
			expect(f.port.sends).toHaveLength(1);
			expect(f.port.steers).toHaveLength(1);
		} finally {
			gate.mockRestore();
			basis.mockRestore();
			scope.mockRestore();
		}
	}, 30_000);

test("negative audit rejects forged qualification and rolls back before retaining permanent null intent", async () => {
	const f = await fixture();
	await heldControl(f);
	const task = f.db.workTaskGet(taskId)!;
	const selected = await corruptAndRecover(f, task.opRef);
	const request = {
		...publicDisposition(selected.basis, "control", "cli:negative-atomic"),
		validationUnavailable: selected.qualification,
	};
	const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
	const deliveryId = workTaskSourceDeliveryId(taskId, sourceId, thread);
	const raw = new Database(f.config.dbPath);
	try {
		const before = raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId);
		for (const qualification of [
			{ ...selected.qualification, corruptionFingerprint: "a".repeat(64) },
			{ ...selected.qualification, sourceId: `unavailability-${"b".repeat(64)}` },
			{ ...selected.qualification, firstObservedAt: "2026-01-01T00:00:00.000Z" },
			{ ...selected.qualification, scope: "code_mutating" },
		])
			expect((await dispositionCli(f, { ...request, validationUnavailable: qualification })).code).toBe(1);
		raw.exec(`CREATE TRIGGER reject_negative_audit BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'disposition-%' BEGIN SELECT RAISE(ABORT, 'injected negative audit failure'); END`);
		expect((await dispositionCli(f, request)).code).toBe(1);
		expect(raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId)).toEqual(before);
		expect(f.db.workTaskSourceGet(sourceId)).toBeUndefined();
		expect(f.db.deliveryGet(deliveryId)).toBeUndefined();
		raw.exec("DROP TRIGGER reject_negative_audit");
		f.db.inboundEnqueue({
			messageId: "negative-presentation-busy",
			originKey: originKey(thread),
			originRefJson: JSON.stringify(thread),
			body: "Previously admitted persona work",
		});
		const response = await dispositionCli(f, request);
		expect(response.code).toBe(0);
		const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
		expect(result.deliveryId).toBeNull();
		expect(f.db.workTaskSourceGet(sourceId)?.deliveryId).toBeNull();
		expect(f.db.deliveryGet(deliveryId)).toBeUndefined();
		// Restore availability through the existing pending-input discard primitive.
		expect(f.db.inboundDiscardBefore(originKey(thread), new Date().toISOString())).toContain(
			"negative-presentation-busy",
		);
		expect(f.db.inboundPendingCount(originKey(thread))).toBe(0);
		expect(validateWorkTaskDispositionResult(JSON.parse((await dispositionCli(f, request)).stdout))).toEqual({
			...result,
			disposition: "duplicate",
		});
		expect(f.db.deliveryGet(deliveryId)).toBeUndefined();
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		raw.close();
	}
}, 30_000);

for (const damage of ["task", "assignment", "mapping"] as const)
	test(`negative wire basis withholds unverifiable ${damage} without an audit or guessed route`, async () => {
		const f = await fixture();
		await f.admit();
		await f.bind();
		await f.confirm();
		const original = f.db.workTaskGet(taskId)!;
		const selected = await corruptAndRecover(f, original.opRef);
		const request = {
			...publicDisposition(selected.basis, "report", `cli:negative-corrupt-${damage}`),
			validationUnavailable: selected.qualification,
		};
		const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
		const raw = new Database(f.config.dbPath);
		try {
			if (damage === "task") raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(taskId);
			else if (damage === "assignment")
				raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?").run(`assignment-${taskId}`);
			else raw.query("UPDATE deliveries SET payload_json = '{}' WHERE delivery_id = ?").run(f.marker());
			const response = await standaloneBasisCli(f);
			expect(response.code).toBe(0);
			expect(validateWorkTaskDispositionBasisResult(JSON.parse(response.stdout))).toEqual({
				execution: "none",
				kind: "unavailable",
				basis: null,
			});
			expect((await dispositionCli(f, request)).code).toBe(1);
			expect(raw.query("SELECT source_id FROM work_task_sources WHERE source_id = ?").get(sourceId)).toBeNull();
			expect(f.db.deliveryGet(workTaskSourceDeliveryId(taskId, sourceId, thread))).toBeUndefined();
			expect(f.port.sends).toHaveLength(1);
			expect(f.port.steers).toHaveLength(0);
		} finally {
			raw.close();
		}
	}, 30_000);

for (const change of ["coordinator_config", "worktree"] as const)
	test(`negative administration revalidates retained worktree against changed ${change}`, async () => {
		const f = await fixture();
		const primary = join(f.directory, "workspace");
		const worker = join(f.directory, "negative-worker");
		const git = (...args: string[]) => {
			const result = Bun.spawnSync(["git", "-C", primary, ...args], { stdout: "pipe", stderr: "pipe" });
			if (result.exitCode !== 0) throw new Error(result.stderr.toString());
		};
		git("init");
		git(
			"-c",
			"user.name=Fixture",
			"-c",
			"user.email=fixture@example.invalid",
			"commit",
			"--allow-empty",
			"-m",
			"fixture",
		);
		git("worktree", "add", "-b", "negative-worker", worker);
		const assignment = f.assignment();
		expect(
			(
				await f.request("work.start", {
					...assignment,
					cwd: worker,
					task: { ...assignment.task, kind: "code_mutating" },
				})
			).error,
		).toBeUndefined();
		await f.bind();
		await f.confirm();
		const original = f.db.workTaskGet(taskId)!;
		const selected = await corruptAndRecover(f, original.opRef);
		const request = {
			...publicDisposition(selected.basis, "report", `cli:negative-${change}`),
			validationUnavailable: selected.qualification,
		};
		const raw = new Database(f.config.dbPath);
		const before = raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId);
		let restoreGate = () => {};
		try {
			if (change === "coordinator_config") {
				const changed = join(f.directory, "different-configured-workspace");
				await mkdir(changed);
				await f.restart(changed);
			} else {
				const exclusive = f.port.runExclusive.bind(f.port);
				let drifted = false;
				const gate = spyOn(f.port, "runExclusive").mockImplementation(
					async <T>(key: string, work: () => Promise<T>): Promise<T> => {
						if (!drifted && key === `work/task/${original.laneName}`) {
							drifted = true;
							await rename(worker, join(f.directory, "moved-negative-worker"));
							await mkdir(worker);
						}
						return exclusive(key, work);
					},
				);
				restoreGate = () => {
					gate.mockRestore();
				};
			}
			expect((await dispositionCli(f, request)).code).toBe(1);
			expect((await standaloneBasisCli(f)).code).toBe(1);
			expect(raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId)).toEqual(before);
			expect(f.db.workTaskSourceGet(workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN))).toBeUndefined();
			expect(f.port.sends).toHaveLength(1);
			expect(f.port.steers).toHaveLength(0);
		} finally {
			restoreGate();
			raw.close();
		}
	}, 30_000);

test("real task.context joins original control disposition, administrative audit, exception and Git checkpoint", async () => {
	const f = await fixture();
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", f.directory, ...args], { stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(result.stderr.toString());
		return result.stdout.toString().trim();
	};
	git("init");
	git(
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"baseline",
	);
	const control = await heldControl(f);
	const task = f.db.workTaskGet(taskId)!;
	const administrative = await f.request(
		"work.task.disposition",
		dispositionRequest(f, {
			kind: "control",
			controlId: control.controlId,
			eventId: control.request.evidence.eventId,
			clientRef: control.clientRef,
		}),
	);
	expect(administrative.error).toBeUndefined();
	const admin = validateWorkTaskDispositionResult(administrative.result);
	const oldId = `disposition-${control.controlId}-held`;
	const old = f.db.workTaskSourceGet(oldId)!;
	expect(old.kind).toBe("observation");
	const status = spyOn(f.port, "status").mockRejectedValue(new Error("private credential must not be retained"));
	cleanup.push(async () => {
		status.mockRestore();
	});
	await f.restart();
	await until(() =>
		f.db
			.workTaskSources(taskId)!
			.sources.some(
				(source) => source.sourceId.startsWith("execution-") && source.body.includes("reconciliation unavailable"),
			),
	);
	const failure = f.db
		.workTaskSources(taskId)!
		.sources.find(
			(source) => source.sourceId.startsWith("execution-") && source.body.includes("reconciliation unavailable"),
		)!;
	status.mockRestore();
	git(
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"checkpoint",
	);
	const sha = git("rev-parse", "HEAD");
	f.port.complete(task.opRef, "Original terminal answer");
	await f.restart();
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	const checkpoint = f.db
		.workTaskSources(taskId)!
		.sources.find((source) => source.sourceId.startsWith("execution-") && source.body.includes(sha))!;
	expect(checkpoint).toBeDefined();
	const response = await f.request("work.task.context", { taskId });
	expect(response.error).toBeUndefined();
	for (const source of [old, f.db.workTaskSourceGet(admin.sourceId)!, failure, checkpoint]) {
		expect(
			response.result.manifest.find((entry: { sourceId: string }) => entry.sourceId === source.sourceId),
		).toMatchObject({
			taskId,
			revision: source.contentHash,
			evidenceAt: source.evidence.evidenceAt,
			observedAt: source.evidence.observedAt,
		});
		expect(
			response.result.items.find((entry: { sourceId: string }) => entry.sourceId === source.sourceId)?.text,
		).toContain(source.body);
		expect(JSON.parse(f.db.deliveryGet(source.deliveryId!)!.payload_json)).toMatchObject({
			origin: thread,
			workTask: { taskId, opRef: task.opRef, sourceId: source.sourceId, mappedOnly: true },
		});
	}
	expect(response.result.completeness).toBe("partial");
	expect(JSON.stringify(response.result)).not.toContain("private credential");
	const repeat = await f.request("work.task.context", { taskId, continuation: response.result.continuation });
	expect(repeat.result.snapshot).toBe("same");
	expect(f.db.workTaskSourceGet(oldId)).toEqual(old);
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.port.sends).toHaveLength(1);
});

test("status basis qualifies omitted held controls without paging or dispatching them", async () => {
	const f = await fixture();
	const first = await heldControl(f);
	// ScriptedSessionPort owns this synthetic session/epoch; no live SDK identity is claimed.
	const original = f.db.workTaskGet(taskId)!;
	f.db.withTransaction(() => {
		for (let index = 0; index < 20; index++) {
			const at = new Date(floor + index).toISOString();
			f.db.workControlAdmitInTransaction({
				taskId,
				expectedOpRef: original.opRef,
				kind: "cancel_request",
				scope: "read_only",
				body: "Retain unresolved local cancellation debt",
				evidence: {
					principalId: "owner:42",
					origin: thread,
					eventId: `cap-${index}`,
					editId: null,
					evidenceAt: at,
					observedAt: at,
				},
			});
		}
	});
	const before = f.db.workControlList(taskId);
	expect(before).toHaveLength(21);
	expect(before.every((control) => control.phase === "held")).toBe(true);
	const basis = await statusBasisCli(f, original.opRef);
	expect(basis.controlsCompleteness).toBe("partial");
	expect(basis.controls).toHaveLength(20);
	expect(basis.controls[0]).toEqual({
		kind: "control",
		controlId: first.controlId,
		eventId: first.request.evidence.eventId,
		clientRef: first.clientRef,
	});
	expect(basis.controls[19]?.clientRef).toBeNull();
	expect(f.db.workControlList(taskId)).toEqual(before);
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
}, 30_000);

test("mapped held observations and administrative records retain distinct source identities", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	f.setOnSteer(async () => {
		throw new Error("synthetic lost mapped receipt");
	});
	const mapped = await f.request("chat.send", event());
	expect(mapped.error).toBeUndefined();
	await until(() => f.db.workControlList(taskId).some((control) => control.phase === "held"));
	const control = f.db.workControlList(taskId)[0]!;
	const observationId = `disposition-${control.controlId}-held`;
	const observation = f.db.workTaskSourceGet(observationId)!;
	expect(observation.kind).toBe("observation");
	expect(observation.administrative).toBeUndefined();
	const basis = await statusBasisCli(f, control.request.expectedOpRef);
	const request = publicDisposition(basis, "control", "cli:mapped-audit");
	const response = await dispositionCli(f, request);
	expect(response.code).toBe(0);
	const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
	expect(result.sourceId).toMatch(/^disposition-[0-9a-f]{64}$/);
	expect(result.sourceId).not.toBe(observationId);
	expect(f.db.workTaskSourceGet(observationId)).toEqual(observation);
	expect(f.db.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.port.steers).toHaveLength(1);
}, 30_000);

test("status discards awaited evidence when the durable disposition version changes", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const original = f.db.workTaskGet(taskId)!;
	f.port.complete(original.opRef, "x".repeat(16 * 1024 + 1));
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "held");
	const basis = await statusBasisCli(f, original.opRef);
	const originalStatus = f.port.status.bind(f.port);
	let entered = false;
	let release = () => {};
	const barrier = new Promise<void>((resolve) => {
		release = resolve;
	});
	const status = spyOn(f.port, "status").mockImplementationOnce(async (input) => {
		const observed = await originalStatus(input);
		entered = true;
		await barrier;
		return observed;
	});
	try {
		const pending = f.request("work.status", {
			name: `fm-${taskId}`,
			taskId,
			expectedOpRef: basis.expectedOpRef,
		});
		await until(() => entered);
		const recorded = await dispositionCli(f, publicDisposition(basis, "report", "cli:status-race"));
		expect(recorded.code).toBe(0);
		release();
		const response = await pending;
		expect(response.error).toBeUndefined();
		expect(response.result.op).toBeNull();
		expect(validateWorkTaskDispositionBasis(response.result.task.dispositionBasis)).toEqual({
			...basis,
			expectedTaskVersion: basis.expectedTaskVersion + 1,
		});
		expect(response.result.task.finalReport.reportId).toBe(basis.report?.reportId);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.steers).toHaveLength(0);
	} finally {
		release();
		status.mockRestore();
	}
}, 30_000);

test("server validates disposition shape before caller or task lookup", async () => {
	const f = await fixture();
	const tasks = spyOn(f.db, "workTaskGet");
	const callers = spyOn(f.db, "originForSessionId");
	try {
		const response = await f.request("work.task.disposition", {
			taskId,
			callerSessionId: crypto.randomUUID(),
			principalId: "forged-owner",
		});
		expect(response.error?.code).toBe("invalid_params");
		expect(
			(
				await f.request("work.task.disposition.basis", {
					taskId,
					callerSessionId: crypto.randomUUID(),
					principalId: "forged-owner",
				})
			).error?.code,
		).toBe("invalid_params");
		expect(tasks).not.toHaveBeenCalled();
		expect(callers).not.toHaveBeenCalled();
		expect(f.port.sends).toHaveLength(0);
	} finally {
		tasks.mockRestore();
		callers.mockRestore();
	}
});

test("actual CLI owner disposition audits held control without settlement, replay or successor release", async () => {
	const f = await fixture();
	const control = await heldControl(f);
	const before = f.db.workTaskGet(taskId)!;
	const runtime = f.db.workAttemptGet(before.opRef);
	const history = f.db.workTaskSources(taskId)!.sources;
	const unknownStatus = spyOn(f.port, "status").mockResolvedValue({
		operationRef: before.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	const basis = await statusBasisCli(f, before.opRef);
	unknownStatus.mockRestore();
	const request = publicDisposition(basis, "control");
	expect(request.target).toEqual({
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	const lookup = spyOn(f.port, "lookupSteerStatus");
	const reads = f.port.workerOutputReads.length;
	const response = await dispositionCli(f, request);
	expect(response.code).toBe(0);
	expect(response.stderr).toBe("");
	const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
	expect(result).toMatchObject({
		execution: "none",
		disposition: "recorded",
		record: {
			request,
			principalId: "local-ipc:owner",
			origin: LOOPBACK_ORIGIN,
			retained: { obligationState: before.obligationState, controlPhase: "held" },
		},
	});
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.db.workAttemptGet(before.opRef)).toEqual(runtime);
	expect(f.db.workTaskGet(taskId)).toEqual({
		...before,
		version: before.version + 1,
		updatedAt: result.record.recordedAt,
	});
	const source = f.db.workTaskSourceGet(result.sourceId)!;
	expect(source.administrative).toEqual(result.record);
	expect(source.kind).toBe("decision");
	expect(source.completeness).toBe("incomplete");
	expect(f.db.workTaskSources(taskId)!.sources.slice(0, history.length)).toEqual([...history]);
	const delivery = f.db.deliveryGet(result.deliveryId!)!;
	expect(JSON.parse(delivery.payload_json)).toMatchObject({
		origin: thread,
		final: false,
		workTask: { taskId, opRef: before.opRef, sourceId: result.sourceId, mappedOnly: true },
	});
	expect(JSON.parse(delivery.payload_json).text).toContain("Execution unchanged");
	const duplicate = await dispositionCli(f, request);
	expect(duplicate.code).toBe(0);
	expect(validateWorkTaskDispositionResult(JSON.parse(duplicate.stdout))).toEqual({
		...result,
		disposition: "duplicate",
	});
	expect(f.db.workTaskGet(taskId)?.version).toBe(before.version + 1);
	const refreshed = await statusBasisCli(f, basis.expectedOpRef);
	expect(refreshed).toEqual({ ...basis, expectedTaskVersion: basis.expectedTaskVersion + 1 });
	const second = await dispositionCli(f, publicDisposition(refreshed, "control", "cli:second-audit"));
	expect(second.code).toBe(0);
	expect(validateWorkTaskDispositionResult(JSON.parse((await dispositionCli(f, request)).stdout))).toEqual({
		...result,
		disposition: "duplicate",
	});
	const afterAudits = f.db.workTaskGet(taskId);
	const sourcesAfterAudits = f.db.workTaskSources(taskId);
	for (const changed of [
		{ ...request, reason: "Changed immutable decision" },
		{ ...request, evidence: { ...request.evidence, detail: "Changed evidence" } },
		{ ...request, eventId: "cli:stale-version" },
	])
		expect((await dispositionCli(f, changed)).code).toBe(1);
	expect(f.db.workTaskGet(taskId)).toEqual(afterAudits);
	expect(f.db.workTaskSources(taskId)).toEqual(sourcesAfterAudits);
	const staleSource = workTaskDispositionId(taskId, "cli:stale-version", LOOPBACK_ORIGIN);
	expect(f.db.workTaskSourceGet(staleSource)).toBeUndefined();
	expect(f.db.deliveryGet(workTaskSourceDeliveryId(taskId, staleSource, thread))).toBeUndefined();
	expect(lookup).not.toHaveBeenCalled();
	expect(f.port.workerOutputReads).toHaveLength(reads);
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	lookup.mockRestore();
	const successor = await f.request("work.steer", {
		name: before.laneName,
		taskId,
		expectedOpRef: before.opRef,
		eventId: "cli:ordered-successor",
		text: "Do not bypass original hold",
	});
	expect(successor.error).toBeUndefined();
	expect(successor.result.delivery).toBe("pending");
	expect(f.port.steers).toHaveLength(1);
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect((await f.request("work.retire", { name: before.laneName, force: true })).result).toMatchObject({
		retired: false,
	});
	expect(f.db.workTaskGet(taskId)?.sessionId).toBe(before.sessionId);
}, 30_000);

test("actual CLI report disposition retains historical qualification and permits an independent healthy lane", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const original = f.db.workTaskGet(taskId)!;
	f.port.complete(original.opRef, "x".repeat(16 * 1024 + 1));
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "held");
	const before = f.db.workTaskGet(taskId)!;
	const unknownStatus = spyOn(f.port, "status").mockResolvedValue({
		operationRef: original.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	const basis = await statusBasisCli(f, original.opRef);
	unknownStatus.mockRestore();
	expect(basis.report).toEqual({ kind: "report", reportId: before.terminalReportId });
	const request = {
		...publicDisposition(basis, "report"),
		outcome: "unresolved" as const,
		evidence: {
			availability: "partial" as const,
			detail: "Only bounded historical evidence available",
			evidenceAt: "2026-10-05T01:00:00.000Z",
		},
	};
	const persona = await f.port.bind({ originKey: originKey(parent), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(parent), JSON.stringify(parent));
	const reads = f.port.workerOutputReads.length;
	expect(
		(
			await dispositionCli(
				f,
				{ ...request, target: { kind: "report", reportId: "wrong-original-report" } },
				persona.sessionId,
			)
		).code,
	).toBe(1);
	const response = await dispositionCli(f, request, ` ${persona.sessionId} `);
	expect(response.code).toBe(0);
	const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
	expect(result.record).toMatchObject({
		principalId: `local-ipc:persona:${persona.sessionId}`,
		origin: parent,
		request: { evidence: request.evidence, callerSessionId: persona.sessionId },
		retained: {
			obligationState: "held",
			reportId: before.terminalReportId,
			holdReason: before.holdReason,
			controlPhase: null,
		},
	});
	expect(result.record.recordedAt).not.toBe(request.evidence.evidenceAt);
	expect(f.db.workTaskGet(taskId)).toEqual({
		...before,
		version: before.version + 1,
		updatedAt: result.record.recordedAt,
	});
	expect(f.port.workerOutputReads).toHaveLength(reads);
	expect(f.port.steers).toHaveLength(0);
	expect(await statusBasisCli(f, original.opRef)).toEqual({
		...basis,
		expectedTaskVersion: basis.expectedTaskVersion + 1,
	});
	const duplicate = await dispositionCli(f, request, persona.sessionId);
	expect(duplicate.code).toBe(0);
	expect(validateWorkTaskDispositionResult(JSON.parse(duplicate.stdout))).toEqual({
		...result,
		disposition: "duplicate",
	});
	expect((await dispositionCli(f, { ...request, outcome: "abandoned" }, persona.sessionId)).code).toBe(1);
	expect((await dispositionCli(f, { ...request, eventId: "cli:stale-report" }, persona.sessionId)).code).toBe(1);
	const healthy = await f.request("work.start", {
		name: "independent-healthy",
		text: "Inspect independently",
		cwd: f.directory,
	});
	expect(healthy.error).toBeUndefined();
	expect(healthy.result.started).toBe(true);
	f.port.complete(healthy.result.opRef, "Independent complete original result");
	await until(() => Boolean(f.db.workAttemptGet(healthy.result.opRef)?.terminal));
	expect(f.db.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
	expect(f.db.workTaskGet(taskId)?.obligationState).toBe("held");
}, 30_000);

test("actual CLI disposition rejects every wrong original fence and unresolved or worker caller", async () => {
	const f = await fixture();
	const control = await heldControl(f);
	const before = f.db.workTaskGet(taskId)!;
	const request = dispositionRequest(f, {
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
	const lookup = spyOn(f.port, "lookupSteerStatus");
	const reads = f.port.workerOutputReads.length;
	for (const wrong of [
		{ taskId: crypto.randomUUID() },
		{ jobId: "wrong-job" },
		{ expectedOpRef: "gw-wrong-original" },
		{ sessionId: crypto.randomUUID() },
		{ epoch: before.epoch! + 1 },
		{ cwd: `${f.directory}/wrong` },
		{ requestHash: "a".repeat(64) },
		{ expectedTaskVersion: before.version + 1 },
		{ target: { ...request.target, controlId: "wrong-control" } },
		{ target: { ...request.target, eventId: "cli:wrong-original" } },
		{ target: { ...request.target, clientRef: "gw-wrong-client" } },
		{ target: { kind: "report", reportId: "wrong-report" } },
		{ scope: "code_mutating" },
		{ principalId: "owner" },
		{ origin: parent },
	]) {
		expect((await dispositionCli(f, { ...request, ...wrong })).code).toBe(1);
		expect(f.db.workTaskGet(taskId)).toEqual(before);
		expect(f.db.workTaskSourceGet(sourceId)).toBeUndefined();
	}
	const mapped = await f.port.bind({ originKey: originKey(thread), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(thread), JSON.stringify(thread));
	const unresolved = await f.port.bind({
		originKey: originKey(parent),
		epoch: 0,
		repo: join(f.directory, "workspace"),
	});
	for (const hint of [crypto.randomUUID(), before.sessionId!, mapped.sessionId, unresolved.sessionId, "invalid-uuid"]) {
		expect((await dispositionCli(f, request, hint)).code).toBe(1);
		expect(f.db.workTaskGet(taskId)).toEqual(before);
	}
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(lookup).not.toHaveBeenCalled();
	expect(f.port.workerOutputReads).toHaveLength(reads);
	lookup.mockRestore();
}, 30_000);

test("disposition rollback leaves neither source nor delivery and unavailable mapping never falls back", async () => {
	const f = await fixture();
	const control = await heldControl(f);
	const before = f.db.workTaskGet(taskId)!;
	const request = dispositionRequest(f, {
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
	const deliveryId = workTaskSourceDeliveryId(taskId, sourceId, thread);
	const raw = new Database(f.config.dbPath);
	try {
		// Abort after delivery insertion, proving the existing transaction owns both writes.
		raw.exec(`CREATE TRIGGER reject_administrative_audit BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'disposition-%' BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END`);
		expect((await dispositionCli(f, request)).code).toBe(1);
		expect(f.db.workTaskSourceGet(sourceId)).toBeUndefined();
		expect(f.db.deliveryGet(deliveryId)).toBeUndefined();
		expect(f.db.workTaskGet(taskId)).toEqual(before);
		raw.exec("DROP TRIGGER reject_administrative_audit");
	} finally {
		raw.close();
	}
	// Synthetic pre-existing persona inbox makes the mapped surface unavailable.
	f.db.inboundEnqueue({
		messageId: "pending-persona-disposition",
		originKey: originKey(thread),
		originRefJson: JSON.stringify(thread),
		body: "Already admitted persona work",
	});
	const response = await dispositionCli(f, request);
	expect(response.code).toBe(0);
	const result = validateWorkTaskDispositionResult(JSON.parse(response.stdout));
	expect(result.deliveryId).toBeNull();
	expect(f.db.workTaskSourceGet(sourceId)?.deliveryId).toBeNull();
	expect(f.db.deliveryGet(deliveryId)).toBeUndefined();
	expect((await dispositionCli(f, request)).code).toBe(0);
	expect(f.db.workControlGet(control.controlId)).toEqual(control);
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
}, 30_000);

test("actual CLI disposition fails closed on corrupt original storage without an intent", async () => {
	const f = await fixture();
	const control = await heldControl(f);
	const request = dispositionRequest(f, {
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?").run('{"taskId":false}', taskId);
		expect((await dispositionCli(f, request)).code).toBe(1);
		expect(raw.query("SELECT source_id FROM work_task_sources WHERE source_id = ?").get(sourceId)).toBeNull();
		expect(f.db.deliveryGet(workTaskSourceDeliveryId(taskId, sourceId, thread))).toBeUndefined();
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		raw.close();
	}
}, 20_000);

test("actual CLI disposition revalidates original mutation worktree inside lane admission", async () => {
	const f = await fixture();
	const primary = join(f.directory, "workspace");
	const worker = join(f.directory, "worker");
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", primary, ...args], { stdout: "pipe", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(result.stderr.toString());
	};
	git("init");
	git(
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"fixture",
	);
	git("worktree", "add", "-b", "disposition-worker", worker);
	const assignment = f.assignment();
	expect(
		(await f.request("work.start", { ...assignment, cwd: worker, task: { ...assignment.task, kind: "code_mutating" } }))
			.error,
	).toBeUndefined();
	await f.bind();
	await f.confirm();
	f.setOnSteer(async () => {
		throw new Error("synthetic lost original mutation receipt");
	});
	const original = f.db.workTaskGet(taskId)!;
	const steered = await f.request("work.steer", {
		name: original.laneName,
		taskId,
		expectedOpRef: original.opRef,
		eventId: "cli:mutation-original",
		text: "Inspect dedicated worktree",
	});
	expect(steered.error).toBeUndefined();
	const control = f.db.workControlGet(steered.result.controlId)!;
	expect(control.phase).toBe("held");
	const request = dispositionRequest(f, {
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	const before = f.db.workTaskGet(taskId)!;
	const proof = f.db.workTaskSourceGet(`worktree-admission-${taskId}`);
	// Drift after the public call has queued, before its exclusive admission callback.
	const exclusive = f.port.runExclusive.bind(f.port);
	let drifted = false;
	const gate = spyOn(f.port, "runExclusive").mockImplementation(
		async <T>(key: string, work: () => Promise<T>): Promise<T> => {
			if (!drifted && key === `work/task/${original.laneName}`) {
				drifted = true;
				await rename(primary, join(f.directory, "moved-coordinator"));
				await mkdir(primary);
			}
			return exclusive(key, work);
		},
	);
	try {
		expect((await dispositionCli(f, request)).code).toBe(1);
		expect(drifted).toBe(true);
		expect(f.db.workTaskGet(taskId)).toEqual(before);
		expect(f.db.workControlGet(control.controlId)).toEqual(control);
		expect(f.db.workTaskSourceGet(`worktree-admission-${taskId}`)).toEqual(proof);
		const sourceId = workTaskDispositionId(taskId, request.eventId, LOOPBACK_ORIGIN);
		expect(f.db.workTaskSourceGet(sourceId)).toBeUndefined();
		expect(f.db.deliveryGet(workTaskSourceDeliveryId(taskId, sourceId, thread))).toBeUndefined();
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.sends).toHaveLength(1);
	} finally {
		gate.mockRestore();
	}
}, 30_000);

test("real Unix status fences pending original tasks before any session operation", async () => {
	const f = await fixture();
	await f.admit();
	const task = f.db.workTaskGet(taskId)!;
	const status = spyOn(f.port, "status");
	const liveness = spyOn(f.port, "liveness");
	const params = { name: task.laneName, taskId, expectedOpRef: task.opRef };
	const response = await f.request("work.status", params);
	expect(response.error).toBeUndefined();
	expect(response.result).toMatchObject({
		jobId: task.jobId,
		state: "pending",
		sessionId: "",
		attempt: null,
		op: null,
		task: {
			taskId,
			name: task.laneName,
			opRef: task.opRef,
			sessionId: null,
			epoch: null,
			surface: { phase: "pending" },
			obligation: "awaiting_final",
			finalReport: { reportId: null, completeness: "unavailable", disposition: "pending" },
		},
	});
	expect((await f.request("work.status", { name: task.laneName })).result.task).toEqual(response.result.task);
	expect(validateWorkTaskDispositionBasis(response.result.task.dispositionBasis)).toMatchObject({
		sessionId: null,
		epoch: null,
		expectedTaskVersion: task.version,
		controls: [],
		controlsCompleteness: "complete",
		report: null,
	});
	const jobs = await f.request("work.jobs");
	expect(jobs.error).toBeUndefined();
	expect(
		jobs.result.tasks.find((value: { taskId: string }) => value.taskId === taskId)?.dispositionBasis,
	).toBeUndefined();
	const unknown = crypto.randomUUID();
	for (const invalid of [
		{ ...params, expectedOpRef: "gw-wrong" },
		{ ...params, name: "other" },
		{ ...params, taskId: unknown },
		{ name: `fm-${unknown}`, taskId: unknown, expectedOpRef: task.opRef },
		{ name: task.laneName, expectedOpRef: task.opRef },
		{ ...params, taskId: "malformed" },
	])
		expect((await f.request("work.status", invalid)).error?.code).toBe("invalid_params");
	expect(status).not.toHaveBeenCalled();
	expect(liveness).not.toHaveBeenCalled();
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.workerOutputReads).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.db.workTaskGet(taskId)).toEqual(task);
	status.mockRestore();
	liveness.mockRestore();
});

test("real Unix status exposes prepared and final task evidence without declaring mandate success", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	expect(task.dispatchPhase).toBe("prepared");
	const params = { name: task.laneName, taskId, expectedOpRef: task.opRef };
	const prepared = await f.request("work.status", params);
	expect(prepared.error).toBeUndefined();
	expect(prepared.result).toMatchObject({
		sessionId: task.sessionId,
		attempt: { opRef: task.opRef },
		task: {
			taskId,
			opRef: task.opRef,
			sessionId: task.sessionId,
			epoch: task.epoch,
			obligation: "awaiting_final",
			finalReport: { completeness: "unavailable", disposition: "pending" },
		},
	});
	f.port.complete(task.opRef, "Original final evidence");
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
	expect(f.db.workTaskGet(taskId)?.holdReason).toBeNull();
	expect((await f.request("work.task.recover", { taskId })).error).toBeUndefined();
	const final = await f.request("work.status", params);
	expect(final.error).toBeUndefined();
	expect(final.result.task).toMatchObject({
		taskId,
		opRef: task.opRef,
		sessionId: task.sessionId,
		epoch: task.epoch,
		obligation: "final_admitted",
		finalReport: {
			completeness: "complete",
			disposition: "admitted",
			reportId: f.db.workTaskGet(taskId)!.terminalReportId,
		},
	});
	expect(final.result.attempt.opRef).toBe(task.opRef);
	expect(final.result.op).toMatchObject({ status: "terminal_ok", clientRef: task.opRef, receiptState: "present" });
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
});

test("real Unix status exposes a surface hold before a lane job exists", async () => {
	const f = await fixture();
	await f.admit();
	expect((await f.request("work.thread.claim", { taskId, claimId })).error).toBeUndefined();
	expect(
		(
			await f.request("work.thread.bind", {
				taskId,
				claimId,
				outcome: { kind: "held", reason: "original surface inaccessible" },
			})
		).error,
	).toBeUndefined();
	const task = f.db.workTaskGet(taskId)!;
	const status = spyOn(f.port, "status");
	const response = await f.request("work.status", {
		name: task.laneName,
		taskId,
		expectedOpRef: task.opRef,
	});
	expect(response.error).toBeUndefined();
	expect(response.result).toMatchObject({
		jobId: task.jobId,
		state: "held",
		sessionId: "",
		attempt: null,
		op: null,
		task: {
			taskId,
			opRef: task.opRef,
			sessionId: null,
			epoch: null,
			surface: { phase: "held", reason: "original surface inaccessible" },
			finalReport: { completeness: "unavailable" },
		},
	});
	expect(status).not.toHaveBeenCalled();
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	status.mockRestore();
});

test("real Unix status retains held original report incompleteness", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.complete(task.opRef, "x".repeat(16 * 1024 + 1));
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "held");
	const response = await f.request("work.status", {
		name: task.laneName,
		taskId,
		expectedOpRef: task.opRef,
	});
	expect(response.error).toBeUndefined();
	expect(response.result.task).toMatchObject({
		taskId,
		opRef: task.opRef,
		sessionId: task.sessionId,
		epoch: task.epoch,
		obligation: "held",
		finalReport: { completeness: "unavailable", disposition: "held" },
	});
	expect(response.result.task.holdReason).toBe(f.db.workTaskGet(taskId)!.holdReason);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
});

test("real wire full source read and coordinator review stay separate from consumed handoff and owner answer", async () => {
	const f = await fixture();
	const key = originKey(LOOPBACK_ORIGIN);
	const persona = await f.port.bind({ originKey: key, epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(key, JSON.stringify(LOOPBACK_ORIGIN));
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const text = "한".repeat(5449) + "x\nOwner decision: choose red or blue?";
	f.port.complete(task.opRef, text);
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
	const original = f.db.workTaskOriginalSource(taskId)!;
	await until(() => f.port.sends.some((send) => send.sessionId === persona.sessionId));
	const turn = f.port.sends.find((send) => send.sessionId === persona.sessionId)!;
	expect(turn.text).toContain(original.contentHash);
	expect(turn.text).toContain("pending_reviews");
	f.port.complete(turn.opRef!, "[SILENT]");
	const raw = new Database(f.config.dbPath);
	try {
		await until(
			() =>
				raw
					.query<{ state: string }, [string]>("SELECT state FROM inbound_messages WHERE message_id = ?")
					.get(original.reportId!)?.state === "done",
		);
	} finally {
		raw.close();
	}
	const sends = f.port.sends.length;
	await f.restart();
	expect(f.port.sends).toHaveLength(sends);
	const pending = await f.request("work.task.context", { mode: "pending_reviews", callerSessionId: persona.sessionId });
	expect(pending.error).toBeUndefined();
	expect(pending.result.items[0]).toMatchObject({ taskId, contentHash: original.contentHash, pending: true });
	const snapshot = await f.request("work.task.context", { taskId });
	expect(JSON.stringify(snapshot.result)).toContain(original.contentHash);
	let cursor: string | undefined;
	let body = "";
	do {
		const response = await f.request("work.task.context", {
			mode: "source",
			taskId,
			sourceId: original.sourceId,
			contentHash: original.contentHash,
			...(cursor ? { cursor } : {}),
		});
		expect(response.error).toBeUndefined();
		expect(Buffer.byteLength(JSON.stringify(response) + "\n")).toBeLessThanOrEqual(16384);
		body += response.result.body;
		cursor = response.result.nextCursor;
		expect(response.result.eof).toBe(!cursor);
	} while (cursor);
	expect(body).toBe(text);
	const review = {
		taskId,
		expectedOpRef: task.opRef,
		reportId: original.reportId!,
		sourceId: original.sourceId,
		contentHash: original.contentHash,
		reviewId: crypto.randomUUID(),
		expectedReviewId: null,
		callerSessionId: persona.sessionId,
		callerEpoch: f.db.getSessionRecord(key)!.epoch,
		fullRead: true,
		disposition: "owner_question",
		rationale: "Full original asks for owner's selection.",
		question: "Choose red or blue?",
	};
	expect((await f.request("work.task.review", { ...review, callerSessionId: task.sessionId })).error).toBeDefined();
	expect((await f.request("work.task.review", { ...review, callerEpoch: review.callerEpoch + 1 })).error).toBeDefined();
	expect((await f.request("work.task.review", { ...review, fullRead: false })).error).toBeDefined();
	const result = await f.request("work.task.review", review);
	expect(result.error).toBeUndefined();
	expect(result.result).toMatchObject({ execution: "none", disposition: "recorded" });
	const questionDelivery = f.db.deliveryGet(result.result.deliveryId)!;
	expect(questionDelivery.origin_key).toBe(key);
	expect(JSON.parse(questionDelivery.payload_json).text).toContain(review.question);
	expect((await f.request("work.task.review", review)).result.disposition).toBe("duplicate");
	expect(f.db.workTaskReviewLocator(taskId)?.ownerQuestions).toHaveLength(1);
	expect(f.db.workTaskReviewLocator(taskId)?.pending).toBe(false);
	expect(f.port.sends).toHaveLength(sends);
	expect(f.port.resumes).toHaveLength(0);
});

test("real Unix status never adopts a replacement binding or hides original held debt", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.complete(task.opRef, "x".repeat(16 * 1024 + 1));
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	const runtime = f.db.workAttemptGet(task.opRef)!;
	const params = { name: task.laneName, taskId, expectedOpRef: task.opRef };
	const status = spyOn(f.port, "status").mockRejectedValue(new Error("original host unavailable"));
	const dead = await f.request("work.status", params);
	expect(dead.error).toBeUndefined();
	expect(dead.result).toMatchObject({
		sessionId: task.sessionId,
		op: null,
		task: {
			sessionId: task.sessionId,
			epoch: task.epoch,
			opRef: task.opRef,
			obligation: "held",
			finalReport: { completeness: "unavailable", disposition: "held" },
		},
	});
	expect(status).toHaveBeenCalledTimes(1);
	expect(status.mock.calls.every(([input]) => input.sessionId === task.sessionId && input.opRef === task.opRef)).toBe(
		true,
	);
	status.mockClear();
	const replacementPort = attachTestBrokerOwnership(
		f.db,
		new ScriptedSessionPort({ sessionIdForBind: () => crypto.randomUUID() }),
		join(f.directory, "agent"),
	);
	// Synthetic epoch transition through the supported store API, not live GJC restoration evidence.
	const epoch = f.db.rebindEpoch(runtime.sessionKey);
	await replacementPort.bind({ originKey: runtime.sessionKey, epoch, repo: f.directory, codingRegister: true });
	const retired = await f.request("work.status", params);
	expect(retired.error).toBeUndefined();
	expect(retired.result.task).toEqual(dead.result.task);
	expect(validateWorkTaskDispositionBasis(retired.result.task.dispositionBasis)).toMatchObject({
		sessionId: task.sessionId,
		epoch: task.epoch,
		expectedOpRef: task.opRef,
		cwd: task.request.cwd,
		requestHash: task.requestHash,
		report: { kind: "report", reportId: f.db.workTaskGet(taskId)!.terminalReportId },
	});
	expect(retired.result.sessionId).toBe(task.sessionId);
	expect(retired.result.op).toBeNull();
	expect(retired.result.attempt.opRef).toBe(task.opRef);
	for (const invalid of [
		{ ...params, expectedOpRef: "gw-wrong" },
		{ ...params, name: "other" },
		{ ...params, taskId: crypto.randomUUID() },
	])
		expect((await f.request("work.status", invalid)).error?.code).toBe("invalid_params");
	expect(status).not.toHaveBeenCalled();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	status.mockRestore();
});

test("task assignment, claim, bind, platform floor, direct control and original result use one lane", async () => {
	const f = await fixture();
	expect((await f.admit()).result).toMatchObject({ taskId, execution: "pending_surface" });
	expect(f.port.sends).toHaveLength(0);
	const task = f.db.workTaskGet(taskId)!;
	expect(task.request.coordinator).toEqual(LOOPBACK_ORIGIN);
	expect(task.request.evidence.principalId).toBe("local-ipc:owner");
	await f.restart();
	expect(f.port.sends).toHaveLength(0);
	expect((await f.bind()).error).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	expect((await f.bind()).result.disposition).toBe("duplicate");
	expect(f.port.sends).toHaveLength(1);
	const before = await f.request("chat.send", event());
	expect(before.result).toMatchObject({ route: "work_task", taskId, opRef: task.opRef, acceptance: "durable" });
	expect(before.result.turnId).toBeUndefined();
	expect(before.result.engaged).toBeUndefined();
	expect(f.port.steers).toHaveLength(0);
	expect((await f.confirm()).error).toBeUndefined();
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)?.evidence.evidenceAt).toBe(new Date(floor).toISOString());
	expect(f.port.steers).toHaveLength(1);
	const duplicate = await f.request(
		"chat.send",
		event({ originSource: { platformCreatedAt: floor + 100, recovered: true } }),
	);
	expect(duplicate.result.controlId).toBe(before.result.controlId);
	expect(f.port.steers).toHaveLength(1);
	f.port.complete(task.opRef, "Original complete result with evidence");
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	const recovered = await f.request("work.task.recover", { taskId });
	expect(recovered.error).toBeUndefined();
	expect(f.port.sends.filter((send) => send.opRef === task.opRef)).toHaveLength(1);
	expect(f.db.workTaskGet(taskId)?.opRef).toBe(task.opRef);
	expect(f.port.workerOutputReads.every((read) => read.opRef === task.opRef)).toBe(true);
	const original = f.db
		.workTaskSources(taskId)
		?.sources.find((source) => source.body === "Original complete result with evidence" && source.deliveryId);
	expect(original).toBeDefined();
	expect(JSON.parse(f.db.deliveryGet(original!.deliveryId!)!.payload_json).text).toBe(
		"Original complete result with evidence",
	);
});

test("mapped invalid owner, bot, unknown edit and missing creation evidence never reach persona", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	for (const [verb, params] of [
		["chat.send", event({ engagement: { authorId: "99", mentioned: true, group: true } })],
		["chat.send", event({ engagement: { authorId: "42", authorIsBot: true, mentioned: true, group: true } })],
		["chat.send", event({ originSource: undefined })],
		["chat.send", event({ originSource: { platformCreatedAt: "bad" } })],
		["chat.send", event({ originSource: { platformCreatedAt: floor + 999 } })],
		["chat.edit", event({ messageId: snowflake(floor + 101) })],
		["chat.send", event({ messageId: "malformed" })],
		["chat.send", event({ origin: { ...thread, boundaryId: "2" } })],
	] as const)
		expect((await f.request(verb, params)).error).toBeDefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.db.inboundPendingCount(originKey(thread))).toBe(0);
	expect(f.db.getSession(originKey(thread))).toBeUndefined();
});

test("mapped reset, model and restart commands never execute persona commands; cancel stays local", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	for (const [index, text] of ["/new", "/reset", "/model", "/restart", "/cancel"].entries()) {
		const response = await f.request(
			"chat.send",
			event({
				text,
				messageId: snowflake(floor + 110 + index),
				originSource: { platformCreatedAt: floor + 110 + index },
			}),
		);
		expect(response.result?.route).toBe("work_task");
	}
	const controls = f.db.workControlList(taskId);
	expect(controls.map((control) => control.request.kind)).toEqual([
		"reset_notice",
		"reset_notice",
		"steer",
		"steer",
		"cancel_request",
	]);
	expect(f.port.models).toHaveLength(0);
	expect(f.port.sends.filter((send) => send.opRef === f.db.workTaskGet(taskId)!.opRef)).toHaveLength(1);
	expect(f.db.getSession(originKey(thread))).toBeUndefined();
	expect((await f.request("work.jobs")).result.tasks[0].taskId).toBe(taskId);
});

test("edits retain original creation time, deterministic identity, and reject pre-floor history", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	const old = event({
		messageId: snowflake(floor - 100),
		originSource: { platformCreatedAt: floor - 100 },
		receivedAt: new Date(floor + 1000).toISOString(),
	});
	expect((await f.request("chat.send", old)).error).toBeDefined();
	await f.request("chat.send", event());
	const edited = event({ text: "Inspect the updated evidence", receivedAt: new Date(floor + 2000).toISOString() });
	const first = await f.request("chat.edit", edited);
	expect(first.result.route).toBe("work_task");
	expect((await f.request("chat.edit", edited)).result.controlId).toBe(first.result.controlId);
	expect(f.db.workControlGet(first.result.controlId)?.request.evidence.evidenceAt).toBe(
		new Date(floor + 100).toISOString(),
	);
	expect(f.port.steers).toHaveLength(2);
});

test("whole receipt requires exact origin and immutable complete ID fingerprint", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	expect((await f.request("delivery.confirm", { deliveryId: f.marker() })).error).toBeDefined();
	expect(
		(
			await f.request("delivery.confirm", {
				deliveryId: f.marker(),
				platformReceipt: {
					origin: { ...thread, boundaryId: "2" },
					messageIds: [snowflake(floor)],
				},
			})
		).error,
	).toBeDefined();
	expect((await f.confirm([])).error).toBeDefined();
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	const ids = [snowflake(floor), snowflake(floor, 1)];
	expect((await f.confirm(ids)).error).toBeUndefined();
	expect((await f.confirm(ids)).error).toBeUndefined();
	expect((await f.confirm([ids[0]!])).error).toBeDefined();
	expect((await f.confirm([ids[0]!, snowflake(floor, 2)])).error).toBeDefined();
	expect(f.db.deliveryGet(f.marker())?.state).toBe("confirmed");
});

test("confirmation and activation source roll back as one transaction", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	expect(() => f.db.deliveryConfirmWithSettleInTransaction(f.marker(), "delivered")).toThrow(
		"operation requires a database transaction",
	);
	expect((await f.request("chat.send", event())).error).toBeUndefined();
	expect(f.port.steers).toHaveLength(0);
	const before = f.db.deliveryGet(f.marker())!.state;
	const append = f.db.workTaskSourceAppendInTransaction.bind(f.db);
	const failure = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation((source, delivery) => {
		if (source.sourceId === `activation-floor-${taskId}`) throw new Error("injected activation storage failure");
		return append(source, delivery);
	});
	try {
		expect((await f.confirm()).error).toBeDefined();
	} finally {
		failure.mockRestore();
	}
	expect(f.db.deliveryGet(f.marker())?.state).toBe(before);
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	expect(f.db.workTaskSources(taskId)?.sources.some((source) => source.sourceId.startsWith("receipt-"))).toBe(false);
	expect(f.port.steers).toHaveLength(0);
	expect((await f.confirm()).error).toBeUndefined();
	expect(f.port.steers).toHaveLength(1);
});

test("expired and failed mapped deliveries cannot create activation evidence", async () => {
	for (const state of ["expired", "failed_ambiguous"] as const) {
		const f = await fixture();
		await f.admit();
		await f.bind();
		f.db.deliveryUpdate(f.marker(), state);
		expect((await f.confirm()).error).toBeDefined();
		expect(f.db.deliveryGet(f.marker())?.state).toBe(state);
		expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	}
});

test("bound ordinary bypass and unresolved caller hints cannot dispatch work", async () => {
	const f = await fixture();
	for (const hint of [null, "", "unknown-session"]) {
		expect((await f.request("work.start", { ...f.assignment(), callerSessionId: hint })).error).toBeDefined();
	}
	expect(f.db.workTaskGet(taskId)).toBeUndefined();
	await f.admit();
	await f.bind();
	for (const verb of ["work.start", "work.run", "work.steer"]) {
		expect((await f.request(verb, { name: `fm-${taskId}`, cwd: f.directory, text: "bypass" })).error).toBeDefined();
	}
	const secondId = crypto.randomUUID();
	expect(
		(await f.request("work.start", { ...f.assignment(secondId), callerSessionId: f.db.workTaskGet(taskId)!.sessionId }))
			.error,
	).toBeDefined();
	expect(f.port.sends).toHaveLength(1);
});

test("ordinary work start, status, steer, run and receipt remain available", async () => {
	const f = await fixture();
	const started = await f.request("work.start", { name: "ordinary", text: "Inspect", cwd: f.directory });
	expect(started.error).toBeUndefined();
	expect((await f.request("work.status", { name: "ordinary" })).error).toBeUndefined();
	expect((await f.request("work.steer", { name: "ordinary", text: "Inspect one more fact" })).error).toBeUndefined();
	const running = f.request("work.run", { name: "ordinary-two", text: "Inspect separately", cwd: f.directory });
	await until(() => f.port.sends.length === 2);
	f.port.complete(f.port.sends[1]!.opRef, "Ordinary result");
	expect((await running).error).toBeUndefined();
	const deliveryId = "ordinary-confirmation";
	f.db.deliveryCreate({
		id: deliveryId,
		turnId: "ordinary-turn",
		originKey: originKey(parent),
		payloadJson: JSON.stringify({
			deliveryId,
			turnId: "ordinary-turn",
			origin: parent,
			role: "assistant",
			text: "Ordinary delivery",
			final: true,
		}),
	});
	expect((await f.request("delivery.confirm", { deliveryId })).error).toBeUndefined();
	expect(f.db.deliveryGet(deliveryId)?.state).toBe("confirmed");
});

test("persona queued before bind wins the race and no worker is created", async () => {
	const f = await fixture();
	await f.admit();
	f.db.inboundEnqueue({
		messageId: "pending-persona",
		originKey: originKey(thread),
		originRefJson: JSON.stringify(thread),
		body: "Already admitted",
	});
	expect((await f.bind()).error).toBeDefined();
	expect(f.port.sends).toHaveLength(0);
	expect(f.db.workTaskGet(taskId)?.thread).toBeNull();
});

test("selected active persona surface refuses assignment without worker creation", async () => {
	const f = await fixture();
	const sent = await f.request(
		"chat.send",
		event({ text: "Ordinary persona work", engagement: { authorId: "42", mentioned: true, group: true } }),
	);
	expect(sent.error).toBeUndefined();
	await until(() => f.port.sends.length === 1);
	try {
		const input = f.assignment();
		const admission = await f.request("work.start", {
			...input,
			task: { ...input.task, surface: { threadOrigin: thread } },
		});
		expect(admission.error).toBeDefined();
		expect(f.db.workTaskGet(taskId)).toBeUndefined();
		expect(f.port.sends).toHaveLength(1);
	} finally {
		f.port.complete(f.port.sends[0]!.opRef, "Original persona result");
	}
});

test("explicit steer uses actual local source and fences original task operation", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	const task = f.db.workTaskGet(taskId)!;
	const input = {
		name: task.laneName,
		taskId,
		expectedOpRef: task.opRef,
		eventId: "cli:owner-direction",
		text: "Inspect this task only",
	};
	expect((await f.request("work.steer", { ...input, expectedOpRef: "gw-wrong" })).error).toBeDefined();
	const first = await f.request("work.steer", input);
	expect(first.result).toMatchObject({ route: "work_task", taskId });
	const evidence = f.db.workControlGet(first.result.controlId)!.request.evidence;
	expect(evidence.origin).toEqual(LOOPBACK_ORIGIN);
	expect(evidence.principalId).toBe("local-ipc:owner");
	expect((await f.request("work.steer", input)).result.controlId).toBe(first.result.controlId);
	expect(f.port.steers).toHaveLength(1);
});

test("current persona hint selects DB origin, while null origin cannot fall back to owner", async () => {
	const f = await fixture();
	const binding = await f.port.bind({ originKey: originKey(parent), epoch: 0, repo: join(f.directory, "workspace") });
	expect(
		(await f.request("work.start", { ...f.assignment(), callerSessionId: binding.sessionId })).error,
	).toBeDefined();
	f.db.updateActivity(originKey(parent), JSON.stringify(parent));
	expect(
		(await f.request("work.start", { ...f.assignment(), callerSessionId: binding.sessionId })).error,
	).toBeUndefined();
	expect(f.db.workTaskGet(taskId)?.request.coordinator).toEqual(parent);
	expect(f.db.workTaskGet(taskId)?.request.evidence.principalId).toBe(`local-ipc:persona:${binding.sessionId}`);
	expect(f.port.sends).toHaveLength(0);
});

test("actual CLI steer routes worker, mapped and unresolved hints without owner fallback", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	await f.confirm();
	const original = f.db.workTaskGet(taskId)!;
	const mapped = await f.port.bind({ originKey: originKey(thread), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(thread), JSON.stringify(thread));
	const persona = await f.port.bind({ originKey: originKey(parent), epoch: 0, repo: join(f.directory, "workspace") });
	const invoke = async (hint: string | undefined, eventId: string) => {
		const env = { ...process.env };
		delete env.GJC_SESSION_ID;
		if (hint !== undefined) env.GJC_SESSION_ID = hint;
		const child = Bun.spawn(
			[
				process.execPath,
				join(import.meta.dir, "../../cli/src/main.ts"),
				"--socket",
				f.config.socketPath,
				"work",
				"steer",
				`fm-${taskId}`,
				"Inspect original evidence",
				"--task-id",
				taskId,
				"--expected-op-ref",
				original.opRef,
				"--event-id",
				eventId,
			],
			{ env, stdout: "pipe", stderr: "pipe" },
		);
		const [code, stdout, stderr] = await Promise.all([
			child.exited,
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
		]);
		return { code, stdout, stderr };
	};
	for (const [index, hint] of [
		original.sessionId!,
		mapped.sessionId,
		crypto.randomUUID(),
		persona.sessionId,
	].entries()) {
		const response = await invoke(hint, `cli:rejected-${index}`);
		expect(response.code).toBe(1);
		expect(response.stderr).toMatch(/current non-work persona required|mapped conversation is not a persona/);
		expect(f.db.workControlList(taskId)).toHaveLength(0);
		expect(
			f.db.workTaskSources(taskId)?.sources.some((source) => source.evidence.eventId === `cli:rejected-${index}`),
		).toBe(false);
		expect(f.port.steers).toHaveLength(0);
		expect(f.port.sends).toHaveLength(1);
	}
	f.db.updateActivity(originKey(parent), JSON.stringify(parent));
	for (const [hint, eventId, expectedOrigin, principalId] of [
		[persona.sessionId, "cli:persona-steer", parent, `local-ipc:persona:${persona.sessionId}`],
		[undefined, "cli:owner-steer", LOOPBACK_ORIGIN, "local-ipc:owner"],
	] as const) {
		const response = await invoke(hint, eventId);
		expect(response.code).toBe(0);
		expect(response.stderr).toBe("");
		const result = JSON.parse(response.stdout.trim().split("\n").at(-1)!);
		expect(result).toMatchObject({ route: "work_task", taskId, opRef: original.opRef });
		const control = f.db.workControlGet(result.controlId)!;
		expect(control.request.evidence).toMatchObject({ origin: expectedOrigin, principalId, eventId });
		const source = f.db.workTaskSources(taskId)?.sources.find((entry) => entry.evidence.eventId === eventId);
		expect(source?.evidence).toEqual(control.request.evidence);
	}
	expect(f.db.workControlList(taskId)).toHaveLength(2);
	expect(f.port.steers).toHaveLength(2);
	expect(f.port.sends).toHaveLength(1);
}, 20_000);

test("wrong persisted task metadata cannot confirm a mapped payload", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const row = f.db.deliveryGet(f.marker())!;
	const raw = new Database(f.config.dbPath);
	try {
		const payload = JSON.parse(row.payload_json);
		payload.workTask.taskId = crypto.randomUUID();
		raw.query("UPDATE deliveries SET payload_json = ? WHERE delivery_id = ?").run(JSON.stringify(payload), f.marker());
		expect((await f.confirm()).error).toBeDefined();
		expect(f.db.deliveryGet(f.marker())?.state).toBe(row.state);
		expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	} finally {
		raw.close();
	}
});

test("corrupt permanent mapping fails closed before command or persona fallback", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?").run('{"taskId":false}', taskId);
	} finally {
		raw.close();
	}
	for (let repeat = 0; repeat < 3; repeat++) {
		const discovery = await f.request("work.jobs");
		expect(discovery.error).toBeUndefined();
		expect(discovery.result.tasks).toEqual([]);
		expect(discovery.result.taskErrors).toEqual([{ taskId, reason: expect.any(String) }]);
		expect(discovery.result.nextTaskId).toBeUndefined();
		expect((await f.request("work.thread.claim", { taskId, claimId })).error).toBeDefined();
		expect(
			(await f.request("work.thread.bind", { taskId, claimId, outcome: { kind: "bound", origin: thread } })).error,
		).toBeDefined();
		expect((await f.request("chat.send", event({ text: "/new" }))).error).toBeDefined();
		expect(f.db.workTaskDiscordLocator(thread.conversationId)).toEqual({ taskId });
	}
	expect(f.db.getSession(originKey(thread))).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
});

for (const state of ["active", "corrupt", "malformed", "settled"] as const) {
	test(`permanent conversation fence rejects changed parent/boundary before persona routes (${state})`, async () => {
		const f = await fixture();
		await f.admit();
		await f.bind();
		await f.confirm();
		const original = f.db.workTaskGet(taskId)!;
		if (state === "settled") {
			f.port.complete(original.opRef, "Original final result");
			await until(() => f.db.workAttemptGet(original.opRef)?.settledAt != null);
			await f.restart();
		}
		if (state === "corrupt" || state === "malformed") {
			const raw = new Database(f.config.dbPath);
			try {
				raw
					.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?")
					.run(
						state === "malformed"
							? '{"taskId":false}'
							: JSON.stringify({ ...original, thread: { ...thread, conversationId: "999" } }),
						taskId,
					);
			} finally {
				raw.close();
			}
		}
		const binds = f.port.binds.length;
		const closes = f.port.closes.length;
		const contextRecord = spyOn(f.db, "contextRecord");
		const contextUpdate = spyOn(f.db, "contextUpdateBody");
		try {
			expect(
				(
					await f.request("engagement.panel_response", {
						origin: thread,
						panelId: "persona-panel",
						responseKind: "approved",
						responderId: "42",
						engagement: { authorId: "42" },
					})
				).error,
			).toMatchObject({ code: "unauthorized" });
			for (const origin of [
				{ ...thread, parentId: "101" },
				{ ...thread, boundaryId: "2" },
				{ ...thread, parentId: "101", boundaryId: "2" },
				{ ...parent, conversationId: thread.conversationId },
				...(state === "corrupt" || state === "malformed" ? [thread] : []),
			]) {
				for (const text of ["ordinary message", "/new", "/reset", "/model", "/cancel"]) {
					for (const verb of ["chat.send", "chat.edit"]) {
						const response = await f.request(
							verb,
							event({
								origin,
								text,
								originSource: { platformCreatedAt: floor + 100, recovered: true },
								engagement: { authorId: "42", mentioned: true, group: true },
							}),
						);
						expect(response.error).toBeDefined();
						expect(response.result).toBeUndefined();
					}
				}
				expect(
					(
						await f.request("engagement.reaction", {
							origin,
							targetMessageId: snowflake(floor),
							emoji: "ack",
							action: "add",
							engagement: { authorId: "42" },
						})
					).error,
				).toBeDefined();
				expect(
					(
						await f.request("engagement.panel_response", {
							origin,
							panelId: "persona-panel",
							responseKind: "approved",
							responderId: "42",
							engagement: { authorId: "42" },
						})
					).error,
				).toMatchObject({ code: "unauthorized" });
				expect(f.db.getSession(originKey(origin))).toBeUndefined();
				expect(f.db.inboundPendingCount(originKey(origin))).toBe(0);
			}
			expect(contextRecord).not.toHaveBeenCalled();
			expect(contextUpdate).not.toHaveBeenCalled();
			expect(f.port.sends).toHaveLength(1);
			expect(f.port.steers).toHaveLength(0);
			expect(f.port.models).toHaveLength(0);
			expect(f.port.binds).toHaveLength(binds);
			expect(f.port.closes).toHaveLength(closes);
			expect(f.db.workTaskDiscordLocator(thread.conversationId)).toEqual({ taskId });
			expect(f.db.workTaskDiscordLocator("999")).toBeUndefined();
		} finally {
			contextRecord.mockRestore();
			contextUpdate.mockRestore();
		}
	});
}

test("routing locator authenticates owner before reading corrupt task facts", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?").run('{"taskId":false}', taskId);
	} finally {
		raw.close();
	}
	const readTask = spyOn(f.db, "workTaskGet");
	try {
		const response = await f.request(
			"chat.send",
			event({ origin: { ...thread, parentId: "101" }, engagement: { authorId: "99", mentioned: true, group: true } }),
		);
		expect(response.error?.code).toBe("unauthorized");
		expect(readTask).not.toHaveBeenCalled();
		expect(f.port.sends).toHaveLength(1);
	} finally {
		readTask.mockRestore();
	}
});

test("genuinely unmapped Discord conversation remains reachable after another task binds", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const origin = { ...thread, conversationId: "201" };
	expect(f.db.workTaskDiscordLocator(origin.conversationId)).toBeUndefined();
	const response = await f.request(
		"chat.send",
		event({ origin, engagement: { authorId: "42", mentioned: true, group: true } }),
	);
	expect(response.error).toBeUndefined();
	expect(response.result?.engaged).toBe(true);
	await until(() => f.port.sends.length === 2);
	f.port.complete(f.port.sends[1]!.opRef, "Ordinary persona result");
	expect(f.db.getSession(originKey(origin))).toBeDefined();
});

test("changed-parent persona session hint cannot authorize a mapped conversation", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const origin = { ...thread, parentId: "101" };
	const binding = await f.port.bind({ originKey: originKey(origin), epoch: 0, repo: join(f.directory, "workspace") });
	f.db.updateActivity(originKey(origin), JSON.stringify(origin));
	const id = crypto.randomUUID();
	const response = await f.request("work.start", { ...f.assignment(id), callerSessionId: binding.sessionId });
	expect(response.error?.code).toBe("unauthorized");
	expect(f.db.workTaskGet(id)).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("task jobs cursor and context preserve corrupt task isolation without invented operation IDs", async () => {
	const f = await fixture();
	const ids = Array.from({ length: 22 }, () => crypto.randomUUID()).sort();
	for (const id of ids) expect((await f.request("work.start", f.assignment(id))).error).toBeUndefined();
	const raw = new Database(f.config.dbPath);
	try {
		raw.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?").run('{"taskId":false}', ids[0]!);
	} finally {
		raw.close();
	}
	const first = (await f.request("work.jobs", {})).result;
	expect(first.tasks).toHaveLength(19);
	expect(first.taskErrors).toHaveLength(1);
	expect(first.taskErrors[0]).toMatchObject({ taskId: ids[0] });
	expect(first.taskErrors[0].opRef).toBeUndefined();
	expect(first.nextTaskId).toBe(ids[19]);
	const second = (await f.request("work.jobs", { afterTaskId: first.nextTaskId })).result;
	expect(second.tasks).toHaveLength(2);
	expect(second.nextTaskId).toBeUndefined();
	const context = (await f.request("work.task.context", {})).result;
	expect(context.completeness).not.toBe("complete");
	expect(Buffer.byteLength(JSON.stringify(context))).toBeLessThanOrEqual(16 * 1024);
	expect(context.manifest.length).toBeGreaterThan(0);
	expect(f.port.sends).toHaveLength(0);
});

import { Database } from "bun:sqlite";
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type ChatMessagePayload,
	LOOPBACK_ORIGIN,
	type OriginRef,
	originKey,
	type WorkTaskDispositionParams,
	type WorkTaskReviewParams,
} from "@gajae-gateway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord } from "@gajae-gateway/subsession";
import { buildDeliveryPayload } from "../src/delivery/delivery";
import {
	BrokerAuthorityError,
	GatewayDatabase,
	type WorkAttemptRuntime,
	WorkAttemptStateError,
	type WorkControl,
	type WorkControlReceipt,
	type WorkControlRequest,
	type WorkControlRouteAdmission,
	type WorkControlScopeAdmission,
	type WorkTask,
	type WorkTaskEvidence,
	type WorkTaskRequest,
	type WorkTaskSourceInput,
	WorkTaskStateError,
	workAttemptDeliveryId,
	workAttemptReportId,
	workTaskDispositionId,
	workTaskDispositionText,
	workTaskSourceDeliveryId,
} from "../src/store/db";

const START = "2026-10-06T00:00:00.000Z";
const LATER = "2026-10-06T00:01:00.000Z";
const TASK = "bd2f2494-2584-4d13-b7b6-c6ac24a1087f";
const OTHER = "cd2f2494-2584-4d13-b7b6-c6ac24a1087f";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const THREAD: OriginRef = {
	platform: "discord",
	kind: "thread",
	conversationId: "1556589606403842128",
	parentId: "1511673764574793798",
	boundaryId: "1510336487894286436",
};
const evidence: WorkTaskEvidence = {
	principalId: "owner:123",
	origin: THREAD,
	eventId: "discord:assignment",
	editId: null,
	evidenceAt: START,
	observedAt: START,
};
const request: WorkTaskRequest = {
	text: "Inspect the task transaction boundary.",
	context: "Do not send a second prompt.",
	kind: "read_only",
	cwd: "/work",
	coordinator: LOOPBACK_ORIGIN,
	surface: { thread: THREAD },
	evidence,
};
const directories: string[] = [];
const handles: Array<{ close(): void }> = [];
afterEach(async () => {
	for (const handle of handles.splice(0)) handle.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "work-task-store-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const database = await GatewayDatabase.open(path);
	handles.push(database);
	const canonicalAgentDir = join(directory, "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	const raw = new Database(path);
	handles.push(raw);
	const create = (taskId = TASK, opRef = "gw-firstmate-original", value = request) =>
		database.workTaskCreate({ taskId, opRef, request: value });
	const bind = (task: WorkTask, thread = THREAD) =>
		database.withTransaction(
			() =>
				database.workTaskSurfaceInTransaction(task.taskId, task.version, {
					phase: "bound",
					thread,
					claimId: null,
					at: START,
				})!,
		);
	const prepare = (task: WorkTask) => {
		const sessionKey = `work/task/${task.laneName}`;
		database.recordOwnedBinding({ authority, sessionId: SESSION, originKey: sessionKey, epoch: 0, repo: request.cwd });
		const record = appendAttempt(
			createLaneJobRecord({
				jobId: task.jobId,
				branch: "main",
				worktreePath: request.cwd,
				sessionId: SESSION,
				now: () => new Date(START),
			}),
			{ opRef: task.opRef, sessionId: SESSION, startedAt: START },
		);
		const runtime: WorkAttemptRuntime = {
			opRef: task.opRef,
			jobId: task.jobId,
			laneKey: `work-${task.laneName}`,
			sessionKey,
			sessionId: SESSION,
			epoch: 0,
			cwd: request.cwd,
			startedAt: START,
			mode: "start",
			sendPhase: "prepared",
			sendEvidence: null,
			terminal: null,
			output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
			parent: { kind: "persona", originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
			reportId: workAttemptReportId(database.instanceId, task.jobId, task.opRef),
			wakeReportId: null,
			noticeHash: null,
			deliveryId: workAttemptDeliveryId(database.instanceId, task.jobId, task.opRef),
			decision: "undecided",
			settledAt: null,
			version: 0,
		};
		return { runtime, record };
	};
	return { directory, path, database, raw, authority, create, bind, prepare };
}
function controlRequest(eventId: string, body = "Keep the original identity."): WorkControlRequest {
	return {
		taskId: TASK,
		expectedOpRef: "gw-firstmate-original",
		kind: "steer",
		scope: "read_only",
		body,
		evidence: { ...evidence, eventId, evidenceAt: LATER, observedAt: LATER },
	};
}
const identity = { opRef: "gw-firstmate-original", sessionId: SESSION, epoch: 0 };
function receipt(control: WorkControl, outcome: "accepted" | "refused" = "accepted"): WorkControlReceipt {
	return {
		...identity,
		source: "turn.steer_status",
		clientRef: control.clientRef!,
		eventId: control.request.evidence.eventId,
		outcome,
		observedAt: LATER,
		evidence: "Exact original receipt.",
	};
}
function source(sourceId: string): WorkTaskSourceInput {
	return {
		sourceId,
		taskId: TASK,
		kind: "decision",
		body: "Retain the original attempt.",
		evidence: { ...evidence, eventId: sourceId, evidenceAt: START, observedAt: LATER },
		supersedes: null,
		completeness: "complete",
		controlId: null,
		reportId: null,
	};
}
function payload(task: WorkTask, sourceId: string, text = "Original retained detail."): ChatMessagePayload {
	return {
		turnId: task.opRef,
		deliveryId: workTaskSourceDeliveryId(task.taskId, sourceId, THREAD),
		workTask: { taskId: task.taskId, opRef: task.opRef, sourceId, mappedOnly: true },
		origin: THREAD,
		role: "assistant",
		text,
		final: true,
	};
}

async function linkedUnavailableFixture(mapped = true) {
	const f = await fixture();
	const bound = f.bind(f.create().record);
	if (mapped) {
		const marker = {
			...source(`activation-${TASK}`),
			kind: "observation" as const,
			body: `Assignment ${TASK}\n${bound.request.text}\nScope: ${bound.request.kind}; repository admission is not an OS sandbox.`,
			evidence: { ...evidence, principalId: "gateway" },
		};
		f.database.withTransaction(() =>
			f.database.workTaskSourceAppendInTransaction(marker, {
				...payload(bound, marker.sourceId, marker.body),
				final: false,
			}),
		);
	}
	const { runtime, record } = f.prepare(bound);
	f.database.workAttemptPrepare(runtime, record);
	const task = f.database.workTaskGet(TASK)!;
	f.raw.query("UPDATE work_attempt_runtime SET record_json = '{private-secret' WHERE op_ref = ?").run(task.opRef);
	const recordNegative = (at = LATER, available = true) =>
		f.database.withTransaction(() =>
			f.database.workTaskRecordLinkedUnavailableInTransaction(TASK, at, () => available),
		);
	const readNegative = () =>
		f.database.withTransaction(() => f.database.workTaskLinkedUnavailableSourcesInTransaction(TASK));
	return { ...f, task, recordNegative, readNegative };
}

describe("negative-only linked evidence durability", () => {
	test("negative receipt and ledger roll back together and persist across database reopen", async () => {
		const f = await linkedUnavailableFixture();
		const negative = f.recordNegative()!;
		const deliveryId = negative.deliveryId!;
		const receiptId = `receipt-${createHash("sha256").update(deliveryId).digest("hex")}`;
		const params = {
			deliveryId,
			platformReceipt: { origin: THREAD, messageIds: ["1556589606403842128", "1556589606403842129"] },
		};
		const confirm = () =>
			f.database.withTransaction(() => f.database.workTaskNegativeDeliveryConfirmInTransaction(params));
		const before = f.database.deliveryGet(deliveryId);
		f.raw.exec(`CREATE TRIGGER reject_negative_receipt BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id = '${receiptId}' BEGIN SELECT RAISE(ABORT, 'receipt write failed'); END`);
		expect(confirm).toThrow("receipt write failed");
		expect(f.database.deliveryGet(deliveryId)).toEqual(before);
		expect(f.database.workTaskSourceGet(receiptId)).toBeUndefined();
		f.raw.exec("DROP TRIGGER reject_negative_receipt");
		expect(confirm()).toBe("transitioned");
		const retained = f.database.workTaskSourceGet(receiptId);
		expect(retained).toBeDefined();
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.withTransaction(() => reopened.workTaskNegativeDeliveryConfirmInTransaction(params))).toBe(
			"already_terminal",
		);
		expect(reopened.workTaskSourceGet(receiptId)).toEqual(retained);
		expect(reopened.deliveryGet(deliveryId)?.state).toBe("confirmed");
		expect(() => reopened.workTaskGet(TASK)).toThrow();
	});

	for (const tamper of ["source", "op", "body", "destination", "receipt", "admission", "activation"] as const)
		test(`negative confirmation rejects ${tamper} without receipt or settlement`, async () => {
			const f = await linkedUnavailableFixture();
			const negative = f.recordNegative()!;
			const deliveryId = negative.deliveryId!;
			const receiptId = `receipt-${createHash("sha256").update(deliveryId).digest("hex")}`;
			const row = f.database.deliveryGet(deliveryId)!;
			const body = JSON.parse(row.payload_json);
			if (tamper === "source") body.workTask.sourceId = `activation-${TASK}`;
			if (tamper === "op") body.workTask.opRef = "gw-wrong-original";
			if (tamper === "body") body.text += " modified";
			if (tamper === "destination") body.origin.conversationId = "999";
			f.raw.query("UPDATE deliveries SET payload_json = ? WHERE delivery_id = ?").run(JSON.stringify(body), deliveryId);
			if (tamper === "admission") f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(TASK);
			if (tamper === "activation")
				f.raw
					.query("UPDATE deliveries SET payload_json = '{}' WHERE delivery_id = ?")
					.run(f.database.workTaskSourceGet(`activation-${TASK}`)!.deliveryId!);
			const before = f.database.deliveryGet(deliveryId);
			const params = {
				deliveryId,
				platformReceipt: {
					origin: THREAD,
					messageIds: tamper === "receipt" ? ["1556589606403842128", "1556589606403842128"] : ["1556589606403842128"],
				},
			};
			// A nonnegative source selects strict handling, which still rejects corruption.
			expect(() =>
				f.database.withTransaction(() => {
					const outcome = f.database.workTaskNegativeDeliveryConfirmInTransaction(params);
					if (outcome === undefined) f.database.workTaskGet(TASK);
				}),
			).toThrow();
			expect(f.database.deliveryGet(deliveryId)).toEqual(before);
			expect(f.database.workTaskSourceGet(receiptId)).toBeUndefined();
		});

	test("valid runtime cannot acquire a negative fact and nonreserved observation names remain valid", async () => {
		const f = await fixture();
		const bound = f.bind(f.create().record);
		const { runtime, record } = f.prepare(bound);
		f.database.workAttemptPrepare(runtime, record);
		expect(
			f.database.withTransaction(() =>
				f.database.workTaskRecordLinkedUnavailableInTransaction(TASK, LATER, () => true),
			),
		).toBeUndefined();
		f.database.withTransaction(() =>
			f.database.workTaskSourceAppendInTransaction(source("unavailability-control-phase")),
		);
		expect(
			f.database.withTransaction(() => f.database.workTaskLinkedUnavailableSourcesInTransaction(TASK)).sources,
		).toEqual([]);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction(source(`unavailability-${"a".repeat(64)}`)),
			),
		).toThrow();
	});

	test("corrupt linked JSON retains first source and mapped intent without repairing or enabling task writes", async () => {
		const f = await linkedUnavailableFixture();
		const taskRow = f.raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(TASK);
		const runtimeRow = f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef);
		const first = f.recordNegative()!;
		expect(first.completeness).toBe("incomplete");
		expect(first.body).toContain("stored admission is not GJC truth");
		expect(first.body).toContain("terminal outcome and safety are unknown");
		expect(first.body).not.toContain("private-secret");
		expect(first.body.length).toBeLessThan(2048);
		expect(first.deliveryId).not.toBeNull();
		expect(JSON.parse(f.database.deliveryGet(first.deliveryId!)!.payload_json)).toMatchObject({
			text: first.body,
			origin: THREAD,
			final: false,
			workTask: { taskId: TASK, opRef: f.task.opRef, sourceId: first.sourceId, mappedOnly: true },
		});
		expect(f.recordNegative("2026-10-07T00:00:00.000Z")).toEqual(first);
		expect(f.readNegative().sources).toEqual([first]);
		expect(f.raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(TASK)).toEqual(taskRow);
		expect(f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef)).toEqual(runtimeRow);
		expect(() => f.database.workTaskGet(TASK)).toThrow();
		expect(() =>
			f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(source("ordinary"))),
		).toThrow();
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = '{changed-private-secret' WHERE op_ref = ?")
			.run(f.task.opRef);
		const changed = f.recordNegative("2026-10-07T00:00:00.000Z")!;
		expect(changed.sourceId).not.toBe(first.sourceId);
		expect(f.database.workTaskSourceGet(first.sourceId)).toEqual(first);
		expect(f.readNegative().sources).toEqual([changed, first]);
		// Corruption can change before quarantine; quarantine freezes that exact row.
		const changedRuntimeRow = f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef);
		f.database.workAttemptQuarantineInvalid(f.task.jobId);
		expect(() =>
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = '{forbidden-after-quarantine' WHERE op_ref = ?")
				.run(f.task.opRef),
		).toThrow("broker authority: quarantined");
		expect(f.recordNegative("2026-10-08T00:00:00.000Z")).toEqual(changed);
		expect(f.database.workTaskSourceGet(first.sourceId)).toEqual(first);
		expect(f.readNegative().sources).toEqual([changed, first]);
		expect(f.raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(TASK)).toEqual(taskRow);
		expect(f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef)).toEqual(
			changedRuntimeRow,
		);
		expect(() => f.database.workTaskGet(TASK)).toThrow();
		expect(() =>
			f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(source("ordinary"))),
		).toThrow();
	});

	test("source insertion failure rolls back the joined intent, not merely the source", async () => {
		const f = await linkedUnavailableFixture();
		const before = f.database.deliveryRows();
		f.raw.exec(`CREATE TRIGGER fail_negative_source BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'unavailability-%'
			BEGIN SELECT RAISE(ABORT, 'fixture source failure'); END`);
		expect(() => f.recordNegative()).toThrow("fixture source failure");
		expect(f.database.deliveryRows()).toEqual(before);
		expect(f.readNegative().sources).toEqual([]);
		f.raw.exec("DROP TRIGGER fail_negative_source");
		expect(f.recordNegative()?.deliveryId).not.toBeNull();
	});

	for (const tamper of [
		"absent-marker",
		"marker-payload",
		"mapping-column",
		"assignment",
		"task-json",
		"request-hash",
		"task-identity",
	] as const)
		test(`negative anchor never invents identity or destination (${tamper})`, async () => {
			const f = await linkedUnavailableFixture(tamper !== "absent-marker");
			if (tamper === "marker-payload") {
				const marker = f.database.workTaskSourceGet(`activation-${TASK}`)!;
				f.raw.query("UPDATE deliveries SET payload_json = '{}' WHERE delivery_id = ?").run(marker.deliveryId!);
			}
			if (tamper === "mapping-column")
				f.raw.query("UPDATE work_tasks SET thread_origin_key = 'discord/thread/999' WHERE task_id = ?").run(TASK);
			if (tamper === "assignment")
				f.raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?").run(`assignment-${TASK}`);
			if (tamper === "task-json") f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(TASK);
			if (tamper === "request-hash")
				f.raw
					.query("UPDATE work_tasks SET record_json = json_set(record_json, '$.requestHash', ?) WHERE task_id = ?")
					.run("0".repeat(64), TASK);
			if (tamper === "task-identity")
				f.raw
					.query(
						"UPDATE work_tasks SET record_json = json_set(record_json, '$.opRef', 'gw-forged-original') WHERE task_id = ?",
					)
					.run(TASK);
			const before = f.database.deliveryRows();
			if (tamper === "absent-marker" || tamper === "marker-payload") {
				const source = f.recordNegative()!;
				expect(source.deliveryId).toBeNull();
				expect(source.body).toContain("null delivery intent");
				expect(f.recordNegative("2026-10-07T00:00:00.000Z")).toEqual(source);
			} else {
				expect(() => f.recordNegative()).toThrow();
				expect(
					f.raw.query("SELECT COUNT(*) AS count FROM work_task_sources WHERE source_id LIKE 'unavailability-%'").get(),
				).toEqual({ count: 0 });
			}
			expect(f.database.deliveryRows()).toEqual(before);
		});

	test("unavailable surface records honest null intent permanently without render-time freshness", async () => {
		const f = await linkedUnavailableFixture();
		const first = f.recordNegative(LATER, false)!;
		expect(first.deliveryId).toBeNull();
		expect(f.recordNegative("2026-10-07T00:00:00.000Z", true)).toEqual(first);
		expect(f.readNegative().sources[0]?.evidence.observedAt).toBe(LATER);
	});
});

function dispositionParams(task: WorkTask, target: WorkTaskDispositionParams["target"]): WorkTaskDispositionParams {
	return {
		taskId: task.taskId,
		jobId: task.jobId,
		expectedOpRef: task.opRef,
		sessionId: task.sessionId,
		epoch: task.epoch,
		cwd: task.request.cwd,
		requestHash: task.requestHash,
		target,
		eventId: "local:administrative:1",
		expectedTaskVersion: task.version,
		outcome: "abandoned",
		reason: "Original receipt remains irrecoverable.",
		evidence: {
			availability: "unavailable",
			detail: "No authoritative remote receipt is available.",
			evidenceAt: null,
		},
	};
}
function dispositionEvidence(params: WorkTaskDispositionParams): WorkTaskEvidence {
	// This fixture stands in for the trusted local server context, not wire params.
	return {
		principalId: "local-ipc:owner",
		origin: LOOPBACK_ORIGIN,
		eventId: params.eventId,
		editId: null,
		evidenceAt: LATER,
		observedAt: LATER,
	};
}

describe("qualified negative administrative disposition", () => {
	async function negative() {
		const f = await linkedUnavailableFixture();
		const fact = f.recordNegative()!;
		const selected = f.database.workTaskDispositionBasis(TASK);
		if (selected.kind !== "validation_unavailable") throw new Error("negative basis missing");
		const params: WorkTaskDispositionParams = {
			...dispositionParams(f.task, { kind: "report", reportId: null }),
			validationUnavailable: selected.qualification,
		};
		const record = (input = params, available = true) =>
			f.database.withTransaction(() =>
				f.database.workTaskNegativeDispositionInTransaction(input, dispositionEvidence(input), () => available),
			);
		return { ...f, fact, selected, params, record };
	}

	test("quarantined corrupt runtime admits only an audit CAS and original mapped notice", async () => {
		const f = await negative();
		f.database.workAttemptQuarantineInvalid(f.task.jobId);
		const frozen = () =>
			["work_attempt_runtime", "lane_jobs", "work_controls", "broker_owned_bindings", "broker_quarantine"].map(
				(table) => f.raw.query(`SELECT * FROM ${table}`).all(),
			);
		const before = frozen();
		const result = f.record();
		expect(result.execution).toBe("none");
		expect(result.record.retained).toEqual({
			obligationState: "awaiting_final",
			reportId: null,
			holdReason: null,
			controlPhase: null,
		});
		expect(frozen()).toEqual(before);
		const row = f.raw
			.query<{ record_json: string }, [string]>("SELECT record_json FROM work_tasks WHERE task_id = ?")
			.get(TASK)!;
		expect(JSON.parse(row.record_json)).toEqual({ ...f.task, version: f.task.version + 1 });
		expect(f.database.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
		expect(JSON.parse(f.database.deliveryGet(result.deliveryId!)!.payload_json)).toMatchObject({
			final: false,
			workTask: { mappedOnly: true },
			text: workTaskDispositionText(f.params),
		});
		expect(f.record()).toEqual({ ...result, disposition: "duplicate" });
		expect(() => f.record({ ...f.params, reason: "changed" })).toThrow();
		expect(() => f.record({ ...f.params, eventId: "another-event" })).toThrow();
		expect(() => f.database.workTaskGet(TASK)).toThrow();
		expect(() =>
			f.raw.query("UPDATE work_attempt_runtime SET record_json = '{}' WHERE op_ref = ?").run(f.task.opRef),
		).toThrow("broker authority: quarantined");
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
			),
		).toThrow();
	});

	test("failed audit insertion rolls back task CAS and joined delivery", async () => {
		const f = await negative();
		const rows = f.raw.query("SELECT * FROM work_tasks").all();
		const deliveries = f.database.deliveryRows();
		f.raw.exec(`CREATE TRIGGER fail_disposition_source BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'disposition-%'
			BEGIN SELECT RAISE(ABORT, 'fixture audit failure'); END`);
		expect(() => f.record()).toThrow("fixture audit failure");
		expect(f.raw.query("SELECT * FROM work_tasks").all()).toEqual(rows);
		expect(f.database.deliveryRows()).toEqual(deliveries);
	});

	test("held control references require independent original instruction attribution", async () => {
		const f = await heldDispositionFixture(true);
		const marker = {
			...source(`activation-${TASK}`),
			kind: "observation" as const,
			body: `Assignment ${TASK}\n${f.task.request.text}\nScope: ${f.task.request.kind}; repository admission is not an OS sandbox.`,
			evidence: { ...evidence, principalId: "gateway" },
		};
		f.database.withTransaction(() => {
			f.database.workTaskSourceAppendInTransaction(marker, {
				...payload(f.task, marker.sourceId, marker.body),
				final: false,
			});
		});
		f.raw.query("UPDATE work_attempt_runtime SET record_json = '{bad' WHERE op_ref = ?").run(f.task.opRef);
		f.database.withTransaction(() => f.database.workTaskRecordLinkedUnavailableInTransaction(TASK, LATER, () => true));
		const selected = f.database.workTaskDispositionBasis(TASK);
		if (selected.kind !== "validation_unavailable") throw new Error("negative basis missing");
		if (f.params.target.kind !== "control") throw new Error("control target missing");
		expect(selected.basis.controls).toEqual([f.params.target]);
		const params = { ...f.params, validationUnavailable: selected.qualification };
		const result = f.database.withTransaction(() =>
			f.database.workTaskNegativeDispositionInTransaction(params, dispositionEvidence(params), () => true),
		);
		expect(result.record.retained.controlPhase).toBe("held");
		expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
		f.raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = 'original-instruction'").run();
		const changed = f.database.workTaskDispositionBasis(TASK);
		if (changed.kind !== "validation_unavailable") throw new Error("negative basis missing");
		expect(changed.basis.controls).toEqual([]);
		expect(changed.basis.controlsCompleteness).toBe("partial");
	});

	test("changed corruption rejects captured proof before quarantine and null intent stays null", async () => {
		const f = await negative();
		f.raw.query("UPDATE work_attempt_runtime SET record_json = '{changed' WHERE op_ref = ?").run(f.task.opRef);
		expect(f.database.workTaskDispositionBasis(TASK).kind).toBe("unavailable");
		expect(() => f.record()).toThrow();
		f.recordNegative();
		const selected = f.database.workTaskDispositionBasis(TASK);
		if (selected.kind !== "validation_unavailable") throw new Error("missing changed proof");
		const params = { ...f.params, validationUnavailable: selected.qualification };
		const result = f.record(params, false);
		expect(result.deliveryId).toBeNull();
		expect(f.record(params, true)).toEqual({ ...result, disposition: "duplicate" });
	});

	test("qualification and original tuple mismatches cannot authorize recording", async () => {
		const f = await negative();
		const before = f.raw.query("SELECT * FROM work_tasks").all();
		for (const changed of [
			{ ...f.params, jobId: "different-job" },
			{ ...f.params, expectedTaskVersion: f.params.expectedTaskVersion + 1 },
			{ ...f.params, target: { kind: "report" as const, reportId: "invented-report" } },
			{ ...f.params, validationUnavailable: { ...f.selected.qualification, scope: "code_mutating" as const } },
			{ ...f.params, validationUnavailable: { ...f.selected.qualification, firstObservedAt: START } },
		])
			expect(() => f.record(changed)).toThrow();
		expect(f.raw.query("SELECT * FROM work_tasks").all()).toEqual(before);
	});

	test("historical qualification remains readable after original runtime repair", async () => {
		const f = await negative();
		const result = f.record();
		const { runtime } = f.prepare(f.task);
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(runtime), f.task.opRef);
		expect(f.database.workTaskGet(TASK)?.version).toBe(f.task.version + 1);
		expect(f.database.workTaskSourceGet(result.sourceId)?.administrative).toEqual(result.record);
		expect(f.database.workTaskDispositionBasis(TASK).kind).toBe("original");
		expect(f.record()).toEqual({ ...result, disposition: "duplicate" });
	});

	for (const tamper of ["metadata", "assignment", "destination", "negative"] as const)
		test(`no administrative identity invented from corrupted ${tamper}`, async () => {
			const f = await negative();
			if (tamper === "metadata") f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(TASK);
			if (tamper === "assignment" || tamper === "negative")
				f.raw
					.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?")
					.run(tamper === "assignment" ? `assignment-${TASK}` : f.fact.sourceId);
			if (tamper === "destination") {
				const activation = f.database.workTaskSourceGet(`activation-${TASK}`)!;
				f.raw.query("UPDATE deliveries SET payload_json = '{}' WHERE delivery_id = ?").run(activation.deliveryId!);
			}
			expect(f.database.workTaskDispositionBasis(TASK)).toEqual({
				execution: "none",
				kind: "unavailable",
				basis: null,
			});
			expect(() => f.record()).toThrow();
		});
});

async function heldDispositionFixture(retainRoute = false, settledPredecessors = 0) {
	const f = await fixture();
	const bound = f.bind(f.create().record);
	const { runtime, record } = f.prepare(bound);
	f.database.workAttemptPrepare(runtime, record);
	f.database.workAttemptUpdate(runtime.opRef, 0, {
		sendPhase: "accepted",
		sendEvidence: { source: "receipt", observedAt: START },
	});
	for (let index = 0; index < settledPredecessors; index++) {
		const prior = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction({
				...controlRequest(`settled-${index}`),
				kind: "reset_notice",
			}),
		).record;
		expect(prior.phase).toBe("accepted");
	}
	const instruction = controlRequest("uncertain");
	const pending = f.database.withTransaction(() => {
		if (retainRoute)
			f.database.workTaskSourceAppendInTransaction({
				...source("original-instruction"),
				kind: "instruction",
				body: instruction.body,
				evidence: instruction.evidence,
			});
		return f.database.workControlAdmitInTransaction(
			instruction,
			undefined,
			retainRoute
				? {
						taskId: TASK,
						opRef: runtime.opRef,
						thread: THREAD,
						sourceId: "original-instruction",
					}
				: undefined,
		);
	}).record;
	const sending = f.database.withTransaction(() =>
		f.database.workControlTransitionInTransaction(pending.controlId, pending.version, {
			phase: "sending",
			identity,
			at: LATER,
		}),
	)!;
	const control = f.database.withTransaction(() =>
		f.database.workControlTransitionInTransaction(sending.controlId, sending.version, {
			phase: "held",
			identity,
			at: LATER,
			reason: "receipt_lost",
		}),
	)!;
	const task = f.database.workTaskGet(TASK)!;
	const params = dispositionParams(task, {
		kind: "control",
		controlId: control.controlId,
		eventId: control.request.evidence.eventId,
		clientRef: control.clientRef,
	});
	return { ...f, task, control, params };
}

async function reviewFixture() {
	const f = await fixture();
	const bound = f.bind(f.create().record);
	const { runtime, record } = f.prepare(bound);
	f.database.workAttemptPrepare(runtime, record);
	const body = "Complete original answer requiring an owner choice.";
	const report = buildDeliveryPayload(runtime.opRef, LOOPBACK_ORIGIN, body, runtime.deliveryId)!;
	f.database.workAttemptSettle(
		runtime.opRef,
		0,
		closeAttempt({ record, opRef: runtime.opRef, endState: "completed", endedAt: LATER }),
		{
			terminal: {
				kind: "broker",
				observedAt: LATER,
				reasonCode: "end_turn",
				status: {
					status: "terminal_ok",
					receiptState: "present",
					outcome: { reason: "end_turn" },
					terminalAt: Date.parse(LATER),
				},
			},
			output: {
				...runtime.output,
				disposition: "available",
				excerpt: body,
				proof: {
					opRef: runtime.opRef,
					sessionId: runtime.sessionId,
					epoch: runtime.epoch,
					observedAtMs: Date.parse(LATER),
					source: "turn.result",
					attribution: "operation_ref",
					fullness: "original",
					clientRef: runtime.opRef,
					repo: runtime.cwd,
					terminalAt: Date.parse(LATER),
					contentVersion: 1,
					byteLength: Buffer.byteLength(body),
				},
			},
			decision: "report",
			settledAt: LATER,
		},
		{
			kind: "persona",
			row: {
				messageId: runtime.reportId,
				originKey: originKey(LOOPBACK_ORIGIN),
				originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
				body,
				receivedAt: LATER,
			},
			fallbackPayload: report,
		},
	);
	const task = f.database.workTaskGet(TASK)!;
	f.database.withTransaction(() => {
		f.database.workTaskSourceAppendInTransaction({
			...source(`final-${TASK}-original`),
			kind: "observation",
			body,
			evidence: { ...evidence, eventId: runtime.opRef },
			reportId: runtime.reportId,
		});
		f.database.workTaskObligationInTransaction(TASK, task.version, {
			identity,
			state: "final_admitted",
			reason: null,
			at: LATER,
		});
	});
	const original = f.database.workTaskOriginalSource(TASK)!;
	const review: WorkTaskReviewParams = {
		taskId: TASK,
		expectedOpRef: runtime.opRef,
		reportId: runtime.reportId,
		sourceId: original.sourceId,
		contentHash: original.contentHash,
		reviewId: "ed2f2494-2584-4d13-b7b6-c6ac24a1087f",
		expectedReviewId: null,
		callerSessionId: "persona-reviewer",
		callerEpoch: 0,
		fullRead: true,
		disposition: "owner_question",
		rationale: "The retained answer leaves an owner choice.",
		question: "Choose red or blue?",
	};
	return { ...f, review };
}

describe("dedicated review append boundary", () => {
	test("generic append rejects review metadata and reserved identities without admitting review state", async () => {
		const f = await reviewFixture();
		const before = f.database.workTaskReviewLocator(TASK);
		const deliveries = f.database.deliveryRows();
		// No live reviewer is registered: the generic path must not admit this otherwise well-shaped review.
		const forged: WorkTaskSourceInput = {
			...source(`review-${f.review.reviewId}`),
			reportId: f.review.reportId,
			evidence: {
				...evidence,
				principalId: `coordinator:${f.review.callerSessionId}`,
				origin: LOOPBACK_ORIGIN,
				eventId: f.review.reviewId,
			},
			review: f.review,
		};
		const { review: _review, ...reserved } = forged;
		for (const input of [forged, reserved, { ...forged, sourceId: "ordinary-review-metadata" }]) {
			expect(() => f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(input))).toThrow(
				WorkTaskStateError,
			);
			expect(f.database.workTaskSourceGet(input.sourceId)).toBeUndefined();
			expect(f.database.workTaskReviewLocator(TASK)).toEqual(before);
			expect(f.database.deliveryRows()).toEqual(deliveries);
		}
		expect(before?.pending).toBe(true);
	});

	test("dedicated owner-question review rolls back atomically then succeeds once across retry", async () => {
		const f = await reviewFixture();
		f.database.recordOwnedBinding({
			authority: f.authority,
			sessionId: f.review.callerSessionId,
			originKey: originKey(LOOPBACK_ORIGIN),
			epoch: 0,
			repo: request.cwd,
		});
		f.database.updateActivity(originKey(LOOPBACK_ORIGIN), JSON.stringify(LOOPBACK_ORIGIN));
		const task = f.database.workTaskGet(TASK);
		const runtime = f.database.workAttemptGet(task!.opRef);
		const before = f.database.workTaskReviewLocator(TASK);
		const deliveries = f.database.deliveryRows();
		expect(() => f.database.workTaskReview({ ...f.review, callerEpoch: 1 }, LATER)).toThrow("identity");
		const write = spyOn(f.database, "deliveryCreateInTransaction").mockImplementation(() => {
			throw new Error("review delivery fault");
		});
		try {
			expect(() => f.database.workTaskReview(f.review, LATER)).toThrow("review delivery fault");
			expect(f.database.workTaskSourceGet(`review-${f.review.reviewId}`)).toBeUndefined();
			expect(f.database.workTaskReviewLocator(TASK)).toEqual(before);
			expect(f.database.deliveryRows()).toEqual(deliveries);
		} finally {
			write.mockRestore();
		}
		const admitted = f.database.workTaskReview(f.review, LATER);
		expect(admitted.disposition).toBe("recorded");
		expect(f.database.workTaskReview(f.review, LATER)).toEqual({ ...admitted, disposition: "duplicate" });
		expect(() => f.database.workTaskReview({ ...f.review, question: "Changed?" }, LATER)).toThrow("conflict");
		expect(() => f.database.workTaskReview({ ...f.review, reviewId: OTHER }, LATER)).toThrow("order");
		expect(f.database.deliveryRows()).toHaveLength(deliveries.length + 1);
		const delivery = f.database.deliveryGet(admitted.deliveryId!)!;
		expect(delivery.origin_key).toBe(originKey(LOOPBACK_ORIGIN));
		expect(JSON.parse(delivery.payload_json).text).toContain(f.review.question);
		expect(f.database.workTaskSourceGet(admitted.sourceId)?.review).toEqual(f.review);
		expect(f.database.workTaskReviewLocator(TASK)).toMatchObject({
			pending: false,
			ownerQuestions: [{ reviewId: f.review.reviewId, question: f.review.question, deliveryId: admitted.deliveryId }],
		});
		expect(f.database.workTaskGet(TASK)).toEqual(task);
		expect(f.database.workAttemptGet(task!.opRef)).toEqual(runtime);
	});
});

describe("owner administrative hold disposition transaction", () => {
	for (const delta of [{ sessionId: "wrong-session" }, { epoch: 1 }])
		test(`healthy direct held discovery rejects mismatched ${"sessionId" in delta ? "session" : "epoch"}`, async () => {
			const f = await heldDispositionFixture();
			const changed = { ...f.control, ...delta };
			f.raw
				.query("UPDATE work_controls SET record_json = ? WHERE control_id = ?")
				.run(JSON.stringify(changed), f.control.controlId);
			expect(f.database.workControlGet(f.control.controlId)).toEqual(changed);
			const basis = f.database.workTaskDispositionBasis(TASK);
			expect(basis.kind).toBe("original");
			expect(basis.basis?.controls).toEqual([]);
			expect(basis.basis?.controlsCompleteness).toBe("partial");
		});

	for (const [corrupt, retainRoute] of [
		[false, false],
		[false, true],
		[true, true],
	] as const)
		test(`held targets after settled predecessors remain discoverable (${corrupt ? "corrupt" : "healthy"} runtime, ${retainRoute ? "retained route" : "direct"})`, async () => {
			const f = await heldDispositionFixture(retainRoute, 21);
			const successor = f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction(controlRequest("successor")),
			).record;
			if (corrupt) {
				const marker = {
					...source(`activation-${TASK}`),
					kind: "observation" as const,
					body: `Assignment ${TASK}\n${f.task.request.text}\nScope: ${f.task.request.kind}; repository admission is not an OS sandbox.`,
					evidence: { ...evidence, principalId: "gateway" },
				};
				f.database.withTransaction(() =>
					f.database.workTaskSourceAppendInTransaction(marker, {
						...payload(f.task, marker.sourceId, marker.body),
						final: false,
					}),
				);
				f.raw.query("UPDATE work_attempt_runtime SET record_json = '{bad' WHERE op_ref = ?").run(f.task.opRef);
				f.database.withTransaction(() =>
					f.database.workTaskRecordLinkedUnavailableInTransaction(TASK, LATER, () => true),
				);
			}
			const beforeRuntime = f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef);
			const basis = f.database.workTaskDispositionBasis(TASK);
			expect(basis.kind).toBe(corrupt ? "validation_unavailable" : "original");
			if (f.params.target.kind !== "control") throw new Error("held fixture requires control target");
			expect(basis.basis?.controls).toEqual([f.params.target]);
			expect(basis.basis?.controlsCompleteness).toBe("complete");
			if (basis.kind === "validation_unavailable") {
				const params = { ...f.params, validationUnavailable: basis.qualification };
				f.database.withTransaction(() =>
					f.database.workTaskNegativeDispositionInTransaction(params, dispositionEvidence(params), () => true),
				);
			} else {
				const id = workTaskDispositionId(TASK, f.params.eventId, LOOPBACK_ORIGIN);
				f.database.withTransaction(() =>
					f.database.workTaskDispositionInTransaction(
						f.params,
						dispositionEvidence(f.params),
						payload(f.task, id, workTaskDispositionText(f.params)),
					),
				);
			}
			expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
			expect(f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef)).toEqual(
				beforeRuntime,
			);
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workControlTransitionInTransaction(successor.controlId, successor.version, {
						phase: "sending",
						identity,
						at: LATER,
					}),
				),
			).toThrow(corrupt ? WorkAttemptStateError : WorkTaskStateError);
			expect(f.database.workControlGet(successor.controlId)?.phase).toBe("pending");
		});

	for (const retainRoute of [false, true])
		test(`corrupt runtime rejects held controls without independent ${retainRoute ? "instruction" : "route"} proof`, async () => {
			const f = await heldDispositionFixture(retainRoute, 21);
			const marker = {
				...source(`activation-${TASK}`),
				kind: "observation" as const,
				body: `Assignment ${TASK}\n${f.task.request.text}\nScope: ${f.task.request.kind}; repository admission is not an OS sandbox.`,
				evidence: { ...evidence, principalId: "gateway" },
			};
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction(marker, {
					...payload(f.task, marker.sourceId, marker.body),
					final: false,
				}),
			);
			if (retainRoute)
				f.raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?").run("original-instruction");
			f.raw.query("UPDATE work_attempt_runtime SET record_json = '{bad' WHERE op_ref = ?").run(f.task.opRef);
			f.database.withTransaction(() =>
				f.database.workTaskRecordLinkedUnavailableInTransaction(TASK, LATER, () => true),
			);
			const before = f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef);
			const basis = f.database.workTaskDispositionBasis(TASK);
			expect(basis.kind).toBe("validation_unavailable");
			expect(basis.basis?.controls).toEqual([]);
			expect(basis.basis?.controlsCompleteness).toBe("partial");
			if (basis.kind !== "validation_unavailable") throw new Error("expected qualified negative basis");
			const params = { ...f.params, validationUnavailable: basis.qualification };
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskNegativeDispositionInTransaction(params, dispositionEvidence(params), () => true),
				),
			).toThrow(WorkTaskStateError);
			expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
			expect(f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef)).toEqual(before);
		});

	test("local trusted attribution, immutable audit and mapped intent retain uncertain steer ordering", async () => {
		const f = await heldDispositionFixture();
		const beforeRuntime = f.database.workAttemptGet(f.task.opRef);
		const sourceId = workTaskDispositionId(TASK, f.params.eventId, LOOPBACK_ORIGIN);
		const delivery = payload(f.task, sourceId, workTaskDispositionText(f.params));
		const result = f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params), delivery),
		);
		expect(result).toMatchObject({
			execution: "none",
			disposition: "recorded",
			sourceId,
			dispositionId: sourceId,
			deliveryId: delivery.deliveryId,
			record: {
				principalId: "local-ipc:owner",
				origin: LOOPBACK_ORIGIN,
				retained: { obligationState: "awaiting_final", controlPhase: "held", reportId: null },
			},
		});
		expect(f.database.workTaskGet(TASK)).toEqual({ ...f.task, version: f.task.version + 1, updatedAt: LATER });
		expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
		expect(f.database.workAttemptGet(f.task.opRef)).toEqual(beforeRuntime);
		expect(beforeRuntime?.terminal).toBeNull();
		expect(beforeRuntime?.settledAt).toBeNull();
		expect(f.database.workTaskSourceGet(sourceId)?.administrative).toEqual(result.record);
		expect(f.database.deliveryRows()).toHaveLength(1);
		const next = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("successor")),
		).record;
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(next.controlId, next.version, {
					phase: "sending",
					identity,
					at: LATER,
				}),
			),
		).toThrow("work task: order");
		const nextParams = {
			...f.params,
			expectedTaskVersion: result.record.taskVersion,
			eventId: "local:administrative:2",
		};
		f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(nextParams, dispositionEvidence(nextParams)),
		);
		const duplicate = f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(
				f.params,
				{ ...dispositionEvidence(f.params), observedAt: "2026-10-06T00:02:00.000Z" },
				delivery,
			),
		);
		expect(duplicate).toEqual({ ...result, disposition: "duplicate" });
		expect(f.database.deliveryRows()).toHaveLength(1);
		for (const changed of [
			{ ...f.params, reason: "Changed" },
			{ ...f.params, outcome: "unresolved" as const },
			{ ...f.params, expectedTaskVersion: result.record.taskVersion },
		]) {
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskDispositionInTransaction(changed, dispositionEvidence(changed)),
				),
			).toThrow("work task: conflict");
		}
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, {
					...dispositionEvidence(f.params),
					principalId: "other-owner",
				}),
			),
		).toThrow("work task: conflict");
		// Later original receipt reconciliation does not invalidate the old administrative event.
		f.database.withTransaction(() =>
			f.database.workControlTransitionInTransaction(f.control.controlId, f.control.version, {
				phase: "accepted",
				identity,
				at: LATER,
				receipt: receipt(f.control),
			}),
		);
		expect(
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
			),
		).toEqual({ ...result, disposition: "duplicate" });
	});

	test("rejects stale CAS, wrong original fences and synthetic authority with no audit or delivery", async () => {
		const f = await heldDispositionFixture();
		const target = f.params.target as Extract<WorkTaskDispositionParams["target"], { kind: "control" }>;
		for (const params of [
			{ ...f.params, taskId: OTHER },
			{ ...f.params, jobId: "wrong" },
			{ ...f.params, expectedOpRef: "gw-wrong" },
			{ ...f.params, sessionId: "wrong" },
			{ ...f.params, epoch: 1 },
			{ ...f.params, cwd: "/different" },
			{ ...f.params, requestHash: "a".repeat(64) },
			{ ...f.params, expectedTaskVersion: f.task.version - 1 },
			{ ...f.params, target: { ...target, controlId: "wrong" } },
			{ ...f.params, target: { ...target, eventId: "wrong" } },
			{ ...f.params, target: { ...target, clientRef: "wrong" } },
			{ ...f.params, target: { kind: "report" as const, reportId: null } },
			{ ...f.params, principalId: "forged-owner" },
			{ ...f.params, authenticated: true },
		])
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskDispositionInTransaction(params, dispositionEvidence(params)),
				),
			).toThrow();
		for (const proof of [
			undefined,
			{ ...dispositionEvidence(f.params), eventId: "wrong" },
			{ ...dispositionEvidence(f.params), principalId: "" },
			{ ...dispositionEvidence(f.params), editId: "edit" },
		])
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskDispositionInTransaction(f.params, proof as WorkTaskEvidence),
				),
			).toThrow();
		expect(f.database.workTaskGet(TASK)).toEqual(f.task);
		expect(f.database.workTaskSources(TASK)?.sources).toHaveLength(1);
		expect(f.database.deliveryRows()).toHaveLength(0);
	});

	test("source, CAS and optional delivery roll back together; generic sources cannot bypass the fence", async () => {
		const f = await heldDispositionFixture();
		const sourceId = workTaskDispositionId(TASK, f.params.eventId, LOOPBACK_ORIGIN);
		const delivery = payload(f.task, sourceId, workTaskDispositionText(f.params));
		expect(() => f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params))).toThrow(
			"requires a database transaction",
		);
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params), delivery);
				throw new Error("rollback disposition");
			}),
		).toThrow("rollback disposition");
		expect(f.database.workTaskSourceGet(sourceId)).toBeUndefined();
		expect(f.database.workTaskGet(TASK)).toEqual(f.task);
		expect(f.database.deliveryRows()).toHaveLength(0);
		f.raw.exec(
			`CREATE TRIGGER refuse_admin BEFORE UPDATE ON work_tasks BEGIN SELECT RAISE(ABORT, 'CAS write failed'); END`,
		);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params), delivery),
			),
		).toThrow("CAS write failed");
		f.raw.exec("DROP TRIGGER refuse_admin");
		expect(f.database.workTaskSourceGet(sourceId)).toBeUndefined();
		expect(f.database.deliveryRows()).toHaveLength(0);
		expect(f.database.workTaskGet(TASK)).toEqual(f.task);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params), {
					...delivery,
					text: "Claim invented success",
				}),
			),
		).toThrow();
		const result = f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
		);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction({
					...source("bypass"),
					administrative: result.record,
				}),
			),
		).toThrow();
		expect(() =>
			f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(source(sourceId))),
		).toThrow();
		f.raw.query("UPDATE work_task_sources SET record_json = ? WHERE source_id = ?").run(
			JSON.stringify({
				...f.database.workTaskSourceGet(sourceId),
				administrative: { ...result.record, taskVersion: -1 },
			}),
			sourceId,
		);
		expect(() => f.database.workTaskSourceGet(sourceId)).toThrow();
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
			),
		).toThrow();
	});

	test("held report records exact original identity without terminal admission or resource proof", async () => {
		const f = await heldDispositionFixture();
		const held = f.database.withTransaction(() =>
			f.database.workTaskObligationInTransaction(TASK, f.task.version, {
				identity,
				state: "held",
				reason: "original_output_unavailable",
				at: LATER,
			}),
		)!;
		const params = dispositionParams(held, { kind: "report", reportId: held.terminalReportId });
		const before = f.database.workAttemptGet(held.opRef);
		for (const reportId of [null, "wrong-report"])
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskDispositionInTransaction(
						{ ...params, target: { kind: "report", reportId } },
						dispositionEvidence(params),
					),
				),
			).toThrow("work task: identity");
		const result = f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(params, dispositionEvidence(params)),
		);
		expect(result.record.retained).toEqual({
			obligationState: "held",
			reportId: held.terminalReportId,
			holdReason: held.holdReason,
			controlPhase: null,
		});
		expect(f.database.workTaskGet(TASK)).toEqual({ ...held, version: held.version + 1, updatedAt: LATER });
		expect(f.database.workAttemptGet(held.opRef)).toEqual(before);
		expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
		expect(before?.terminal).toBeNull();
		expect(before?.output.proof).toBeNull();
		expect(before?.decision).toBe("undecided");
	});

	test("exact terminal proof stays independent of administrative disposition and original report debt", async () => {
		const f = await heldDispositionFixture();
		const runtime = f.database.workAttemptGet(f.task.opRef)!;
		f.database.workAttemptUpdate(f.task.opRef, runtime.version, {
			terminal: {
				kind: "broker",
				observedAt: LATER,
				reasonCode: "end_turn",
				status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
			},
		});
		const held = f.database.withTransaction(() =>
			f.database.workTaskObligationInTransaction(TASK, f.task.version, {
				identity,
				state: "held",
				reason: "final_missing",
				at: LATER,
			}),
		)!;
		const params = dispositionParams(held, { kind: "report", reportId: held.terminalReportId });
		const original = f.database.workAttemptGet(held.opRef);
		f.database.withTransaction(() => f.database.workTaskDispositionInTransaction(params, dispositionEvidence(params)));
		expect(f.database.workAttemptGet(held.opRef)).toEqual(original);
		expect(f.database.workTaskGet(TASK)?.reportAdmittedAt).toBeNull();
		expect(f.database.workTaskGet(TASK)?.obligationState).toBe("held");
		expect(f.database.workControlGet(f.control.controlId)).toEqual(f.control);
	});

	test("authentic stored unknown report stays null, while corrupt original records fail closed", async () => {
		const f = await fixture();
		const created = f.create().record;
		// Persisted pre-attempt hold fixture: there never was a remote report/session.
		const held: WorkTask = { ...created, obligationState: "held", holdReason: "original_identity_unavailable" };
		f.raw
			.query("UPDATE work_tasks SET obligation_state = 'held', record_json = ? WHERE task_id = ?")
			.run(JSON.stringify(held), TASK);
		const params = dispositionParams(held, { kind: "report", reportId: null });
		const result = f.database.withTransaction(() =>
			f.database.workTaskDispositionInTransaction(params, dispositionEvidence(params)),
		);
		expect(result.record.request).toMatchObject({
			sessionId: null,
			epoch: null,
			target: { kind: "report", reportId: null },
		});
		expect(result.record.retained.reportId).toBeNull();
		expect(f.database.workAttemptGet(held.opRef)).toBeUndefined();
		expect(f.database.deliveryRows()).toHaveLength(0);
		f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(TASK);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(params, dispositionEvidence(params)),
			),
		).toThrow();
	});

	test("nonheld controls and corrupt original control rows cannot acquire an administrative decision", async () => {
		const f = await heldDispositionFixture();
		f.database.withTransaction(() =>
			f.database.workControlTransitionInTransaction(f.control.controlId, f.control.version, {
				phase: "accepted",
				identity,
				at: LATER,
				receipt: receipt(f.control),
			}),
		);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
			),
		).toThrow("work task: transition");
		f.raw.query("UPDATE work_controls SET record_json = '{}' WHERE control_id = ?").run(f.control.controlId);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskDispositionInTransaction(f.params, dispositionEvidence(f.params)),
			),
		).toThrow();
		expect(f.database.workTaskSources(TASK)?.sources).toHaveLength(1);
		expect(f.database.workTaskGet(TASK)).toEqual(f.task);
	});
});

describe("Firstmate durable task and control store", () => {
	test("Discord locator uses the permanent SQL index, not task JSON or claimed metadata", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		const readTask = spyOn(f.database, "workTaskGet");
		try {
			expect(f.database.workTaskDiscordLocator(THREAD.conversationId)).toEqual({ taskId: TASK });
			expect(f.database.workTaskDiscordLocator(`${THREAD.conversationId}0`)).toBeUndefined();
			expect(f.database.workTaskDiscordLocator(THREAD.conversationId.slice(0, -1))).toBeUndefined();
			expect(f.database.workTaskDiscordLocator("999")).toBeUndefined();
			for (const id of ["d-owner", "c1", "thread", "01", "0", "-1", "18446744073709551616"])
				expect(f.database.workTaskDiscordLocator(id)).toBeUndefined();
			expect(readTask).not.toHaveBeenCalled();
			const plan = f.raw
				.query<{ detail: string }, [string, string]>(
					"EXPLAIN QUERY PLAN SELECT task_id FROM work_tasks WHERE thread_origin_key >= ? AND thread_origin_key < ? LIMIT 2",
				)
				.all(`discord/thread/${THREAD.conversationId}/`, `discord/thread/${THREAD.conversationId}0`);
			expect(
				plan.some(
					(row) => /SEARCH work_tasks USING INDEX/.test(row.detail) && row.detail.includes("thread_origin_key"),
				),
			).toBe(true);
		} finally {
			readTask.mockRestore();
		}
	});

	for (const conversationId of [THREAD.conversationId, "retained-thread"])
		test(`locator discovers corrupt permanent mapping for ${conversationId} without validating or bypassing it`, async () => {
			const f = await fixture();
			f.bind(f.create().record);
			f.raw.exec("PRAGMA ignore_check_constraints = ON");
			try {
				f.raw
					.query("UPDATE work_tasks SET thread_origin_key = ?, record_json = '{broken' WHERE task_id = ?")
					.run(originKey({ ...THREAD, conversationId }), TASK);
			} finally {
				f.raw.exec("PRAGMA ignore_check_constraints = OFF");
			}
			expect(f.database.workTaskDiscordLocator(conversationId)).toEqual({ taskId: TASK });
			expect(() => f.database.workTaskGet(TASK)).toThrow();
			expect(f.database.workTaskDiscordLocator(`${conversationId}0`)).toBeUndefined();

			const other = f.create(OTHER, "gw-other").record;
			f.raw.exec("PRAGMA ignore_check_constraints = ON");
			try {
				f.raw
					.query("UPDATE work_tasks SET thread_origin_key = ? WHERE task_id = ?")
					.run(originKey({ ...THREAD, conversationId, parentId: "999" }), other.taskId);
			} finally {
				f.raw.exec("PRAGMA ignore_check_constraints = OFF");
			}
			expect(() => f.database.workTaskDiscordLocator(conversationId)).toThrow(WorkTaskStateError);
		});

	test("permanent conversation ownership rejects changed-parent binding and ambiguous stored identities", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		const other = f.create(OTHER, "gw-other", {
			...request,
			surface: { thread: { ...THREAD, parentId: "999" } },
		}).record;
		expect(() => f.bind(other, { ...THREAD, parentId: "999" })).toThrow(WorkTaskStateError);
		expect(f.database.workTaskGet(OTHER)).toEqual(other);
		const separateThread = { ...THREAD, conversationId: "999" };
		const separate = f.create(crypto.randomUUID(), "gw-separate", {
			...request,
			surface: { thread: separateThread },
		}).record;
		f.bind(separate, separateThread);
		// Simulate conflicting durable identity evidence, not a second registry.
		f.raw
			.query("UPDATE work_tasks SET thread_origin_key = ? WHERE task_id = ?")
			.run(originKey({ ...THREAD, parentId: "999" }), separate.taskId);
		expect(() => f.database.workTaskDiscordLocator(THREAD.conversationId)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskDiscordLocator("999")).toBeUndefined();
	});

	test("locator follows binding transaction rollback and persists across database reopen", async () => {
		const f = await fixture();
		const task = f.create().record;
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workTaskSurfaceInTransaction(task.taskId, task.version, {
					phase: "bound",
					thread: THREAD,
					claimId: null,
					at: START,
				});
				expect(f.database.workTaskDiscordLocator(THREAD.conversationId)).toEqual({ taskId: TASK });
				throw new Error("rollback binding");
			}),
		).toThrow("rollback binding");
		expect(f.database.workTaskDiscordLocator(THREAD.conversationId)).toBeUndefined();
		expect(f.database.workTaskGet(TASK)).toEqual(task);
		f.bind(task);
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workTaskDiscordLocator(THREAD.conversationId)).toEqual({ taskId: TASK });
	});

	test("corrupt and terminal tombstone records retain routing ownership without confirming facts", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		for (const record of [{ ...task, thread: { ...THREAD, conversationId: "999" } }, { taskId: false }]) {
			f.raw.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?").run(JSON.stringify(record), TASK);
			expect(() => f.database.workTaskGet(TASK)).toThrow(WorkTaskStateError);
			expect(f.database.workTaskDiscordLocator(THREAD.conversationId)).toEqual({ taskId: TASK });
			expect(f.database.workTaskDiscordLocator("999")).toBeUndefined();
		}
		f.raw.query("UPDATE work_tasks SET obligation_state = 'final_admitted' WHERE task_id = ?").run(TASK);
		const other = f.create(OTHER, "gw-other").record;
		expect(() => f.bind(other, { ...THREAD, parentId: "999" })).toThrow(WorkTaskStateError);
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workTaskDiscordLocator(THREAD.conversationId)).toEqual({ taskId: TASK });
		expect(() => reopened.workTaskGet(TASK)).toThrow(WorkTaskStateError);
		expect(reopened.workTaskGet(OTHER)).toEqual(other);
	});

	test("recent source selection is bounded, newest-first, non-consuming and never a history cursor", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		f.database.withTransaction(() => {
			for (let index = 0; index < 55; index++)
				f.database.workTaskSourceAppendInTransaction(source(`decision-${index}`));
		});
		const recent = f.database.workTaskSources(TASK, { recent: true, limit: 3 })!;
		expect(recent.sources.map((item) => item.sourceId)).toEqual(["decision-54", "decision-53", "decision-52"]);
		expect(recent.omitted).toBe(53);
		expect(f.database.workTaskSources(TASK, { recent: true, limit: 3 })).toEqual(recent);
		expect(f.database.workTaskSources(TASK, { limit: 3 })?.sources.map((item) => item.sequence)).toEqual([1, 2, 3]);
		expect(f.database.workTaskSources(TASK, { afterSequence: 53 })?.sources.map((item) => item.sequence)).toEqual([
			54, 55, 56,
		]);
		expect(
			f.database.withTransaction(() => f.database.workTaskSourcesInTransaction(TASK, { recent: true, limit: 3 })),
		).toEqual(recent);
		expect(() => f.database.workTaskSources(TASK, { recent: true, afterSequence: 0 })).toThrow(WorkTaskStateError);
		expect(() => f.database.workTaskSources(TASK, { recent: true, limit: 51 })).toThrow(WorkTaskStateError);
		expect(f.database.workTaskSources(TASK, { recent: true, limit: 2, manifest: recent.manifest })?.newSnapshot).toBe(
			true,
		);
		f.database.withTransaction(() =>
			f.database.workTaskSourceAppendInTransaction({
				...source("oversized-latest"),
				body: "x".repeat(16 * 1024),
			}),
		);
		const bounded = f.database.workTaskSources(TASK, { recent: true, limit: 3, manifest: recent.manifest })!;
		expect(bounded.newSnapshot).toBe(true);
		expect(bounded.overflow).toBe(true);
		expect(bounded.sources.map((item) => item.sourceId)).toEqual(["decision-54", "decision-53"]);
		expect(bounded.omitted).toBe(55);
		expect(Buffer.byteLength(JSON.stringify(bounded), "utf8")).toBeLessThanOrEqual(16 * 1024);
		f.raw.query("UPDATE work_task_sources SET record_json = '{}' WHERE source_id = ?").run("oversized-latest");
		expect(() => f.database.workTaskSources(TASK, { recent: true })).toThrow();
	});

	test("mapped source delivery rejects missing and mismatched finalized metadata atomically", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const input = source("strict-mapped-payload");
		const delivery = payload(task, input.sourceId);
		for (const workTask of [
			undefined,
			{ ...delivery.workTask!, taskId: OTHER },
			{ ...delivery.workTask!, sourceId: "different-source" },
		]) {
			expect(() =>
				f.database.withTransaction(() =>
					f.database.workTaskSourceAppendInTransaction(input, { ...delivery, workTask }),
				),
			).toThrow(WorkTaskStateError);
			expect(f.database.workTaskSourceGet(input.sourceId)).toBeUndefined();
			expect(f.database.deliveryRows()).toHaveLength(0);
		}
		f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(input, delivery));
		expect(f.database.deliveryRows()).toHaveLength(1);
	});

	test("snapshot prioritizes the twenty-first held task without consuming or hiding corrupt keys", async () => {
		const f = await fixture();
		const ids = Array.from({ length: 21 }, (_, index) => `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`);
		for (const [index, id] of ids.entries()) f.create(id, `gw-snapshot-${index}`);
		const last = f.database.workTaskGet(ids[20]!)!;
		f.database.withTransaction(() =>
			f.database.workTaskSurfaceInTransaction(last.taskId, last.version, {
				phase: "held",
				reason: "surface_receipt_missing",
				at: LATER,
			}),
		);
		const snapshot = f.database.workTaskSnapshotKeys();
		expect(snapshot.keys).toHaveLength(20);
		expect(snapshot.keys[0]?.taskId).toBe(ids[20]);
		expect(snapshot.total).toBe(21);
		expect(snapshot.omitted).toBe(1);
		expect(f.database.workTaskSnapshotKeys()).toEqual(snapshot);
		expect(f.database.workTaskKeys().keys.map((key) => key.taskId)).toEqual(ids.slice(0, 20));
		expect(f.database.workTaskKeys(20, ids[19]!).keys.map((key) => key.taskId)).toEqual([ids[20]!]);
		f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(ids[0]!);
		const keys = f.database.workTaskSnapshotKeys().keys;
		expect(keys.some((key) => key.taskId === ids[0])).toBe(true);
		expect(() => f.database.workTaskGet(ids[0]!)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskGet(keys[0]!.taskId)?.surfacePhase).toBe("held");
		f.database.withTransaction(() =>
			f.database.workTaskSourceAppendInTransaction({
				...source("selection-revision"),
				taskId: last.taskId,
			}),
		);
		expect(f.database.workTaskSnapshotKeys().manifest).not.toBe(snapshot.manifest);
		expect(() => f.database.workTaskSnapshotKeys(21)).toThrow(WorkTaskStateError);
	});

	test("explicit target proof preserves cross-origin attribution and independent per-target outcomes", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		const otherThread = { ...THREAD, conversationId: "1556589606403842129" };
		f.bind(f.create(OTHER, "gw-firstmate-other", { ...request, surface: { thread: otherThread } }).record, otherThread);
		const instruction = {
			...controlRequest("cli:shared-event"),
			evidence: {
				...controlRequest("cli:shared-event").evidence,
				origin: LOOPBACK_ORIGIN,
			},
		};
		expect(() => f.database.withTransaction(() => f.database.workControlAdmitInTransaction(instruction))).toThrow(
			WorkTaskStateError,
		);
		const admit = (value: WorkControlRequest, thread: OriginRef) =>
			f.database.withTransaction(() => {
				const sourceId = `named-target-${value.taskId}`;
				f.database.workTaskSourceAppendInTransaction({
					...source(sourceId),
					taskId: value.taskId,
					kind: "instruction",
					body: value.body,
					evidence: value.evidence,
				});
				return f.database.workControlAdmitInTransaction(value, undefined, {
					taskId: value.taskId,
					opRef: value.expectedOpRef,
					thread,
					sourceId,
				});
			});
		const first = admit(instruction, THREAD).record;
		const second = admit({ ...instruction, taskId: OTHER, expectedOpRef: "gw-firstmate-other" }, otherThread).record;
		expect(first.phase).toBe("pending");
		expect(second.phase).toBe("pending");
		expect(second.controlId).not.toBe(first.controlId);
		expect(second.clientRef).not.toBe(first.clientRef);
		for (const control of [first, second]) {
			expect(control.request.evidence).toEqual(instruction.evidence);
			const decision = f.database.workTaskSourceGet(`route-${control.controlId}`)!;
			expect(decision.evidence.origin).toEqual(LOOPBACK_ORIGIN);
			expect(JSON.parse(decision.body).routeAdmission.taskId).toBe(control.request.taskId);
		}
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction({
					...instruction,
					evidence: { ...instruction.evidence, observedAt: "2026-10-06T00:03:00.000Z" },
				}),
			),
		).toEqual({ disposition: "duplicate", record: first });
	});

	test("explicit routing requires matching immutable source and rolls back with its decision", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		const instruction = {
			...controlRequest("cli:target"),
			evidence: {
				...controlRequest("cli:target").evidence,
				origin: LOOPBACK_ORIGIN,
			},
		};
		const proof: WorkControlRouteAdmission = {
			taskId: TASK,
			opRef: instruction.expectedOpRef,
			thread: THREAD,
			sourceId: "explicit-owner-target",
		};
		const original = {
			...source(proof.sourceId),
			kind: "instruction" as const,
			body: instruction.body,
			evidence: instruction.evidence,
		};
		for (const delta of [
			{ taskId: OTHER },
			{ opRef: "foreign-operation" },
			{ sourceId: "missing" },
			{ thread: { ...THREAD, conversationId: "1556589606403842129" } },
		]) {
			expect(() =>
				f.database.withTransaction(() => {
					f.database.workTaskSourceAppendInTransaction(original);
					f.database.workControlAdmitInTransaction(instruction, undefined, { ...proof, ...delta });
				}),
			).toThrow(WorkTaskStateError);
			expect(f.database.workTaskSourceGet(proof.sourceId)).toBeUndefined();
		}
		for (const delta of [
			{ kind: "decision" as const },
			{ completeness: "incomplete" as const },
			{ body: "different instruction" },
			{ evidence: { ...instruction.evidence, principalId: "foreign-owner" } },
			{ evidence: { ...instruction.evidence, eventId: "foreign-event" } },
			{ evidence: { ...instruction.evidence, origin: THREAD } },
		])
			expect(() =>
				f.database.withTransaction(() => {
					f.database.workTaskSourceAppendInTransaction({ ...original, ...delta });
					f.database.workControlAdmitInTransaction(instruction, undefined, proof);
				}),
			).toThrow(WorkTaskStateError);
		f.raw.exec(`CREATE TRIGGER fail_route BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'route-control-%' BEGIN SELECT RAISE(ABORT, 'route fault'); END`);
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workTaskSourceAppendInTransaction(original);
				f.database.workControlAdmitInTransaction(instruction, undefined, proof);
			}),
		).toThrow();
		expect(f.database.workTaskSourceGet(proof.sourceId)).toBeUndefined();
		expect(f.database.workControlList(TASK)).toHaveLength(0);
	});

	test("typed scope proof admits a deferred bound original and retains one immutable grant across retry", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const instruction = { ...controlRequest("mutation"), scope: "code_mutating" as const };
		const proof: WorkControlScopeAdmission = {
			taskId: TASK,
			opRef: task.opRef,
			cwd: task.request.cwd,
			kind: "code_mutating",
			eventId: instruction.evidence.eventId,
			sourceId: "mutation-owner-source",
			validatedAt: LATER,
		};
		const admitted = f.database.withTransaction(() => {
			f.database.workTaskSourceAppendInTransaction({
				...source(proof.sourceId),
				kind: "instruction",
				body: instruction.body,
				evidence: instruction.evidence,
			});
			return f.database.workControlAdmitInTransaction(instruction, proof);
		});
		expect(admitted.record).toMatchObject({ phase: "pending", sessionId: null, sendingAt: null });
		expect(f.database.workTaskGet(TASK)?.request).toEqual(request);
		const grantId = `scope-${admitted.record.controlId}`;
		const grant = f.database.workTaskSourceGet(grantId)!;
		expect(grant).toMatchObject({
			kind: "decision",
			evidence: instruction.evidence,
			controlId: admitted.record.controlId,
		});
		expect(JSON.parse(grant.body).scopeAdmission).toEqual(proof);
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction(
					{
						...instruction,
						evidence: { ...instruction.evidence, observedAt: "2026-10-06T00:03:00.000Z" },
					},
					{ ...proof, validatedAt: "2026-10-06T00:03:00.000Z" },
				),
			),
		).toEqual({
			disposition: "duplicate",
			record: admitted.record,
		});
		expect(f.database.workTaskSourceGet(grantId)).toEqual(grant);
		expect(f.database.workTaskSources(TASK)?.sources).toHaveLength(3);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		f.database.workAttemptUpdate(task.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		expect(
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(admitted.record.controlId, 0, {
					phase: "sending",
					identity,
					at: LATER,
				}),
			)?.phase,
		).toBe("sending");
		expect(f.database.workTaskGet(TASK)?.request.kind).toBe("read_only");
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workTaskSourceGet(grantId)).toEqual(grant);
		expect(reopened.workControlGet(admitted.record.controlId)?.phase).toBe("sending");
		expect(reopened.workTaskGet(TASK)?.request.kind).toBe("read_only");
	});

	test("scope identity/source failures and decision write failure roll back admission", async () => {
		const f = await fixture();
		const task = f.create().record;
		const instruction = { ...controlRequest("mutation"), scope: "code_mutating" as const };
		const proof: WorkControlScopeAdmission = {
			taskId: TASK,
			opRef: task.opRef,
			cwd: request.cwd,
			kind: "code_mutating",
			eventId: instruction.evidence.eventId,
			sourceId: "scope-owner-source",
			validatedAt: LATER,
		};
		const ownerSource: WorkTaskSourceInput = {
			...source(proof.sourceId),
			kind: "instruction",
			body: instruction.body,
			evidence: instruction.evidence,
		};
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workTaskSourceAppendInTransaction(ownerSource);
				f.database.workControlAdmitInTransaction(instruction, proof);
			}),
		).toThrow(WorkTaskStateError);
		expect(f.database.workTaskSourceGet(ownerSource.sourceId)).toBeUndefined();
		f.bind(task);
		for (const mismatch of [
			{ taskId: OTHER },
			{ opRef: "gw-other" },
			{ cwd: "/other" },
			{ eventId: "other-event" },
			{ sourceId: "missing-source" },
			{ validatedAt: "not-a-time" },
		]) {
			expect(() =>
				f.database.withTransaction(() => {
					f.database.workTaskSourceAppendInTransaction(ownerSource);
					f.database.workControlAdmitInTransaction(instruction, { ...proof, ...mismatch });
				}),
			).toThrow(WorkTaskStateError);
			expect(f.database.workTaskSourceGet(ownerSource.sourceId)).toBeUndefined();
			expect(f.database.workControlList(TASK)).toHaveLength(0);
		}
		for (const mismatch of [
			{ kind: "observation" as const },
			{ body: "Different instruction." },
			{ evidence: { ...instruction.evidence, principalId: "wrong-owner" } },
			{ evidence: { ...instruction.evidence, eventId: "wrong-event" } },
			{ evidence: { ...instruction.evidence, origin: LOOPBACK_ORIGIN } },
			{ evidence: { ...instruction.evidence, editId: "other-edit" } },
		]) {
			expect(() =>
				f.database.withTransaction(() => {
					f.database.workTaskSourceAppendInTransaction({ ...ownerSource, ...mismatch });
					f.database.workControlAdmitInTransaction(instruction, proof);
				}),
			).toThrow(WorkTaskStateError);
			expect(f.database.workControlList(TASK)).toHaveLength(0);
		}
		f.raw.exec(`CREATE TRIGGER fail_scope BEFORE INSERT ON work_task_sources
			WHEN NEW.source_id LIKE 'scope-control-%' BEGIN SELECT RAISE(ABORT, 'scope fault'); END`);
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workTaskSourceAppendInTransaction(ownerSource);
				f.database.workControlAdmitInTransaction(instruction, proof);
			}),
		).toThrow();
		expect(f.database.workTaskSourceGet(ownerSource.sourceId)).toBeUndefined();
		expect(f.database.workControlList(TASK)).toHaveLength(0);
	});

	test("local cancellation debt does not order-block direct steering or invent remote cancellation", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		f.database.workAttemptUpdate(task.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		const cancel = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction({
				...controlRequest("local-cancel"),
				kind: "cancel_request",
			}),
		).record;
		const reset = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction({
				...controlRequest("reset"),
				kind: "reset_notice",
			}),
		).record;
		expect(reset.phase).toBe("accepted");
		const steer = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("after-cancel")),
		).record;
		expect(
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(steer.controlId, 0, {
					phase: "sending",
					identity,
					at: LATER,
				}),
			)?.phase,
		).toBe("sending");
		expect(f.database.workControlGet(cancel.controlId)).toEqual(cancel);
		expect(cancel).toMatchObject({ phase: "held", receipt: null, sendingAt: null, clientRef: null });
		expect(f.database.workTaskGet(TASK)?.obligationState).toBe("awaiting_final");
	});

	test("a held never-sent steer still preserves owner sequence", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		f.database.workAttemptUpdate(task.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		const first = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("held-unsent")),
		).record;
		const held = f.database.withTransaction(() =>
			f.database.workControlTransitionInTransaction(first.controlId, 0, {
				phase: "held",
				identity,
				at: LATER,
				reason: "scope_revalidation_unavailable",
			}),
		)!;
		const second = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("later")),
		).record;
		expect(held.sendingAt).toBeNull();
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(second.controlId, 0, {
					phase: "sending",
					identity,
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		expect(f.database.workControlGet(second.controlId)?.phase).toBe("pending");
	});

	test("retry returns the original disposition and identities across reopen; conflicting assignment is refused", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		const prepared = f.database.workTaskGet(TASK)!;
		expect(f.create(TASK, "gw-ignored-retry-ref")).toEqual({ disposition: "duplicate", record: prepared });
		expect(
			f.create(TASK, "gw-ignored-retry-ref", {
				...request,
				evidence: {
					...evidence,
					eventId: "discord:retry-transport",
					editId: "retry-edit",
					evidenceAt: LATER,
					observedAt: LATER,
				},
			}),
		).toEqual({ disposition: "duplicate", record: prepared });
		expect(f.database.workTaskGet(TASK)?.request.evidence).toEqual(evidence);
		expect(f.database.workTaskSources(TASK)?.sources.map((item) => item.sourceId)).toEqual([`assignment-${TASK}`]);
		expect(f.create(TASK, "gw-ignored-retry-ref", { ...request, text: "Different mandate." })).toEqual({
			disposition: "conflict",
			record: prepared,
		});
		expect(
			f.create(TASK, "gw-ignored-retry-ref", {
				...request,
				evidence: { ...evidence, principalId: "different-owner" },
			}),
		).toEqual({ disposition: "conflict", record: prepared });
		const instance = f.database.instanceId;
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.instanceId).toBe(instance);
		expect(reopened.workTaskGet(TASK)).toEqual(prepared);
		expect(reopened.workAttemptGet(task.opRef)).toEqual(runtime);
		expect(reopened.workTaskByThread(originKey(THREAD))?.taskId).toBe(TASK);
		expect(reopened.workTaskByLane(task.laneName)?.request).toEqual(request);
		expect(reopened.workTaskSources(TASK)?.sources[0]?.evidence).toEqual(evidence);
	});

	test("task/thread/op uniqueness and busy persona admission cannot retarget the original mapping", async () => {
		const f = await fixture();
		const first = f.create().record;
		expect(() => f.create(OTHER)).toThrow(WorkTaskStateError);
		f.database.inboundEnqueue({
			messageId: "busy-persona-input",
			originKey: originKey(THREAD),
			originRefJson: JSON.stringify(THREAD),
			body: "Persona input already admitted.",
			receivedAt: START,
		});
		expect(() => f.bind(first)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskGet(TASK)?.surfacePhase).toBe("pending");
		f.raw.exec("UPDATE inbound_messages SET state = 'done'");
		const bound = f.bind(first);
		const second = f.create(OTHER, "gw-other-task").record;
		expect(() => f.bind(second)).toThrow(WorkTaskStateError);
		expect(() => f.bind(bound, { ...THREAD, conversationId: "1556589606403842129" })).toThrow(WorkTaskStateError);
		expect(f.database.workTaskByThread(originKey(THREAD))?.taskId).toBe(TASK);
	});

	test("claimed surface identity and parent mapping survive holds without a fresh claim", async () => {
		const f = await fixture();
		const task = f.create(TASK, identity.opRef, {
			...request,
			surface: {
				parent: {
					platform: "discord",
					kind: "channel",
					conversationId: THREAD.parentId!,
					boundaryId: THREAD.boundaryId,
				},
			},
		}).record;
		const claimed = f.database.withTransaction(() =>
			f.database.workTaskSurfaceInTransaction(TASK, task.version, {
				phase: "claimed",
				claimId: "original-create-intent",
				at: START,
			}),
		)!;
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSurfaceInTransaction(TASK, claimed.version, {
					phase: "bound",
					thread: THREAD,
					claimId: "different-claim",
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSurfaceInTransaction(TASK, claimed.version, {
					phase: "bound",
					thread: { ...THREAD, parentId: "other-parent" },
					claimId: "original-create-intent",
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		const held = f.database.withTransaction(() =>
			f.database.workTaskSurfaceInTransaction(TASK, claimed.version, {
				phase: "held",
				reason: "creation_receipt_lost",
				at: LATER,
			}),
		)!;
		expect(held.surfaceClaimId).toBe("original-create-intent");
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSurfaceInTransaction(TASK, held.version, {
					phase: "claimed",
					claimId: "replacement",
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
	});

	test("prepare joins original task/history/runtime atomically and rejects missing mapping or wrong parent", async () => {
		const f = await fixture();
		const task = f.create().record;
		const { runtime, record } = f.prepare(task);
		expect(() => f.database.workAttemptPrepare(runtime, record)).toThrow(WorkTaskStateError);
		expect(f.database.laneJobJson(task.jobId)).toBeUndefined();
		expect(f.database.workAttemptGet(task.opRef)).toBeUndefined();
		f.bind(task);
		expect(() => f.database.workAttemptPrepare({ ...runtime, parent: null }, record)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskGet(TASK)?.dispatchPhase).toBe("pending");
		f.raw.exec("CREATE TRIGGER fail_task AFTER UPDATE ON work_tasks BEGIN SELECT RAISE(ABORT, 'task fault'); END");
		expect(() => f.database.workAttemptPrepare(runtime, record)).toThrow();
		expect(f.database.workAttemptGet(task.opRef)).toBeUndefined();
		expect(f.database.laneJobJson(task.jobId)).toBeUndefined();
		f.raw.exec("DROP TRIGGER fail_task");
		f.database.workAttemptPrepare(runtime, record);
		expect(f.database.workTaskGet(TASK)).toMatchObject({
			dispatchPhase: "prepared",
			sessionId: SESSION,
			epoch: 0,
			opRef: task.opRef,
			obligationState: "awaiting_final",
		});
		expect(() => f.database.workAttemptPrepare(runtime, record)).toThrow();
		expect(f.database.workAttemptGet(task.opRef)?.sendPhase).toBe("prepared");
	});

	test("source/delivery failure rolls back a caller-owned prepare transaction", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.raw.exec(
			"CREATE TRIGGER fail_source BEFORE INSERT ON work_task_sources BEGIN SELECT RAISE(ABORT, 'source fault'); END",
		);
		expect(() =>
			f.database.withTransaction(() => {
				f.database.workAttemptPrepareInTransaction(runtime, record);
				f.database.workTaskSourceAppendInTransaction(source("dispatch"), payload(task, "dispatch"));
			}),
		).toThrow();
		expect(f.database.workTaskGet(TASK)?.dispatchPhase).toBe("pending");
		expect(f.database.workAttemptGet(task.opRef)).toBeUndefined();
		expect(f.database.laneJobJson(task.jobId)).toBeUndefined();
		expect(f.database.deliveryRows()).toHaveLength(0);
		expect(() => f.database.workAttemptPrepareInTransaction(runtime, record)).toThrow(
			"requires a database transaction",
		);
	});

	test("ordered controls persist dedupe, exact receipt and uncertainty without reopening or bypass", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		const first = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("first")),
		).record;
		const second = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("second")),
		).record;
		f.database.workAttemptPrepare(runtime, record);
		f.database.workAttemptUpdate(runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		const move = (control: WorkControl, phase: "sending" | "held" | "accepted", proof?: WorkControlReceipt) =>
			f.database.withTransaction(
				() =>
					f.database.workControlTransitionInTransaction(control.controlId, control.version, {
						phase,
						identity,
						at: LATER,
						...(phase === "held" ? { reason: "receipt_lost" } : {}),
						...(proof ? { receipt: proof } : {}),
					})!,
			);
		expect(() => move(second, "sending")).toThrow(WorkTaskStateError);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(first.controlId, 0, {
					phase: "sending",
					identity: { ...identity, epoch: 1 },
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		const sending = move(first, "sending");
		const held = move(sending, "held");
		expect(() => move(held, "sending")).toThrow(WorkTaskStateError);
		expect(() => move(second, "sending")).toThrow(WorkTaskStateError);
		expect(() => move(held, "accepted", { ...receipt(held), clientRef: "wrong" })).toThrow(WorkTaskStateError);
		const accepted = move(held, "accepted", receipt(held));
		expect(accepted.sendingAt).toBe(sending.sendingAt);
		expect(accepted.clientRef).toBe(first.clientRef);
		expect(() => move(accepted, "sending")).toThrow(WorkTaskStateError);
		expect(f.database.withTransaction(() => f.database.workControlAdmitInTransaction(controlRequest("first")))).toEqual(
			{ disposition: "duplicate", record: accepted },
		);
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction({
					...controlRequest("first"),
					evidence: {
						...controlRequest("first").evidence,
						evidenceAt: "2026-10-06T00:02:00.000Z",
						observedAt: "2026-10-06T00:03:00.000Z",
					},
				}),
			),
		).toEqual({ disposition: "duplicate", record: accepted });
		expect(f.database.workControlGet(accepted.controlId)?.request.evidence).toEqual(first.request.evidence);
		expect(f.database.workTaskSources(TASK)?.sources.map((item) => item.sourceId)).toEqual([`assignment-${TASK}`]);
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction(controlRequest("first", "Changed body.")),
			),
		).toEqual({ disposition: "conflict", record: accepted });
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction({
					...controlRequest("first"),
					evidence: { ...controlRequest("first").evidence, principalId: "different-owner" },
				}),
			),
		).toEqual({ disposition: "conflict", record: accepted });
		expect(
			f.database.withTransaction(() =>
				f.database.workControlAdmitInTransaction({
					...controlRequest("first"),
					scope: "code_mutating",
				}),
			),
		).toEqual({ disposition: "conflict", record: accepted });
		const secondSending = move(second, "sending");
		const refused = f.database.withTransaction(() =>
			f.database.workControlTransitionInTransaction(secondSending.controlId, secondSending.version, {
				phase: "refused",
				identity,
				at: LATER,
				reason: "authoritative_refusal",
				receipt: receipt(secondSending, "refused"),
			}),
		)!;
		expect(refused.phase).toBe("refused");
		expect(f.database.workTaskGet(TASK)?.obligationState).toBe("awaiting_final");
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workControlList(TASK)).toEqual([accepted, refused]);
	});

	test("scope elevation, unknown edits, stale identities and cancel requests never claim steering acceptance", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		const admit = (value: WorkControlRequest) =>
			f.database.withTransaction(() => f.database.workControlAdmitInTransaction(value));
		expect(admit({ ...controlRequest("scope"), scope: "code_mutating" }).record).toMatchObject({
			phase: "refused",
			reason: "scope_elevation_refused",
			sendingAt: null,
			receipt: null,
		});
		expect(admit({ ...controlRequest("cancel"), kind: "cancel_request" }).record).toMatchObject({
			phase: "held",
			reason: "local_operator_action_required",
			clientRef: null,
		});
		const edited = { ...controlRequest("edited"), evidence: { ...evidence, eventId: "edited", editId: "digest-v2" } };
		expect(() => admit(edited)).toThrow(WorkTaskStateError);
		admit(controlRequest("edited"));
		expect(admit(edited).record.request.evidence.editId).toBe("digest-v2");
		expect(() => admit({ ...controlRequest("stale"), expectedOpRef: "gw-replacement" })).toThrow(WorkTaskStateError);
		expect(() =>
			admit({
				...controlRequest("wrong-origin"),
				evidence: { ...evidence, origin: LOOPBACK_ORIGIN },
			}),
		).toThrow(WorkTaskStateError);
		expect(() => admit(controlRequest("oversized", "é".repeat(8193)))).toThrow(WorkTaskStateError);
		expect(f.database.workControlList(TASK).some((control) => control.request.evidence.eventId === "oversized")).toBe(
			false,
		);
	});

	test("immutable source revisions retain attribution and non-consuming snapshots detect changes and omissions", async () => {
		const f = await fixture();
		f.bind(f.create().record);
		f.database.inboundEnqueue({
			messageId: "unread-context",
			originKey: originKey(LOOPBACK_ORIGIN),
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: "Not a task source archive.",
			receivedAt: START,
		});
		const initial = f.database.workTaskSources(TASK)!;
		const decision = source("decision-one");
		const first = f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(decision));
		const revision = { ...source("decision-two"), body: "A revised explicit decision.", supersedes: first.sourceId };
		f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(revision));
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction({
					...decision,
					body: "Rewrite history.",
				}),
			),
		).toThrow(WorkTaskStateError);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction({
					...decision,
					evidence: { ...decision.evidence, observedAt: "2026-10-06T00:03:00.000Z" },
				}),
			),
		).toThrow(WorkTaskStateError);
		const next = f.database.workTaskSources(TASK, { manifest: initial.manifest })!;
		expect(next.newSnapshot).toBe(true);
		expect(next.sources.map((item) => item.sequence)).toEqual([1, 2, 3]);
		expect(next.sources[1]?.evidence).toEqual(decision.evidence);
		expect(next.sources[2]?.supersedes).toBe("decision-one");
		expect(f.database.workTaskGet(TASK)?.request.text).toBe(request.text);
		expect(
			f.raw.query<{ state: string }, []>("SELECT state FROM inbound_messages WHERE message_id = 'unread-context'").get()
				?.state,
		).toBe("pending");
		f.database.withTransaction(() =>
			f.database.workTaskSourceAppendInTransaction({
				...source("large-context"),
				body: "x".repeat(16 * 1024),
			}),
		);
		const bounded = f.database.workTaskSources(TASK, { afterSequence: 3 })!;
		expect(bounded.sources).toHaveLength(0);
		expect(bounded.overflow).toBe(true);
		expect(bounded.omitted).toBe(1);
		expect(bounded.lastSequence).toBe(4);
		expect(f.database.workTaskSources(TASK, { limit: 1 })?.omitted).toBe(3);
	});

	for (const runtimeState of ["healthy", "malformed", "missing"] as const)
		test(`retention keeps original task debt and sweeps unrelated history with ${runtimeState} runtime`, async () => {
			const f = await heldDispositionFixture(true);
			const reportId = workAttemptReportId(f.database.instanceId, f.task.jobId, f.task.opRef);
			const notificationId = f.control.notificationId;
			const unrelatedReport = workAttemptReportId(f.database.instanceId, "unrelated-job", "gw-unrelated");
			for (const messageId of [reportId, notificationId, "unrelated-history", unrelatedReport]) {
				f.database.inboundEnqueue({
					messageId,
					originKey: originKey(LOOPBACK_ORIGIN),
					originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
					body: "Old terminal history.",
					receivedAt: START,
				});
				f.raw.query("UPDATE inbound_messages SET state = 'done' WHERE message_id = ?").run(messageId);
			}
			const sources = f.raw.query("SELECT * FROM work_task_sources ORDER BY sequence").all();
			const controls = f.raw.query("SELECT * FROM work_controls ORDER BY sequence").all();
			if (runtimeState === "malformed")
				f.raw.query("UPDATE work_attempt_runtime SET record_json = '{bad' WHERE op_ref = ?").run(f.task.opRef);
			else if (runtimeState === "missing")
				f.raw.query("DELETE FROM work_attempt_runtime WHERE op_ref = ?").run(f.task.opRef);
			const runtime = f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef);
			// A retained prefix must not consume the deletion batch or starve unrelated rows.
			for (let batch = 0; batch < 2; batch++)
				expect(f.database.retentionSweep(new Date("2027-01-01T00:00:00.000Z"), 1).deleted.inbound_messages).toBe(1);
			expect(f.database.retentionSweep(new Date("2027-01-01T00:00:00.000Z"), 1).deleted.inbound_messages).toBe(0);
			expect(
				f.raw.query<{ message_id: string }, []>("SELECT message_id FROM inbound_messages ORDER BY message_id").all(),
			).toEqual([notificationId, reportId].sort().map((message_id) => ({ message_id })));
			expect(f.raw.query("SELECT * FROM work_task_sources ORDER BY sequence").all()).toEqual(sources);
			expect(f.raw.query("SELECT * FROM work_controls ORDER BY sequence").all()).toEqual(controls);
			expect(f.raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(f.task.opRef)).toEqual(runtime);
		});

	test("full immutable delivery payload joins a source and remains retained after confirmation", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const input = source("original-final-detail");
		const completeText = "Recoverable original text. ".repeat(2500);
		const delivery = payload(task, input.sourceId, completeText);
		f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(input, delivery));
		f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(input, delivery));
		expect(f.database.deliveryRows()).toHaveLength(1);
		const persisted = f.raw
			.query<{ payload_json: string }, [string]>("SELECT payload_json FROM deliveries WHERE delivery_id = ?")
			.get(delivery.deliveryId!)!;
		expect(JSON.parse(persisted.payload_json).text).toBe(completeText);
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction(input, {
					...delivery,
					text: "Conflicting retry.",
				}),
			),
		).toThrow();
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskSourceAppendInTransaction(source("redirect"), {
					...payload(task, "redirect"),
					origin: LOOPBACK_ORIGIN,
				}),
			),
		).toThrow(WorkTaskStateError);
		f.raw.exec("UPDATE deliveries SET state = 'confirmed'");
		f.database.deliveryPrune("2027-01-01T00:00:00.000Z");
		f.database.retentionSweep(new Date("2027-01-01T00:00:00.000Z"));
		expect(f.database.deliveryRows()).toHaveLength(1);
		expect(f.database.workTaskSourceGet(input.sourceId)?.deliveryId).toBe(delivery.deliveryId!);
	});

	test("terminal evidence cannot reopen execution or promote an unavailable final to an admitted result", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		f.database.workAttemptUpdate(runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		const control = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("before-terminal")),
		).record;
		f.database.workAttemptUpdate(runtime.opRef, 1, {
			terminal: {
				kind: "broker",
				observedAt: LATER,
				reasonCode: "end_turn",
				status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
			},
		});
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workControlTransitionInTransaction(control.controlId, 0, {
					phase: "sending",
					identity,
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		const prepared = f.database.workTaskGet(TASK)!;
		expect(() =>
			f.database.withTransaction(() =>
				f.database.workTaskObligationInTransaction(TASK, prepared.version, {
					identity,
					state: "final_admitted",
					reason: null,
					at: LATER,
				}),
			),
		).toThrow(WorkTaskStateError);
		const held = f.database.withTransaction(() =>
			f.database.workTaskObligationInTransaction(TASK, prepared.version, {
				identity,
				state: "held",
				reason: "original_output_unavailable",
				at: LATER,
			}),
		)!;
		expect(held.terminalReportId).toBe(runtime.reportId);
		expect(held.dispatchPhase).toBe("prepared");
		expect(f.database.workControlGet(control.controlId)?.phase).toBe("pending");
		const later = f.database.withTransaction(() =>
			f.database.workControlAdmitInTransaction(controlRequest("after-terminal")),
		).record;
		expect(later).toMatchObject({
			phase: "refused",
			reason: "original_attempt_terminal",
			receipt: null,
			sendingAt: null,
		});
	});

	test("task reads fail closed for a torn prepared binding and leave the unrelated task addressable", async () => {
		const f = await fixture();
		const task = f.bind(f.create().record);
		const { runtime, record } = f.prepare(task);
		f.database.workAttemptPrepare(runtime, record);
		const other = f.create(OTHER, "gw-unrelated").record;
		const prepared = f.database.workTaskGet(TASK)!;
		f.raw
			.query("UPDATE work_tasks SET record_json = ? WHERE task_id = ?")
			.run(JSON.stringify({ ...prepared, epoch: prepared.epoch! + 1 }), TASK);
		expect(() => f.database.workTaskGet(TASK)).toThrow(WorkTaskStateError);
		expect(() => f.database.workTaskList()).toThrow(WorkTaskStateError);
		const keys = f.database.workTaskKeys();
		expect(keys).toEqual({
			keys: [
				{ taskId: TASK, jobId: task.jobId },
				{ taskId: OTHER, jobId: other.jobId },
			],
			nextTaskId: null,
		});
		const dispositions = keys.keys.map(({ taskId }) => {
			try {
				return { taskId, disposition: "readable", record: f.database.workTaskGet(taskId) };
			} catch {
				return { taskId, disposition: "corrupt" };
			}
		});
		expect(dispositions).toEqual([
			{ taskId: TASK, disposition: "corrupt" },
			{ taskId: OTHER, disposition: "readable", record: other },
		]);
		expect(f.database.workTaskKeys()).toEqual(keys);
		expect(f.database.workTaskSourceGet(`assignment-${TASK}`)?.evidence).toEqual(evidence);
		expect(f.database.workTaskGet(OTHER)).toEqual(other);
		expect(f.database.workAttemptGet(runtime.opRef)?.opRef).toBe(runtime.opRef);
	});

	test("raw task keys paginate beyond twenty permanent records without consuming or hiding corruption", async () => {
		const f = await fixture();
		const tasks = Array.from({ length: 23 }, (_, index) => {
			const taskId = `00000000-0000-0000-0000-${String(index + 1).padStart(12, "0")}`;
			return f.create(taskId, `gw-page-${index}`).record;
		});
		f.raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(tasks[0]!.taskId);
		f.database.inboundEnqueue({
			messageId: "unconsumed-during-enumeration",
			originKey: originKey(LOOPBACK_ORIGIN),
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: "Keep pending.",
			receivedAt: START,
		});
		const first = f.database.workTaskKeys();
		expect(first.keys).toEqual(tasks.slice(0, 20).map(({ taskId, jobId }) => ({ taskId, jobId })));
		expect(first.nextTaskId).toBe(tasks[19]!.taskId);
		const second = f.database.workTaskKeys(20, first.nextTaskId!);
		expect(second.keys).toEqual(tasks.slice(20).map(({ taskId, jobId }) => ({ taskId, jobId })));
		expect(second.nextTaskId).toBeNull();
		expect(f.database.workTaskKeys(20, tasks[22]!.taskId)).toEqual({ keys: [], nextTaskId: null });
		expect(f.database.workTaskKeys()).toEqual(first);
		expect(() => f.database.workTaskGet(tasks[0]!.taskId)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskGet(tasks[22]!.taskId)).toEqual(tasks[22]!);
		expect(
			f.raw
				.query<{ state: string }, []>(
					"SELECT state FROM inbound_messages WHERE message_id = 'unconsumed-during-enumeration'",
				)
				.get()?.state,
		).toBe("pending");
		expect(f.raw.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM work_task_sources").get()?.count).toBe(23);
	});

	test("local terminal supplement admits only the exact re-observed original status", async () => {
		const f = await fixture();
		const bound = f.bind(f.create().record);
		const { runtime, record } = f.prepare(bound);
		f.database.workAttemptPrepare(runtime, record);
		const reportBody = "Host lost before the gateway could read the result.";
		const report = buildDeliveryPayload(runtime.opRef, LOOPBACK_ORIGIN, reportBody, runtime.deliveryId)!;
		f.database.workAttemptSettle(
			runtime.opRef,
			0,
			closeAttempt({ record, opRef: runtime.opRef, endState: "terminal_uncertain", endedAt: LATER }),
			{
				terminal: { kind: "local", observedAt: LATER, reasonCode: "session_dead" },
				output: { ...runtime.output, disposition: "unavailable" },
				decision: "report",
				settledAt: LATER,
			},
			{
				kind: "persona",
				row: {
					messageId: runtime.reportId,
					originKey: originKey(LOOPBACK_ORIGIN),
					originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
					body: reportBody,
					receivedAt: LATER,
				},
				fallbackPayload: report,
			},
		);
		const settled = f.database.workAttemptGet(runtime.opRef)!;
		expect(settled.terminal).toMatchObject({ kind: "local", reasonCode: "session_dead" });
		const task = f.database.workTaskGet(TASK)!;
		const jobBefore = f.database.laneJobJson(task.jobId);
		const reportBefore = f.raw.query("SELECT * FROM inbound_messages WHERE message_id = ?").get(runtime.reportId);
		const text = "Late original result admitted once.";
		type SupplementInput = Parameters<GatewayDatabase["workTaskSupplementInTransaction"]>[2];
		const proof: SupplementInput["proof"] = {
			source: "turn.result",
			fullness: "original",
			sessionId: SESSION,
			repo: request.cwd,
			opRef: runtime.opRef,
			clientRef: runtime.opRef,
			commandId: "command-1",
			turnId: "turn-1",
			terminalAt: Date.parse(LATER),
			contentVersion: 1,
			byteLength: Buffer.byteLength(text),
		};
		const status: SupplementInput["status"] = {
			status: "terminal_ok",
			receiptState: "present",
			clientRef: runtime.opRef,
			commandId: "command-1",
			turnId: "turn-1",
			startedAt: Date.parse(START),
			terminalAt: Date.parse(LATER),
		};
		const supplement = (
			presented: SupplementInput["status"] = status,
			proofInput: SupplementInput["proof"] = proof,
			version: number = task.version,
		) =>
			f.database.withTransaction(() =>
				f.database.workTaskSupplementInTransaction(
					TASK,
					version,
					{ identity, text, at: LATER, status: presented, proof: proofInput },
					payload(bound, `supplement-${TASK}-original`, text),
				),
			);
		const invalidStatuses: Array<SupplementInput["status"]> = [
			{ ...status, clientRef: undefined },
			{ ...status, status: "in_flight" },
			{ ...status, receiptState: "missing" },
			{ ...status, terminalAt: Date.parse(START) - 1 },
			{ ...status, terminalAt: Date.parse(LATER) + 60_000 },
			{ ...status, commandId: undefined },
			{ ...status, turnId: undefined },
		];
		for (const presented of invalidStatuses) expect(() => supplement(presented)).toThrow(WorkTaskStateError);
		expect(() => supplement(status, { ...proof, turnId: "turn-2" })).toThrow(WorkTaskStateError);
		expect(f.database.workTaskSourceGet(`supplement-${TASK}-original`)).toBeUndefined();
		expect(f.database.workTaskSourceGet(`supplement-proof-${TASK}-original`)).toBeUndefined();
		expect(f.database.workTaskGet(TASK)).toEqual(task);
		expect(f.database.workAttemptGet(runtime.opRef)).toEqual(settled);
		expect(f.database.laneJobJson(task.jobId)).toBe(jobBefore);

		const admitted = supplement();
		expect(admitted).toMatchObject({ obligationState: "final_admitted", holdReason: null });
		expect(f.database.workAttemptGet(runtime.opRef)).toEqual(settled);
		expect(f.database.laneJobJson(task.jobId)).toBe(jobBefore);
		expect(f.raw.query("SELECT * FROM inbound_messages WHERE message_id = ?").get(runtime.reportId)).toEqual(
			reportBefore,
		);
		const stored = JSON.parse(f.database.workTaskSourceGet(`supplement-proof-${TASK}-original`)!.body);
		expect(stored.status).toEqual(status);
		expect(stored.identity).toEqual(identity);
		expect(stored.proof).toEqual(proof);
		expect(f.database.workTaskSourceGet(`supplement-${TASK}-original`)?.body).toBe(text);
		// One stable supplement: the consumed version is refused, a fresh one cannot repeat it.
		expect(supplement()).toBeUndefined();
		expect(() => supplement(status, proof, f.database.workTaskGet(TASK)!.version)).toThrow(WorkTaskStateError);
		expect(f.database.workTaskSourceGet(`supplement-${TASK}-original`)?.body).toBe(text);
	});

	test("broker terminal supplement keeps exact historical status matching", async () => {
		const f = await fixture();
		const bound = f.bind(f.create().record);
		const { runtime, record } = f.prepare(bound);
		f.database.workAttemptPrepare(runtime, record);
		const text = "Original retained answer.";
		const report = buildDeliveryPayload(runtime.opRef, LOOPBACK_ORIGIN, text, runtime.deliveryId)!;
		f.database.workAttemptSettle(
			runtime.opRef,
			0,
			closeAttempt({ record, opRef: runtime.opRef, endState: "completed", endedAt: LATER }),
			{
				terminal: {
					kind: "broker",
					observedAt: LATER,
					reasonCode: "end_turn",
					status: {
						status: "terminal_ok",
						receiptState: "present",
						outcome: { reason: "end_turn" },
						terminalAt: Date.parse(LATER),
					},
				},
				output: {
					...runtime.output,
					disposition: "available",
					excerpt: text,
					proof: {
						opRef: runtime.opRef,
						sessionId: runtime.sessionId,
						epoch: runtime.epoch,
						observedAtMs: Date.parse(LATER),
						source: "turn.result",
						attribution: "operation_ref",
						fullness: "original",
						clientRef: runtime.opRef,
						repo: runtime.cwd,
						terminalAt: Date.parse(LATER),
						contentVersion: 1,
						byteLength: Buffer.byteLength(text),
					},
				},
				decision: "report",
				settledAt: LATER,
			},
			{
				kind: "persona",
				row: {
					messageId: runtime.reportId,
					originKey: originKey(LOOPBACK_ORIGIN),
					originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
					body: text,
					receivedAt: LATER,
				},
				fallbackPayload: report,
			},
		);
		const settled = f.database.workAttemptGet(runtime.opRef)!;
		const recorded = settled.terminal!.status!;
		const task = f.database.workTaskGet(TASK)!;
		type SupplementInput = Parameters<GatewayDatabase["workTaskSupplementInTransaction"]>[2];
		const proof: SupplementInput["proof"] = {
			source: "turn.result",
			fullness: "original",
			sessionId: SESSION,
			repo: request.cwd,
			opRef: runtime.opRef,
			clientRef: runtime.opRef,
			terminalAt: Date.parse(LATER),
			contentVersion: 1,
			byteLength: Buffer.byteLength(text),
		};
		const supplement = (presented: SupplementInput["status"], version: number = task.version) =>
			f.database.withTransaction(() =>
				f.database.workTaskSupplementInTransaction(
					TASK,
					version,
					{ identity, text, at: LATER, status: presented, proof },
					payload(bound, `supplement-${TASK}-original`, text),
				),
			);
		// A fresher observation never replaces the immutable recorded broker status.
		expect(() => supplement({ ...recorded, clientRef: runtime.opRef })).toThrow(WorkTaskStateError);
		expect(() => supplement({ ...recorded, terminalAt: Date.parse(LATER) + 1 })).toThrow(WorkTaskStateError);
		expect(f.database.workTaskSourceGet(`supplement-proof-${TASK}-original`)).toBeUndefined();
		expect(f.database.workTaskGet(TASK)).toEqual(task);
		const admitted = supplement(recorded);
		expect(admitted).toMatchObject({ obligationState: "final_admitted", holdReason: null });
		expect(f.database.workAttemptGet(runtime.opRef)).toEqual(settled);
		const stored = JSON.parse(f.database.workTaskSourceGet(`supplement-proof-${TASK}-original`)!.body);
		expect(stored.status).toEqual(recorded);
		expect(stored.proof).toEqual(proof);
		expect(supplement(recorded)).toBeUndefined();
		expect(f.database.workTaskSourceGet(`supplement-${TASK}-original`)?.body).toBe(text);
	});

	test("pending task without lane history is counted, snapshotted and quarantined through its reserved job", async () => {
		const f = await fixture();
		const task = f.create().record;
		const target = { canonicalAgentDir: join(f.directory, "other-agent"), identity: "other-authority" };
		expect(f.database.inspectBrokerAuthority().openWork).toBeGreaterThan(0);
		expect(() =>
			f.database.cutoverBrokerAuthority({
				expectedAuthority: f.authority,
				targetAuthority: target,
				evidence: "Explicit cutover.",
			}),
		).toThrow(BrokerAuthorityError);
		const cutover = f.database.cutoverBrokerAuthority({
			expectedAuthority: f.authority,
			targetAuthority: target,
			evidence: "Explicit quarantine.",
			disposition: "quarantine",
		});
		const snapshot = JSON.parse(f.database.brokerCutoverSnapshot(cutover)!);
		expect(snapshot.work_tasks[0].task_id).toBe(TASK);
		expect(snapshot.work_task_sources[0].source_id).toBe(`assignment-${TASK}`);
		expect(snapshot.work_controls).toEqual([]);
		expect(f.database.isBrokerQuarantined("work", task.jobId)).toBe(true);
		expect(f.database.workTaskGet(TASK)?.opRef).toBe(task.opRef);
		expect(() => f.bind(task)).toThrow(BrokerAuthorityError);
		expect(() => f.create()).toThrow(BrokerAuthorityError);
		expect(() =>
			f.database.withTransaction(() => f.database.workTaskSourceAppendInTransaction(source("post-cutover"))),
		).toThrow(BrokerAuthorityError);
	});

	test("schema 32 upgrade preserves old data while enabling task transactions", async () => {
		const f = await fixture();
		f.database.metaSet("preexisting-evidence", "unchanged");
		const instance = f.database.instanceId;
		f.database.close();
		handles.splice(handles.indexOf(f.database), 1);
		f.raw.exec(
			"DROP TABLE work_task_sources; DROP TABLE work_controls; DROP TABLE work_tasks; DELETE FROM schema_migrations WHERE version = 33",
		);
		const upgraded = await GatewayDatabase.open(f.path);
		handles.push(upgraded);
		expect(upgraded.metaGet("preexisting-evidence")).toBe("unchanged");
		expect(upgraded.instanceId).toBe(instance);
		expect(upgraded.workTaskCreate({ taskId: TASK, opRef: identity.opRef, request }).disposition).toBe("created");
		expect(upgraded.workTaskSources(TASK)?.sources[0]?.body).toBe(request.text);
	});
});

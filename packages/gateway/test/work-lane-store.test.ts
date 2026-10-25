import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtemp, realpath, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type FirstmateApproval,
	type FirstmateReleaseIntent,
	type FirstmateReleaseReceipt,
	LOOPBACK_ORIGIN,
	type OriginRef,
	originKey,
} from "@gajae-gateway/protocol";
import {
	appendAttempt,
	closeAttempt,
	createLaneJobRecord,
	parseLaneJobRecord,
	recordChecksRetry,
	reopenCompletedJob,
	transitionToAwaitingChecks,
} from "@gajae-gateway/subsession";
import { buildDeliveryPayload, DeliveryService } from "../src/delivery/delivery";
import { DatabaseStartupError, type FirstmateAssignment, type FirstmatePrepare, Store } from "../src/store";
import {
	resolveLaneReportOrigin,
	type WorkAttemptAdmission,
	type WorkAttemptPatch,
	type WorkAttemptRuntime,
	type WorkAttemptSettlement,
	WorkAttemptStateError,
	workAccepted,
	workAttemptDeliveryId,
	workAttemptReportId,
} from "../src/store/groups/work";
import type { WriteTx } from "../src/store/tx";

const START = "2026-09-08T00:00:00.000Z";
const END = "2026-09-08T00:00:10.000Z";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const PARENT_SESSION = "bcb7f8a3-30e5-4d76-9e7e-32d2e2f51f9a";
const directories: string[] = [];
const handles: Array<{ close(): void }> = [];
afterEach(async () => {
	for (const handle of handles.splice(0)) handle.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function fixture(worktree?: { path: string; branch: string }) {
	const directory = await mkdtemp(join(tmpdir(), "work-lane-store-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	handles.push(database);
	const canonicalAgentDir = join(await realpath(directory), "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.transaction((tx) =>
		database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated),
	);
	expect(
		database.transaction((tx) =>
			database.bindings.recordOwnedBinding(tx, {
				authority,
				sessionId: SESSION,
				originKey: "work/task/a",
				epoch: 0,
				repo: worktree?.path ?? "/work",
			}),
		),
	).toBe(true);
	const raw = new Database(path);
	handles.push(raw);
	const record = appendAttempt(
		createLaneJobRecord({
			jobId: "lanejob-61",
			branch: worktree?.branch ?? "main",
			worktreePath: worktree?.path ?? "/work",
			sessionId: SESSION,
			now: () => new Date(START),
		}),
		{ opRef: "gw-work-store-a", sessionId: SESSION, startedAt: START },
	);
	const runtime: WorkAttemptRuntime = {
		opRef: "gw-work-store-a",
		jobId: record.jobId,
		laneKey: "work-a",
		sessionKey: "work/task/a",
		sessionId: SESSION,
		epoch: 0,
		cwd: worktree?.path ?? "/work",
		startedAt: START,
		mode: "start",
		sendPhase: "prepared",
		sendEvidence: null,
		terminal: null,
		output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
		parent: { kind: "persona", originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
		reportId: workAttemptReportId(database.instanceId, record.jobId, "gw-work-store-a"),
		wakeReportId: null,
		noticeHash: null,
		deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, "gw-work-store-a"),
		decision: "undecided",
		settledAt: null,
		version: 0,
	};
	const closed = closeAttempt({ record, opRef: runtime.opRef, endState: "completed", endedAt: END });
	const settlement: WorkAttemptSettlement = {
		terminal: {
			kind: "broker",
			observedAt: END,
			reasonCode: "end_turn",
			status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
		},
		output: { ...runtime.output, disposition: "unavailable" },
		decision: "report",
		settledAt: END,
	};
	const payload = buildDeliveryPayload(
		runtime.opRef,
		LOOPBACK_ORIGIN,
		"[lane a] completed: output_unavailable",
		runtime.deliveryId,
	)!;
	const admission: WorkAttemptAdmission = {
		kind: "persona",
		row: {
			messageId: runtime.reportId,
			originKey: originKey(LOOPBACK_ORIGIN),
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: payload.text,
			receivedAt: END,
		},
		fallbackPayload: payload,
	};
	return { directory, path, database, raw, authority, record, runtime, closed, settlement, payload, admission };
}

async function firstmateFixture() {
	const directory = await mkdtemp(join(tmpdir(), "firstmate-store-worktree-"));
	directories.push(directory);
	const root = await realpath(directory);
	const repository = join(root, "repository");
	const path = join(root, "task");
	const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
	const git = (...args: string[]) =>
		execFileSync("git", args, {
			encoding: "utf8",
			env,
			stdio: ["ignore", "pipe", "pipe"],
		}).trim();
	git("init", "--initial-branch=main", repository);
	git(
		"-C",
		repository,
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"-c",
		"commit.gpgSign=false",
		"commit",
		"--allow-empty",
		"-m",
		"fixture",
	);
	git("-C", repository, "worktree", "add", "-b", "task-a", path);
	const f = await fixture({ path, branch: "task-a" });
	const assignment: FirstmateAssignment = {
		taskId: "task-a",
		name: "a",
		ownerId: "owner-a",
		messageId: "message-a",
		thread: { platform: "discord", kind: "thread", conversationId: "thread-a", parentId: "channel-a" },
		mandate: {
			id: "mandate-a",
			text: "Preserve exact input: 한글\nno implicit push\n",
			checkRetries: true,
			commit: false,
			push: false,
		},
		worktree: {
			path,
			branch: "task-a",
			commonDir: await realpath(git("-C", path, "rev-parse", "--path-format=absolute", "--git-common-dir")),
			gitDir: await realpath(git("-C", path, "rev-parse", "--absolute-git-dir")),
		},
	};
	const runtime: WorkAttemptRuntime = {
		...f.runtime,
		parent: { kind: "persona", originKey: originKey(assignment.thread), origin: assignment.thread },
	};
	const evidence: FirstmatePrepare = { assignment, cause: { kind: "assignment" }, text: assignment.mandate.text };
	return { ...f, runtime, assignment, evidence, owner: f.database.work.firstmate };
}

function firstmateRows(raw: Database) {
	return {
		assignments: raw.query("SELECT * FROM firstmate_assignments ORDER BY task_id").all(),
		attempts: raw.query("SELECT * FROM firstmate_attempts ORDER BY op_ref").all(),
	};
}

function prepareState(f: Awaited<ReturnType<typeof fixture>>) {
	return {
		evidence: firstmateRows(f.raw),
		work: f.database.work.projectionRows(),
		deliveries: f.database.outbox.projectionRows(),
		journal: f.raw.query("SELECT * FROM work_journal ORDER BY seq").all(),
		sessions: f.database.bindings.sessionIdentityRows(),
	};
}

function prepareFirstmate(f: Awaited<ReturnType<typeof firstmateFixture>>) {
	f.database.transaction((tx) => {
		f.database.work.workAttemptPrepare(tx, f.runtime, f.record, true, f.evidence);
		touchWorkSession(f.database, tx, f.runtime, START);
	});
}

async function correctionFixture(checkRetries = false, state: "attempt_ended" | "done" = "attempt_ended") {
	const base = await firstmateFixture();
	const assignment = { ...base.assignment, mandate: { ...base.assignment.mandate, checkRetries } };
	const f = { ...base, assignment, evidence: { ...base.evidence, assignment } };
	prepareFirstmate(f);
	f.database.transaction((tx) =>
		updateAttempt(f.database, tx, f.runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		}),
	);
	const closed = closeAttempt({ record: f.record, opRef: f.runtime.opRef, endState: "completed", endedAt: END });
	const payload = buildDeliveryPayload(f.runtime.opRef, assignment.thread, "prior result", f.runtime.deliveryId)!;
	const admission: WorkAttemptAdmission = {
		kind: "persona",
		row: {
			messageId: f.runtime.reportId,
			originKey: originKey(assignment.thread),
			originRefJson: JSON.stringify(assignment.thread),
			body: payload.text,
			receivedAt: END,
		},
		fallbackPayload: payload,
	};
	f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 1, closed, f.settlement, admission));
	// Ordinary mapped end_turn stays attempt_ended: do not overwrite its settlement.
	// The separate done fixture retains coverage for an already completed native job.
	const settled = state === "done" ? { ...closed, state } : closed;
	if (state === "done") {
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...settled, laneKey: f.runtime.laneKey, json: JSON.stringify(settled) }),
		);
	}
	const opRef = "gw-firstmate-correction";
	const record = appendAttempt(settled.state === "done" ? reopenCompletedJob(settled, f.runtime.opRef) : settled, {
		opRef,
		sessionId: SESSION,
		startedAt: END,
	});
	const runtime: WorkAttemptRuntime = {
		...f.runtime,
		opRef,
		startedAt: END,
		reportId: workAttemptReportId(f.database.instanceId, f.runtime.jobId, opRef),
		deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
	};
	const cause = {
		kind: "correction" as const,
		priorOpRef: f.runtime.opRef,
		ownerId: assignment.ownerId,
		messageId: "correction-message",
		mandateId: assignment.mandate.id,
	};
	const evidence: FirstmatePrepare = { assignment, cause, text: "Correct the answer.\n정확한 입력\n" };
	const prepare = (input = evidence, next = record, attempt = runtime) =>
		f.database.transaction((tx) => {
			f.database.work.workAttemptPrepare(tx, attempt, next, true, input);
			touchWorkSession(f.database, tx, attempt, END);
		});
	return { ...f, settled, correction: { record, runtime, cause, evidence, prepare } };
}

function touchWorkSession(database: Store, tx: WriteTx, runtime: WorkAttemptRuntime, at: string): void {
	database.bindings.touchWorkSession(
		tx,
		runtime.sessionKey,
		runtime.sessionId,
		runtime.epoch,
		at,
		JSON.stringify({ platform: "work", kind: "task", conversationId: runtime.sessionKey.slice("work/task/".length) }),
	);
}

function decisionFixture(f: Awaited<ReturnType<typeof correctionFixture>>): FirstmateApproval {
	return {
		approvalId: "approval-a",
		recordedAt: END,
		name: f.assignment.name,
		identity: {
			taskId: f.assignment.taskId,
			jobId: f.runtime.jobId,
			opRef: f.runtime.opRef,
			ordinal: 0,
			sessionId: f.runtime.sessionId,
			epoch: f.runtime.epoch,
		},
		ownerId: f.assignment.ownerId,
		messageId: "approve-message",
		thread: f.assignment.thread,
		mandateId: f.assignment.mandate.id,
		scope: "Accept this answer only; no publication.",
		decision: "accept_outcome",
		// Store fixtures exercise attribution/shape, not manager observation authenticity.
		evidence: {
			completeness: "complete",
			text: "답변",
			source: "native",
			provenance: {
				source: "turn.result",
				fullness: "original",
				sessionId: f.runtime.sessionId,
				repo: f.runtime.cwd,
				opRef: f.runtime.opRef,
				clientRef: f.runtime.opRef,
				terminalAt: Date.parse(END),
				contentVersion: 1,
				byteLength: Buffer.byteLength("답변"),
			},
			reasons: [],
		},
	};
}

function preservingIntent(approval: FirstmateApproval): FirstmateReleaseIntent {
	return {
		releaseId: "release-a",
		requestedAt: END,
		request: {
			name: approval.name,
			identity: approval.identity,
			ownerId: approval.ownerId,
			messageId: "release-message",
			thread: approval.thread,
			mandateId: approval.mandateId,
			scope: approval.scope,
			approvalId: approval.approvalId,
			action: "release_session_preserve_work",
		},
	};
}

describe("FirstMate immutable decisions and preserving-release persistence", () => {
	test("exact inbox disposition preserves missing, processing, unlinked completion and quarantine evidence", async () => {
		const f = await fixture();
		const origin = originKey(LOOPBACK_ORIGIN);
		const messageId = f.runtime.reportId;
		f.database.transaction((tx) => {
			for (const id of [messageId, "unrelated-same-origin"]) {
				f.database.inbox.enqueue(tx, {
					messageId: id,
					originKey: origin,
					originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
					body: "report",
					receivedAt: END,
					source: "lane_report",
				});
			}
		});
		expect(f.database.inbox.messageDisposition(origin, "missing")).toBeUndefined();
		expect(f.database.inbox.messageDisposition("other-origin", messageId)).toBeUndefined();
		expect(f.database.inbox.messageDisposition(origin, messageId)).toMatchObject({
			message: { message_id: messageId, state: "pending", turn_state: null, terminal_delivery_id: null },
			quarantined: false,
		});
		// Legacy processing rows must not disappear behind replayable/pending filters.
		f.raw.query("UPDATE inbound_messages SET state = 'processing' WHERE message_id = ?").run("unrelated-same-origin");
		expect(f.database.inbox.messageDisposition(origin, "unrelated-same-origin")?.message.state).toBe("processing");
		const opRef = "gw-report-inbox";
		f.database.transaction((tx) => {
			f.database.inbox.bindTurn(tx, {
				messageId,
				originKey: origin,
				epoch: 0,
				opRef,
				sessionId: PARENT_SESSION,
				dispatchedAt: END,
			});
			f.database.inbox.turnAccept(tx, opRef);
		});
		expect(f.database.inbox.messageDisposition(origin, messageId)?.message.turn_state).toBe("accepted");
		f.database.transaction((tx) => f.database.inbox.turnComplete(tx, opRef, "no_delivery"));
		f.raw
			.query("INSERT INTO broker_quarantine (kind, subject_id, cutover_id) VALUES ('inbound', ?, ?)")
			.run(messageId, "closeout-inbox-quarantine");
		const before = f.raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all();
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(reopened.inbox.messageDisposition(origin, messageId)).toMatchObject({
			message: {
				message_id: messageId,
				state: "done",
				turn_state: "done",
				terminal_delivery_id: JSON.stringify({ none: "no_delivery" }),
			},
			quarantined: true,
		});
		expect(reopened.inbox.messageDisposition(origin, "missing")).toBeUndefined();
		expect(f.raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all()).toEqual(before);
	});

	test("task readers isolate exact task IDs and order corrected uncertain preparations by ordinal", async () => {
		const f = await correctionFixture();
		f.correction.prepare();
		f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.correction.runtime.opRef, 0, { sendPhase: "uncertain" }),
		);
		const other = await firstmateFixture();
		const assignment: FirstmateAssignment = {
			...other.assignment,
			taskId: "task-a-extra",
			name: "b",
			messageId: "other-assignment",
			thread: { ...other.assignment.thread, conversationId: "other-thread" },
		};
		const opRef = "gw-other-task";
		const record = appendAttempt(
			createLaneJobRecord({
				jobId: "lanejob-62",
				branch: assignment.worktree.branch,
				worktreePath: assignment.worktree.path,
				sessionId: SESSION,
				now: () => new Date(START),
			}),
			{ opRef, sessionId: SESSION, startedAt: START },
		);
		const runtime: WorkAttemptRuntime = {
			...other.runtime,
			opRef,
			jobId: record.jobId,
			sessionKey: "work/task/b",
			laneKey: "work-b",
			parent: { kind: "persona", originKey: originKey(assignment.thread), origin: assignment.thread },
			reportId: workAttemptReportId(f.database.instanceId, record.jobId, opRef),
			deliveryId: workAttemptDeliveryId(f.database.instanceId, record.jobId, opRef),
		};
		f.database.transaction((tx) =>
			f.database.work.workAttemptPrepare(tx, runtime, record, true, {
				assignment,
				cause: { kind: "assignment" },
				text: assignment.mandate.text,
			}),
		);
		const before = prepareState(f);
		expect(f.owner.attemptsForTask(f.assignment.taskId)).toEqual([
			f.owner.attempt(f.runtime.opRef)!,
			f.owner.attempt(f.correction.runtime.opRef)!,
		]);
		expect(f.owner.attemptsForTask(assignment.taskId).map((attempt) => attempt.opRef)).toEqual([opRef]);
		expect(f.owner.attemptsForTask("task-a%")).toEqual([]);
		expect(f.owner.attemptsForTask("missing")).toEqual([]);
		expect(prepareState(f)).toEqual(before);
	});

	test("completed session coverage includes older corrected ops without rewriting the approved identity", async () => {
		const f = await correctionFixture();
		f.correction.prepare();
		const c = f.correction;
		f.database.transaction((tx) =>
			updateAttempt(f.database, tx, c.runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: END },
			}),
		);
		const endedAt = "2026-09-08T00:00:20.000Z";
		const closed = closeAttempt({ record: c.record, opRef: c.runtime.opRef, endState: "completed", endedAt });
		const payload = buildDeliveryPayload(
			c.runtime.opRef,
			f.assignment.thread,
			"corrected answer",
			c.runtime.deliveryId,
		)!;
		f.database.transaction((tx) =>
			settleAttempt(
				f.database,
				tx,
				c.runtime.opRef,
				1,
				closed,
				{ ...f.settlement, settledAt: endedAt },
				{
					kind: "persona",
					row: {
						messageId: c.runtime.reportId,
						originKey: originKey(f.assignment.thread),
						originRefJson: JSON.stringify(f.assignment.thread),
						body: payload.text,
						receivedAt: endedAt,
					},
					fallbackPayload: payload,
				},
			),
		);
		const base = decisionFixture(f);
		if (base.decision !== "accept_outcome") throw new Error("acceptance fixture required");
		const approval: FirstmateApproval = {
			...base,
			identity: { ...base.identity, opRef: c.runtime.opRef, ordinal: 1 },
			evidence: {
				completeness: "partial",
				source: "retained",
				text: "corrected answer",
				reasons: ["bounded retained evidence"],
				provenance: {
					...base.evidence.provenance,
					opRef: c.runtime.opRef,
					clientRef: c.runtime.opRef,
					terminalAt: Date.parse(endedAt),
					byteLength: Buffer.byteLength("corrected answer"),
				},
			},
		};
		const intent = preservingIntent(approval);
		const receipt: FirstmateReleaseReceipt = {
			...intent,
			releasedAt: endedAt,
			sessionKey: "work/task/a",
			preserved: true,
		};
		f.database.transaction((tx) => {
			f.owner.recordApproval(tx, approval);
			f.owner.recordReleaseIntent(tx, intent);
		});
		expect(f.owner.completedReleaseForSession(f.assignment.taskId, SESSION, 0)).toBeUndefined();
		f.database.transaction((tx) => {
			f.database.bindings.rotateEpoch(tx, f.runtime.sessionKey);
			f.owner.recordReleaseReceipt(tx, receipt);
		});
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		const owner = reopened.work.firstmate;
		expect(owner.correction(f.assignment.taskId, c.cause, c.evidence.text)).toEqual(owner.attempt(c.runtime.opRef));
		reopened.transaction((tx) => {
			owner.recordApproval(tx, approval);
			owner.recordReleaseIntent(tx, intent);
			owner.recordReleaseReceipt(tx, receipt);
		});
		for (const attempt of owner.attemptsForTask(f.assignment.taskId)) {
			expect(owner.completedReleaseForSession(attempt.taskId, attempt.sessionId, attempt.epoch)).toEqual(receipt);
		}
		expect(owner.completedReleaseForSession(f.assignment.taskId, SESSION, 0)?.request.identity.opRef).toBe(
			c.runtime.opRef,
		);
		expect(owner.completedReleaseForSession("task-a-extra", SESSION, 0)).toBeUndefined();
		expect(owner.completedReleaseForSession(f.assignment.taskId, PARENT_SESSION, 0)).toBeUndefined();
		expect(owner.completedReleaseForSession(f.assignment.taskId, SESSION, 1)).toBeUndefined();
		expect(owner.releaseForAttempt(f.runtime.opRef)).toBeUndefined();
	});

	for (const completed of [false, true]) {
		test(`${completed ? "completed release" : "pending intent"} holds fresh preparation after store restart`, async () => {
			const f = await correctionFixture(true);
			const approval = decisionFixture(f);
			const intent = preservingIntent(approval);
			f.database.transaction((tx) => {
				f.owner.recordApproval(tx, approval);
				f.owner.recordReleaseIntent(tx, intent);
				if (completed)
					f.owner.recordReleaseReceipt(tx, { ...intent, releasedAt: END, sessionKey: "work/task/a", preserved: true });
			});
			f.database.close();
			handles.splice(handles.indexOf(f.database), 1);
			const reopened = await Store.open(f.path);
			handles.push(reopened);
			const owner = reopened.work.firstmate;
			const before = prepareState({ ...f, database: reopened });
			const c = f.correction;
			expect(() =>
				reopened.transaction((tx) => reopened.work.workAttemptPrepare(tx, c.runtime, c.record, true, c.evidence)),
			).toThrow("FirstMate admission refused");
			// All cause kinds hit the same durable guard, before cause-specific lineage admission.
			for (const cause of [
				{ kind: "assignment" as const },
				c.cause,
				{
					kind: "checks" as const,
					priorOpRef: f.runtime.opRef,
					prNumber: 1,
					headSha: "head",
					failingChecks: ["ci"],
					retryOrdinal: 1,
				},
			]) {
				let failure: unknown;
				try {
					reopened.transaction((tx) => owner.recordPrepare(tx, c.runtime, c.record, { ...c.evidence, cause }));
				} catch (error) {
					failure = error;
				}
				expect(failure).toMatchObject({ detail: { reason: "task_release_pending_or_completed" } });
			}
			expect(owner.initial(f.assignment.taskId)).toEqual(owner.attempt(f.runtime.opRef));
			reopened.transaction((tx) => {
				owner.recordApproval(tx, approval);
				owner.recordReleaseIntent(tx, intent);
			});
			expect(owner.releaseIntent(intent.releaseId)).toEqual(intent);
			expect(owner.attempt(c.runtime.opRef)).toBeUndefined();
			expect(prepareState({ ...f, database: reopened })).toEqual(before);
		});
	}

	test("failed intent transaction leaves preparation admissible; failed receipt retains the durable hold", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		const intent = preservingIntent(approval);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		expect(() =>
			f.database.transaction((tx) => {
				f.owner.recordReleaseIntent(tx, intent);
				throw new Error("intent failure");
			}),
		).toThrow("intent failure");
		expect(f.owner.releaseIntent(intent.releaseId)).toBeUndefined();
		// Successful admission is rolled back deliberately so the same settled predecessor can release.
		expect(() =>
			f.database.transaction((tx) => {
				f.database.work.workAttemptPrepare(tx, f.correction.runtime, f.correction.record, true, f.correction.evidence);
				throw new Error("admitted preparation rollback");
			}),
		).toThrow("admitted preparation rollback");
		f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, intent));
		const before = prepareState(f);
		expect(() =>
			f.database.transaction((tx) => {
				f.database.bindings.rotateEpoch(tx, f.runtime.sessionKey);
				f.owner.recordReleaseReceipt(tx, { ...intent, releasedAt: END, sessionKey: "work/task/a", preserved: true });
				throw new Error("receipt failure");
			}),
		).toThrow("receipt failure");
		expect(f.owner.completedReleaseForSession(f.assignment.taskId, SESSION, 0)).toBeUndefined();
		expect(() => f.correction.prepare()).toThrow("FirstMate admission refused");
		expect(prepareState(f)).toEqual(before);
	});

	for (const state of ["attempt_ended", "done"] as const) {
		test(`records acceptance without changing native ${state} or journal`, async () => {
			const f = await correctionFixture(false, state);
			const approval = decisionFixture(f);
			const before = prepareState(f);
			f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
			expect(f.owner.approval(approval.approvalId)).toEqual(approval);
			expect(f.owner.approvalCurrent(approval.approvalId)).toBe(true);
			expect(prepareState(f)).toEqual(before);
			const reopened = await Store.open(f.path);
			handles.push(reopened);
			const freshOwner = reopened.work.firstmate;
			expect(freshOwner.approvalForMessage(approval.ownerId, approval.messageId)).toEqual(approval);
			// Caller cannot mutate a persisted receipt through the returned object.
			const copy = freshOwner.approval(approval.approvalId)!;
			(copy.evidence.reasons as string[]).push("changed");
			expect(f.owner.approval(approval.approvalId)).toEqual(approval);
			f.database.transaction((tx) =>
				f.owner.recordApproval(tx, { ...approval, approvalId: "retry-id", recordedAt: START }),
			);
			expect(f.owner.approval("retry-id")).toBeUndefined();
			expect(f.owner.approvalForMessage(approval.ownerId, approval.messageId)).toEqual(approval);
			for (const content of [
				{ ...approval, scope: "Different scope" },
				{ ...approval, evidence: { ...approval.evidence, text: "changed" } },
				{ ...approval, evidence: { ...approval.evidence, reasons: ["different limit"] } },
			]) {
				expect(() =>
					f.database.transaction((tx) => f.owner.recordApproval(tx, content as FirstmateApproval)),
				).toThrow();
			}
			expect(f.owner.approval(approval.approvalId)).toEqual(approval);
		});
	}

	test("partial acceptance and unavailable acknowledgement remain discriminated", async () => {
		const f = await correctionFixture();
		// The persistence validator must also refuse untrusted values outside the public type.
		const boundary: {
			recordApproval(tx: Parameters<typeof f.owner.recordApproval>[0], approval: unknown): void;
		} = f.owner;
		const complete = decisionFixture(f);
		if (complete.decision !== "accept_outcome") throw new Error("acceptance fixture required");
		const partial: FirstmateApproval = {
			...complete,
			evidence: { ...complete.evidence, completeness: "partial", reasons: ["bounded result"] },
		};
		f.database.transaction((tx) => f.owner.recordApproval(tx, partial));
		const acknowledgement: FirstmateApproval = {
			...complete,
			approvalId: "ack-a",
			messageId: "ack-message",
			decision: "acknowledge_unavailable",
			evidence: { completeness: "unavailable", reasons: ["native output unavailable"] },
		};
		f.database.transaction((tx) => f.owner.recordApproval(tx, acknowledgement));
		expect(f.owner.approval("ack-a")).toEqual(acknowledgement);
		expect(f.owner.approvalCurrent("ack-a")).toBe(true);
		expect(() =>
			f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, preservingIntent(acknowledgement))),
		).toThrow();
		expect(f.owner.releaseIntent("release-a")).toBeUndefined();
		const invalid = [
			{ ...acknowledgement, decision: "accept_outcome" },
			{ ...complete, decision: "acknowledge_unavailable" },
			{ ...partial, evidence: { ...partial.evidence, reasons: [] } },
			{ ...acknowledgement, evidence: { ...acknowledgement.evidence, text: "" } },
			{
				...partial,
				evidence: {
					...partial.evidence,
					source: "reconstructed",
					provenance: { source: "logs", fullness: "reconstructed" },
				},
			},
		];
		for (const value of invalid) {
			expect(() => f.database.transaction((tx) => boundary.recordApproval(tx, value))).toThrow();
		}
	});

	test("rejects every changed attribution and authority component before inserting", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		const invalid: FirstmateApproval[] = [
			...Object.entries({
				taskId: "other-task",
				jobId: "other-job",
				opRef: "gw-other",
				ordinal: 1,
				sessionId: PARENT_SESSION,
				epoch: 1,
			}).map(([key, value]) => ({ ...approval, identity: { ...approval.identity, [key]: value } })),
			{ ...approval, name: "other" },
			{ ...approval, ownerId: "other-owner" },
			{ ...approval, mandateId: "other-mandate" },
			{ ...approval, messageId: f.assignment.messageId },
			{ ...approval, scope: " " },
			{ ...approval, thread: { ...approval.thread, conversationId: "other-thread" } },
		];
		for (const value of invalid) {
			expect(() => f.database.transaction((tx) => f.owner.recordApproval(tx, value))).toThrow();
			expect(f.owner.approval(approval.approvalId)).toBeUndefined();
		}
	});

	test("a newer prepared uncertain attempt invalidates freshness, not historical receipts or retries", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		f.correction.prepare();
		f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.correction.runtime.opRef, 0, { sendPhase: "uncertain" }),
		);
		expect(f.owner.approvalCurrent(approval.approvalId)).toBe(false);
		expect(f.owner.approval(approval.approvalId)).toEqual(approval);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		expect(() =>
			f.database.transaction((tx) =>
				f.owner.recordApproval(tx, {
					...approval,
					approvalId: "new-approval",
					messageId: "new-message",
				}),
			),
		).toThrow();
		expect(() => f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, preservingIntent(approval)))).toThrow();
		expect(f.owner.releaseIntent("release-a")).toBeUndefined();
	});

	test("quarantined jobs and stale live bindings cannot admit new decisions or release intents", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		f.raw.query("UPDATE sessions SET epoch = epoch + 1 WHERE origin_key = ?").run(f.runtime.sessionKey);
		expect(() =>
			f.database.transaction((tx) =>
				f.owner.recordApproval(tx, {
					...approval,
					approvalId: "stale",
					messageId: "stale-message",
				}),
			),
		).toThrow();
		expect(() => f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, preservingIntent(approval)))).toThrow();
		f.raw.query("UPDATE sessions SET epoch = ? WHERE origin_key = ?").run(f.runtime.epoch, f.runtime.sessionKey);
		f.raw
			.query("INSERT INTO broker_quarantine (kind, subject_id, cutover_id) VALUES ('work', ?, ?)")
			.run(f.runtime.jobId, "receipt-quarantine");
		expect(() =>
			f.database.transaction((tx) =>
				f.owner.recordApproval(tx, {
					...approval,
					approvalId: "quarantined",
					messageId: "quarantined-message",
				}),
			),
		).toThrow();
		expect(() => f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, preservingIntent(approval)))).toThrow();
		expect(f.owner.approval(approval.approvalId)).toEqual(approval);
		expect(f.owner.releaseIntent("release-a")).toBeUndefined();
	});

	test("release requires every exact approval identity and authority tie", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		const intent = preservingIntent(approval);
		const invalid: FirstmateReleaseIntent[] = [
			...Object.entries({
				taskId: "other-task",
				jobId: "other-job",
				opRef: "gw-other",
				ordinal: 1,
				sessionId: PARENT_SESSION,
				epoch: 1,
			}).map(([key, value]) => ({
				...intent,
				request: { ...intent.request, identity: { ...intent.request.identity, [key]: value } },
			})),
			{ ...intent, request: { ...intent.request, approvalId: "other-approval" } },
			{ ...intent, request: { ...intent.request, ownerId: "other-owner" } },
			{ ...intent, request: { ...intent.request, mandateId: "other-mandate" } },
			{ ...intent, request: { ...intent.request, messageId: approval.messageId } },
			{ ...intent, request: { ...intent.request, thread: { ...approval.thread, conversationId: "other-thread" } } },
		];
		for (const value of invalid) {
			expect(() => f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, value))).toThrow();
			expect(f.owner.releaseIntent(intent.releaseId)).toBeUndefined();
		}
	});

	test("prepared, checks and ambiguous native execution cannot create acceptance", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		const original = f.raw.query("SELECT record_json FROM lane_jobs WHERE job_id = ?").get(f.runtime.jobId) as {
			record_json: string;
		};
		for (const change of [
			{ state: "running" },
			{ state: "awaiting_operator" },
			{ awaitingChecks: { prNumber: 1, headSha: "head", retries: 0 } },
			{ attempts: [{ ...f.settled.attempts[0], endedAt: undefined }] },
			{ attempts: [{ ...f.settled.attempts[0], endState: "terminal_uncertain" }] },
		]) {
			f.raw
				.query("UPDATE lane_jobs SET record_json = ? WHERE job_id = ?")
				.run(JSON.stringify({ ...f.settled, ...change }), f.runtime.jobId);
			expect(() => f.database.transaction((tx) => f.owner.recordApproval(tx, approval))).toThrow();
		}
		f.raw.query("UPDATE lane_jobs SET record_json = ? WHERE job_id = ?").run(original.record_json, f.runtime.jobId);
		const originalRuntime = f.raw
			.query("SELECT record_json FROM work_attempt_runtime WHERE op_ref = ?")
			.get(f.runtime.opRef) as { record_json: string };
		const runtime = JSON.parse(originalRuntime.record_json) as WorkAttemptRuntime;
		for (const change of [
			{ sendPhase: "prepared", sendEvidence: null },
			{ sendPhase: "uncertain", sendEvidence: null },
			{ settledAt: null },
			{ decision: "recovery_indeterminate" },
			{ terminal: { ...runtime.terminal, reasonCode: "terminal_uncertain" } },
			{ epoch: 1 },
			{ parent: { ...runtime.parent, origin: LOOPBACK_ORIGIN } },
		]) {
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
				.run(JSON.stringify({ ...runtime, ...change }), f.runtime.opRef);
			expect(() => f.database.transaction((tx) => f.owner.recordApproval(tx, approval))).toThrow();
		}
		expect(f.owner.approval(approval.approvalId)).toBeUndefined();
	});

	test("decision and release writes roll back with the owning transaction", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		const intent = preservingIntent(approval);
		expect(() =>
			f.database.transaction((tx) => {
				f.owner.recordApproval(tx, approval);
				f.owner.recordReleaseIntent(tx, intent);
				throw new Error("injected rollback");
			}),
		).toThrow("injected rollback");
		expect(f.owner.approval(approval.approvalId)).toBeUndefined();
		expect(f.owner.releaseIntent(intent.releaseId)).toBeUndefined();
		f.database.transaction((tx) => {
			f.owner.recordApproval(tx, approval);
			f.owner.recordReleaseIntent(tx, intent);
		});
		const receipt: FirstmateReleaseReceipt = { ...intent, releasedAt: END, sessionKey: "work/task/a", preserved: true };
		expect(() =>
			f.database.transaction((tx) => {
				f.owner.recordReleaseReceipt(tx, receipt);
				throw new Error("receipt rollback");
			}),
		).toThrow("receipt rollback");
		expect(f.owner.releaseIntent(intent.releaseId)).toEqual(intent);
		expect(f.owner.releaseReceipt(intent.releaseId)).toBeUndefined();
	});

	test("release intent is not a confirmed receipt; exact confirmed content remains immutable", async () => {
		const f = await correctionFixture();
		const approval = decisionFixture(f);
		const intent = preservingIntent(approval);
		const before = prepareState(f);
		f.database.transaction((tx) => f.owner.recordApproval(tx, approval));
		const receipt: FirstmateReleaseReceipt = { ...intent, releasedAt: END, sessionKey: "work/task/a", preserved: true };
		expect(() => f.database.transaction((tx) => f.owner.recordReleaseReceipt(tx, receipt))).toThrow();
		f.database.transaction((tx) => f.owner.recordReleaseIntent(tx, intent));
		expect(f.owner.releaseReceipt(intent.releaseId)).toBeUndefined();
		expect(f.owner.releaseForAttempt(approval.identity.opRef)).toEqual(intent);
		f.database.transaction((tx) =>
			f.owner.recordReleaseIntent(tx, { ...intent, releaseId: "retry-release", requestedAt: START }),
		);
		expect(f.owner.releaseIntent("retry-release")).toBeUndefined();
		expect(() =>
			f.database.transaction((tx) =>
				f.owner.recordReleaseIntent(tx, {
					...intent,
					request: { ...intent.request, scope: "different" },
				}),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => f.owner.recordReleaseReceipt(tx, { ...receipt, sessionKey: "work/task/other" })),
		).toThrow();
		// Persistence boundary only: the eventual native-close caller owns actual confirmation.
		f.database.transaction((tx) => f.owner.recordReleaseReceipt(tx, receipt));
		f.database.transaction((tx) => f.owner.recordReleaseReceipt(tx, receipt));
		expect(f.owner.releaseReceipt(intent.releaseId)).toEqual(receipt);
		expect(() =>
			f.database.transaction((tx) => f.owner.recordReleaseReceipt(tx, { ...receipt, releasedAt: START })),
		).toThrow();
		expect(prepareState(f)).toEqual(before);
		for (const table of ["firstmate_approvals", "firstmate_release_intents", "firstmate_release_receipts"]) {
			expect(() => f.raw.exec(`UPDATE ${table} SET record_json = '{}'`)).toThrow("immutable FirstMate receipt");
			expect(() => f.raw.exec(`DELETE FROM ${table}`)).toThrow("immutable FirstMate receipt");
		}
		expect(f.owner.approval(approval.approvalId)).toEqual(approval);
		expect(f.owner.releaseReceipt(intent.releaseId)).toEqual(receipt);
	});
});

function prepareAttempt(
	database: Store,
	tx: WriteTx,
	runtime: WorkAttemptRuntime,
	record: ReturnType<typeof appendAttempt>,
): void {
	const binding = database.bindings.getSessionRecord(runtime.sessionKey);
	database.work.workAttemptPrepare(
		tx,
		runtime,
		record,
		runtime.mode === "historical" || (binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch),
	);
	touchWorkSession(database, tx, runtime, runtime.startedAt);
}

function updateAttempt(
	database: Store,
	tx: WriteTx,
	opRef: string,
	expectedVersion: number,
	patch: WorkAttemptPatch,
): WorkAttemptRuntime | undefined {
	const current = database.work.workAttemptGet(opRef);
	const updated = database.work.workAttemptUpdate(tx, opRef, expectedVersion, patch);
	if (current && updated && !workAccepted(current) && workAccepted(updated) && updated.noticeHash !== null)
		database.core.metaSet(
			`lane-notice:${updated.sessionKey}`,
			JSON.stringify({ epoch: updated.epoch, hash: updated.noticeHash }),
		);
	return updated;
}

function settleAttempt(
	database: Store,
	tx: WriteTx,
	opRef: string,
	expectedVersion: number,
	record: ReturnType<typeof closeAttempt>,
	patch: WorkAttemptSettlement,
	admission?: WorkAttemptAdmission,
) {
	const checks = {
		...(admission?.kind === "persona"
			? {
					personaSessionExists: database.bindings.getSessionRecord(admission.row.originKey) !== undefined,
					personaMessageExists: database.inbox.messageExists(admission.row.messageId),
				}
			: {}),
		...(admission?.kind === "lane"
			? { laneParentAvailable: database.work.laneParentAvailable(admission.report.parentName) }
			: {}),
	};
	const result = database.work.workAttemptSettle(tx, opRef, expectedVersion, record, patch, admission, checks);
	if (!result) return undefined;
	touchWorkSession(database, tx, result.runtime, result.runtime.settledAt!);
	if (result.runtime.decision === "reported" && admission?.kind === "persona") {
		if (
			!database.inbox.enqueue(tx, {
				messageId: admission.row.messageId,
				originKey: admission.row.originKey,
				originRefJson: resolveLaneReportOrigin(result.runtime.parent, admission.row.originRefJson),
				body: admission.row.body,
				receivedAt: result.runtime.settledAt!,
				source: "lane_report",
			})
		)
			throw new Error("lane report inbound conflict");
	}
	for (const payload of [result.fallbackPayload, result.childFallback]) {
		if (!payload) continue;
		database.outbox.enqueue(tx, {
			deliveryId: payload.deliveryId!,
			turnId: payload.turnId,
			originKey: originKey(payload.origin),
			payloadJson: JSON.stringify(payload),
		});
	}
	return result;
}
function seedLinkedWakeReport(
	f: Awaited<ReturnType<typeof fixture>>,
	root: { readonly originKey: string; readonly origin: OriginRef } | null,
): string {
	const childName = "child";
	const childOpRef = "gw-child-op";
	const childJobId = `lanejob-${Buffer.from(childName, "utf8").toString("hex")}`;
	const reportId = workAttemptReportId(f.database.instanceId, childJobId, childOpRef);
	f.raw
		.query(
			"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, 'a', ?, ?, ?, ?, 'claimed', 'wake', ?, NULL, 1, NULL, NULL, ?, ?)",
		)
		.run(
			reportId,
			childName,
			childOpRef,
			"[lane child] completed: child output",
			root === null ? null : JSON.stringify(root),
			f.runtime.opRef,
			START,
			START,
		);
	return reportId;
}

describe("FirstMate immutable store attribution", () => {
	test("native prepare records exact owner input and native identities, with immutable rows", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		expect(f.owner.assignment("a")).toEqual(f.assignment);
		expect(f.owner.initial(f.assignment.taskId)).toEqual({
			opRef: f.runtime.opRef,
			taskId: f.assignment.taskId,
			jobId: f.runtime.jobId,
			sessionId: f.runtime.sessionId,
			epoch: f.runtime.epoch,
			ordinal: 0,
			cause: { kind: "assignment" },
			text: f.assignment.mandate.text,
			preparedAt: START,
		});
		expect(f.owner.attempt(f.runtime.opRef)).toEqual(f.owner.initial(f.assignment.taskId));
		expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
		expect(f.database.work.laneJobJson(f.record.jobId)).toBe(JSON.stringify(f.record));
		expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
		expect(f.raw.query("SELECT entity, event_type FROM work_journal ORDER BY seq").all()).toEqual([
			{ entity: "lane_job", event_type: "lane_job_running" },
			{ entity: "attempt", event_type: "attempt_started" },
		]);
		const before = firstmateRows(f.raw);
		expect(f.raw.query<{ table: string }, []>("PRAGMA foreign_key_list(firstmate_assignments)").all()).toEqual([]);
		expect(
			f.raw
				.query<{ table: string }, []>("PRAGMA foreign_key_list(firstmate_attempts)")
				.all()
				.map((row) => row.table),
		).toEqual(["firstmate_assignments"]);
		for (const table of ["firstmate_assignments", "firstmate_attempts"]) {
			expect(() => f.raw.exec(`UPDATE ${table} SET task_id = task_id`)).toThrow("immutable FirstMate evidence");
			expect(() => f.raw.exec(`DELETE FROM ${table}`)).toThrow("immutable FirstMate evidence");
		}
		expect(firstmateRows(f.raw)).toEqual(before);
	});

	test("conflicting policy, mapped historical prepare and canonical worktree aliases cannot bypass attribution", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		const before = prepareState(f);
		for (const assignment of [
			{ ...f.assignment, taskId: "another-task" },
			{ ...f.assignment, ownerId: "another-owner" },
			{ ...f.assignment, name: "another-lane" },
			{ ...f.assignment, mandate: { ...f.assignment.mandate, push: true } },
			{ ...f.assignment, thread: { ...f.assignment.thread, conversationId: "another-thread" } },
		]) {
			expect(() => f.owner.assertAssignment(assignment)).toThrow("FirstMate admission refused");
		}
		const alias = join(f.directory, "alias");
		await symlink(f.assignment.worktree.path, alias);
		expect(() => f.owner.assertUnmapped("a")).toThrow("FirstMate admission refused");
		expect(() => f.owner.assertUnmapped("other", alias)).toThrow("FirstMate admission refused");
		const historical = { ...f.runtime, mode: "historical" as const, sendPhase: "uncertain" as const, parent: null };
		expect(() => f.database.transaction((tx) => f.database.work.workAttemptPrepare(tx, historical, f.record))).toThrow(
			"FirstMate admission refused",
		);
		expect(prepareState(f)).toEqual(before);
		expect(() => f.owner.assertUnmapped("other", f.directory)).not.toThrow();
	});

	test("invalid input, missing initial evidence and native binding refusal leave no partial attribution", async () => {
		const f = await firstmateFixture();
		const before = prepareState(f);
		const checks = {
			kind: "checks" as const,
			priorOpRef: "gw-missing-prior",
			prNumber: 7,
			headSha: "a".repeat(40),
			failingChecks: ["unit"],
			retryOrdinal: 1,
		};
		for (const evidence of [
			{ ...f.evidence, text: "changed input" },
			{ ...f.evidence, cause: checks },
			{ ...f.evidence, assignment: { ...f.assignment, worktree: { ...f.assignment.worktree, branch: "wrong" } } },
		]) {
			expect(() =>
				f.database.transaction((tx) => f.database.work.workAttemptPrepare(tx, f.runtime, f.record, true, evidence)),
			).toThrow("FirstMate admission refused");
			expect(prepareState(f)).toEqual(before);
		}
		expect(() =>
			f.database.transaction((tx) => f.database.work.workAttemptPrepare(tx, f.runtime, f.record, false, f.evidence)),
		).toThrow(WorkAttemptStateError);
		expect(prepareState(f)).toEqual(before);
		prepareFirstmate(f);
	});

	test("lost native projections do not authorize replacing a reserved assignment with a new initial attempt", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		// Scratch corruption models missing owner evidence; the production DELETE guard is tested separately.
		f.raw.exec(
			"DROP TRIGGER firstmate_attempts_immutable_delete; DELETE FROM firstmate_attempts; DELETE FROM work_attempt_runtime; DELETE FROM lane_jobs;",
		);
		const before = prepareState(f);
		expect(f.owner.initial(f.assignment.taskId)).toBeUndefined();
		expect(() => prepareFirstmate(f)).toThrow("FirstMate admission refused");
		expect(prepareState(f)).toEqual(before);
		// Projection repair must not recreate the missing original input or infer owner authority.
		expect(f.database.replayJournal()).toEqual({ attempts: 1, laneJobs: 1, deliveries: 0 });
		expect(f.owner.assignment("a")).toEqual(f.assignment);
		expect(f.owner.initial(f.assignment.taskId)).toBeUndefined();
		expect(f.owner.attempt(f.runtime.opRef)).toBeUndefined();
	});

	for (const [point, target] of [
		["assignment", "INSERT ON firstmate_assignments"],
		["attribution", "INSERT ON firstmate_attempts"],
		["lane", "INSERT ON lane_jobs"],
		["lane journal", "INSERT ON work_journal WHEN NEW.entity = 'lane_job'"],
		["runtime", "INSERT ON work_attempt_runtime"],
		["attempt journal", "INSERT ON work_journal WHEN NEW.entity = 'attempt'"],
		["activity", "UPDATE ON sessions"],
	]) {
		test(`failure after ${point} rolls back evidence, native projections, journal and activity`, async () => {
			const f = await firstmateFixture();
			const before = prepareState(f);
			f.raw.exec(`CREATE TRIGGER firstmate_fault AFTER ${target} BEGIN SELECT RAISE(ABORT, 'firstmate fault'); END`);
			expect(() => prepareFirstmate(f)).toThrow("firstmate fault");
			expect(prepareState(f)).toEqual(before);
			f.raw.exec("DROP TRIGGER firstmate_fault");
			prepareFirstmate(f);
			expect(f.owner.initial(f.assignment.taskId)?.opRef).toBe(f.runtime.opRef);
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
		});
	}

	test("check attribution retains predecessor and input while actual ordinal differs from spent retry budget", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		const initial = f.owner.initial(f.assignment.taskId);
		const closed = closeAttempt({ record: f.record, opRef: f.runtime.opRef, endState: "completed", endedAt: END });
		// Seed native receipt state through store primitives, not a transport/send proof.
		expect(
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, f.runtime.opRef, 0, {
					sendPhase: "accepted",
					sendEvidence: { source: "receipt", observedAt: START },
				}),
			)?.version,
		).toBe(1);
		const payload = buildDeliveryPayload(
			f.runtime.opRef,
			f.assignment.thread,
			"store fixture result",
			f.runtime.deliveryId,
		)!;
		const admission: WorkAttemptAdmission = {
			kind: "persona",
			row: {
				messageId: f.runtime.reportId,
				originKey: originKey(f.assignment.thread),
				originRefJson: JSON.stringify(f.assignment.thread),
				body: payload.text,
				receivedAt: END,
			},
			fallbackPayload: payload,
		};
		expect(
			f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 1, closed, f.settlement, admission))
				?.runtime.settledAt,
		).toBe(END);
		const waiting = transitionToAwaitingChecks({ record: closed, at: END, prNumber: 7, headSha: "a".repeat(40) });
		// A failed bind can spend native budget without adding an attempt. This tests store lineage only.
		const spent = recordChecksRetry(recordChecksRetry(waiting, END), END);
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				...spent,
				laneKey: f.runtime.laneKey,
				json: JSON.stringify(spent),
			}),
		);
		const opRef = "gw-firstmate-checks";
		const record = appendAttempt(spent, { opRef, sessionId: SESSION, startedAt: END });
		const runtime: WorkAttemptRuntime = {
			...f.runtime,
			opRef,
			startedAt: END,
			reportId: workAttemptReportId(f.database.instanceId, f.runtime.jobId, opRef),
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
		};
		const cause = {
			kind: "checks" as const,
			priorOpRef: f.runtime.opRef,
			prNumber: 7,
			headSha: "a".repeat(40),
			failingChecks: ["unit", "lint"],
			retryOrdinal: 2,
		};
		const evidence: FirstmatePrepare = { assignment: f.assignment, cause, text: "Repair these checks only.\n한글\n" };
		const before = prepareState(f);
		for (const invalid of [
			{ ...cause, priorOpRef: "gw-wrong-prior" },
			{ ...cause, retryOrdinal: 1 },
			{ ...cause, headSha: "b".repeat(40) },
			{ ...cause, prNumber: 8 },
		]) {
			expect(() =>
				f.database.transaction((tx) =>
					f.database.work.workAttemptPrepare(tx, runtime, record, true, { ...evidence, cause: invalid }),
				),
			).toThrow("FirstMate admission refused");
			expect(prepareState(f)).toEqual(before);
		}
		f.database.transaction((tx) => {
			f.database.work.workAttemptPrepare(tx, runtime, record, true, evidence);
			touchWorkSession(f.database, tx, runtime, END);
		});
		expect(f.owner.attempt(opRef)).toEqual({
			opRef,
			taskId: f.assignment.taskId,
			jobId: runtime.jobId,
			sessionId: SESSION,
			epoch: runtime.epoch,
			ordinal: 1,
			cause,
			text: evidence.text,
			preparedAt: END,
		});
		expect(f.owner.initial(f.assignment.taskId)).toEqual(initial);
		expect(f.owner.assignment("a")?.mandate).toEqual(f.assignment.mandate);
		expect(f.database.work.workAttemptGet(opRef)).toEqual(runtime);
	});

	for (const state of ["attempt_ended", "done"] as const) {
		test(`owner correction from settled ${state} appends exact input without check authority or changing prior evidence`, async () => {
			const f = await correctionFixture(false, state);
			const c = f.correction;
			const before = prepareState(f);
			const prior = f.database.work.workAttemptGet(f.runtime.opRef);
			const persisted = parseLaneJobRecord(before.work.laneJobs[0]!.record_json);
			expect(persisted).toEqual(f.settled);
			expect(persisted.state).toBe(state);
			expect(persisted.awaitingChecks).toBeUndefined();
			expect(persisted.attempts.at(-1)?.endState).toBe("completed");
			expect(prior?.sendPhase).toBe("accepted");
			expect(prior?.settledAt).toBe(END);
			expect(prior?.terminal?.reasonCode).toBe("end_turn");
			expect(f.owner.correction(f.assignment.taskId, c.cause, c.evidence.text)).toBeUndefined();
			expect(() =>
				c.prepare({
					...c.evidence,
					cause: {
						kind: "checks",
						priorOpRef: f.runtime.opRef,
						prNumber: 7,
						headSha: "a".repeat(40),
						failingChecks: ["unit"],
						retryOrdinal: 1,
					},
				}),
			).toThrow("FirstMate admission refused");
			expect(prepareState(f)).toEqual(before);
			c.prepare();
			expect(f.owner.attempt(c.runtime.opRef)).toEqual({
				opRef: c.runtime.opRef,
				taskId: f.assignment.taskId,
				jobId: c.runtime.jobId,
				sessionId: c.runtime.sessionId,
				epoch: c.runtime.epoch,
				ordinal: 1,
				cause: c.cause,
				text: c.evidence.text,
				preparedAt: END,
			});
			expect(f.owner.assignment("a")).toEqual(f.assignment);
			expect(f.owner.assignment("a")?.mandate.checkRetries).toBe(false);
			expect(firstmateRows(f.raw).assignments).toEqual(before.evidence.assignments);
			expect(firstmateRows(f.raw).attempts).toEqual(expect.arrayContaining(before.evidence.attempts));
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(prior);
			expect(f.database.work.projectionRows().laneJobs[0]?.record_json).toBe(JSON.stringify(c.record));
			expect(f.raw.query("SELECT * FROM work_journal ORDER BY seq").all()).toEqual(
				expect.arrayContaining(before.journal),
			);
			expect(f.owner.correction(f.assignment.taskId, { ...c.cause }, c.evidence.text)).toEqual(
				f.owner.attempt(c.runtime.opRef),
			);
			const after = prepareState(f);
			expect(() => c.prepare()).toThrow();
			expect(prepareState(f)).toEqual(after);
		});
	}

	test("correction duplicate lookup rejects changed cause or text and does not reserve another message", async () => {
		const f = await correctionFixture();
		const c = f.correction;
		c.prepare();
		const before = prepareState(f);
		for (const cause of [
			{ ...c.cause, priorOpRef: "gw-another-predecessor" },
			{ ...c.cause, ownerId: "other-owner" },
			{ ...c.cause, mandateId: "other-mandate" },
		]) {
			expect(() => f.owner.correction(f.assignment.taskId, cause, c.evidence.text)).toThrow(
				"FirstMate admission refused",
			);
		}
		expect(() => f.owner.correction(f.assignment.taskId, c.cause, `${c.evidence.text} `)).toThrow(
			"FirstMate admission refused",
		);
		expect(() => f.owner.correction("missing-task", c.cause, c.evidence.text)).toThrow("FirstMate admission refused");
		expect(
			f.owner.correction(f.assignment.taskId, { ...c.cause, messageId: "another-message" }, c.evidence.text),
		).toBeUndefined();
		expect(prepareState(f)).toEqual(before);
	});

	test("invalid correction authority and predecessor leave no attribution or native writes", async () => {
		const f = await correctionFixture();
		const c = f.correction;
		const before = prepareState(f);
		for (const cause of [
			{ ...c.cause, ownerId: "other-owner" },
			{ ...c.cause, mandateId: "other-mandate" },
			{ ...c.cause, messageId: f.assignment.messageId },
			{ ...c.cause, messageId: "" },
			{ ...c.cause, priorOpRef: "gw-wrong-prior" },
		]) {
			expect(() => c.prepare({ ...c.evidence, cause })).toThrow("FirstMate admission refused");
			expect(prepareState(f)).toEqual(before);
		}
		expect(() => c.prepare({ ...c.evidence, text: "" })).toThrow("FirstMate admission refused");
		expect(() =>
			c.prepare({ ...c.evidence, assignment: { ...f.assignment, mandate: { ...f.assignment.mandate, push: true } } }),
		).toThrow("FirstMate admission refused");
		expect(() => c.prepare(c.evidence, { ...c.record, state: "done" })).toThrow("FirstMate admission refused");
		expect(() =>
			c.prepare(c.evidence, { ...c.record, stalledContinuations: c.record.stalledContinuations + 1 }),
		).toThrow("FirstMate admission refused");
		expect(() => c.prepare(c.evidence, c.record, { ...c.runtime, epoch: c.runtime.epoch + 1 })).toThrow();
		expect(prepareState(f)).toEqual(before);
		c.prepare();
	});

	for (const state of ["awaiting_operator", "stalled", "aborted", "running"] as const) {
		test(`correction refuses persisted ${state} even with a supplied reopened job`, async () => {
			const f = await correctionFixture();
			const native = { ...f.settled, state };
			f.database.transaction((tx) =>
				f.database.work.putLaneJob(tx, { ...native, laneKey: f.runtime.laneKey, json: JSON.stringify(native) }),
			);
			const before = prepareState(f);
			expect(() => f.correction.prepare()).toThrow("FirstMate admission refused");
			expect(prepareState(f)).toEqual(before);
		});
	}

	test("attempt_ended correction refuses pending checks and missing, unaccepted, non-end_turn or unsettled predecessor", async () => {
		const f = await correctionFixture();
		expect(f.settled.state).toBe("attempt_ended");
		const waiting = transitionToAwaitingChecks({ record: f.settled, at: END, prNumber: 7, headSha: "a".repeat(40) });
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...waiting, laneKey: f.runtime.laneKey, json: JSON.stringify(waiting) }),
		);
		let before = prepareState(f);
		expect(() => f.correction.prepare()).toThrow("FirstMate admission refused");
		expect(prepareState(f)).toEqual(before);
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...f.settled, laneKey: f.runtime.laneKey, json: JSON.stringify(f.settled) }),
		);
		const prior = f.database.work.workAttemptGet(f.runtime.opRef)!;
		// Scratch projection corruption; immutable attribution is deliberately left intact.
		for (const changed of [
			{ ...prior, sendPhase: "prepared", sendEvidence: null },
			{ ...prior, terminal: { ...prior.terminal!, reasonCode: "session_dead" } },
			{ ...prior, settledAt: null },
			{ ...prior, epoch: prior.epoch + 1 },
		]) {
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
				.run(JSON.stringify(changed), f.runtime.opRef);
			before = prepareState(f);
			// The native validated getter refuses a corrupt row before FirstMate policy sees it.
			expect(() => f.correction.prepare()).toThrow(/FirstMate admission refused|work lane state unavailable/);
			expect(prepareState(f)).toEqual(before);
		}
		f.raw.query("DELETE FROM work_attempt_runtime WHERE op_ref = ?").run(f.runtime.opRef);
		before = prepareState(f);
		expect(() => f.correction.prepare()).toThrow("FirstMate admission refused");
		expect(prepareState(f)).toEqual(before);
	});

	for (const [point, target] of [
		["attribution", "INSERT ON firstmate_attempts"],
		["lane", "UPDATE ON lane_jobs"],
		["lane journal", "INSERT ON work_journal WHEN NEW.entity = 'lane_job'"],
		["runtime", "INSERT ON work_attempt_runtime"],
		["runtime journal", "INSERT ON work_journal WHEN NEW.entity = 'attempt'"],
	] as const) {
		test(`correction failure after ${point} rolls back without orphan rows or reopening`, async () => {
			const f = await correctionFixture();
			const before = prepareState(f);
			f.raw.exec(`CREATE TRIGGER correction_fault AFTER ${target} BEGIN SELECT RAISE(ABORT, 'correction fault'); END`);
			expect(() => f.correction.prepare()).toThrow("correction fault");
			expect(prepareState(f)).toEqual(before);
			expect(f.owner.correction(f.assignment.taskId, f.correction.cause, f.correction.evidence.text)).toBeUndefined();
			f.raw.exec("DROP TRIGGER correction_fault");
			f.correction.prepare();
			expect(f.owner.attempt(f.correction.runtime.opRef)?.ordinal).toBe(1);
		});
	}

	test("checks retries remain a separate cause after owner correction", async () => {
		const f = await correctionFixture(true);
		const c = f.correction;
		c.prepare();
		const closed = closeAttempt({ record: c.record, opRef: c.runtime.opRef, endState: "completed", endedAt: END });
		f.database.transaction((tx) =>
			updateAttempt(f.database, tx, c.runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: END },
			}),
		);
		const payload = buildDeliveryPayload(
			c.runtime.opRef,
			f.assignment.thread,
			"corrected result",
			c.runtime.deliveryId,
		)!;
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, c.runtime.opRef, 1, closed, f.settlement, {
				kind: "persona",
				row: {
					messageId: c.runtime.reportId,
					originKey: originKey(f.assignment.thread),
					originRefJson: JSON.stringify(f.assignment.thread),
					body: payload.text,
					receivedAt: END,
				},
				fallbackPayload: payload,
			}),
		);
		const spent = recordChecksRetry(
			recordChecksRetry(
				transitionToAwaitingChecks({ record: closed, at: END, prNumber: 7, headSha: "a".repeat(40) }),
				END,
			),
			END,
		);
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...spent, laneKey: c.runtime.laneKey, json: JSON.stringify(spent) }),
		);
		const opRef = "gw-checks-after-correction";
		const record = appendAttempt(spent, { opRef, sessionId: SESSION, startedAt: END });
		const runtime = {
			...c.runtime,
			opRef,
			reportId: workAttemptReportId(f.database.instanceId, c.runtime.jobId, opRef),
			deliveryId: workAttemptDeliveryId(f.database.instanceId, c.runtime.jobId, opRef),
		};
		const evidence: FirstmatePrepare = {
			assignment: f.assignment,
			cause: {
				kind: "checks",
				priorOpRef: c.runtime.opRef,
				prNumber: 7,
				headSha: "a".repeat(40),
				failingChecks: ["unit"],
				retryOrdinal: 2,
			},
			text: "Repair only failed checks",
		};
		const before = firstmateRows(f.raw);
		f.database.transaction((tx) => f.database.work.workAttemptPrepare(tx, runtime, record, true, evidence));
		expect(f.owner.attempt(opRef)?.ordinal).toBe(2);
		expect(f.owner.attempt(opRef)?.cause).toEqual(evidence.cause);
		expect(firstmateRows(f.raw).attempts).toEqual(expect.arrayContaining(before.attempts));
		expect(f.owner.correction(f.assignment.taskId, c.cause, c.evidence.text)?.opRef).toBe(c.runtime.opRef);
	});

	test("projection wipe and repeated replay preserve attribution bytes and every native identity", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		f.database.transaction((tx) =>
			f.database.outbox.enqueue(tx, {
				deliveryId: f.runtime.deliveryId,
				turnId: f.runtime.opRef,
				originKey: originKey(f.assignment.thread),
				payloadJson: JSON.stringify({ text: "retained obligation" }),
			}),
		);
		f.database.transaction((tx) => f.database.outbox.fail(tx, f.runtime.deliveryId, true, "socket hang up"));
		const before = prepareState(f);
		f.raw.exec("DELETE FROM work_attempt_runtime; DELETE FROM lane_jobs; DELETE FROM deliveries;");
		expect(firstmateRows(f.raw)).toEqual(before.evidence);
		expect(f.database.replayJournal()).toEqual({ attempts: 1, laneJobs: 1, deliveries: 1 });
		expect(prepareState(f)).toEqual(before);
		// Counts alone could hide delete/reinsert churn; forbid every native rewrite on the second replay.
		for (const table of ["work_attempt_runtime", "lane_jobs", "deliveries"]) {
			for (const action of ["INSERT", "UPDATE", "DELETE"]) {
				f.raw.exec(
					`CREATE TRIGGER no_rewrite_${table}_${action} BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'unexpected replay rewrite'); END`,
				);
			}
		}
		expect(f.database.replayJournal()).toEqual({ attempts: 0, laneJobs: 0, deliveries: 0 });
		expect(prepareState(f)).toEqual(before);
		expect(f.owner.attempt(f.runtime.opRef)?.sessionId).toBe(SESSION);
	});

	test("outbox replay failure rolls back earlier work repairs without changing attribution or journal", async () => {
		const f = await firstmateFixture();
		prepareFirstmate(f);
		f.database.transaction((tx) =>
			f.database.outbox.enqueue(tx, {
				deliveryId: f.runtime.deliveryId,
				turnId: f.runtime.opRef,
				originKey: originKey(f.assignment.thread),
				payloadJson: JSON.stringify({ text: "retained obligation" }),
			}),
		);
		const live = prepareState(f);
		f.raw.exec("DELETE FROM work_attempt_runtime; DELETE FROM lane_jobs; DELETE FROM deliveries;");
		const wiped = prepareState(f);
		f.raw.exec(`CREATE TRIGGER replay_fault AFTER INSERT ON deliveries BEGIN
			SELECT CASE WHEN (SELECT COUNT(*) FROM work_attempt_runtime) = 1 AND (SELECT COUNT(*) FROM lane_jobs) = 1
				THEN RAISE(ABORT, 'outbox replay fault after work repair')
				ELSE RAISE(ABORT, 'work repair missing before outbox') END;
		END`);
		expect(() => f.database.replayJournal()).toThrow("outbox replay fault after work repair");
		expect(prepareState(f)).toEqual(wiped);
		expect(firstmateRows(f.raw)).toEqual(live.evidence);
		f.raw.exec("DROP TRIGGER replay_fault");
		expect(f.database.replayJournal()).toEqual({ attempts: 1, laneJobs: 1, deliveries: 1 });
		expect(prepareState(f)).toEqual(live);
	});
});

describe("work attempt durable transactions", () => {
	for (const mode of ["start", "run"] as const) {
		test(`${mode} publishes worker metadata and activity atomically at prepare and settlement`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, parent: mode === "start" ? f.runtime.parent : null };
			const settlement = { ...f.settlement, decision: mode === "start" ? ("report" as const) : ("no_target" as const) };
			const admission = mode === "start" ? f.admission : undefined;
			const expectedDecision = mode === "start" ? "fallback" : "no_target";
			const identity = () =>
				f.database.bindings.sessionIdentityRows().find((row) => row.origin_key === runtime.sessionKey);
			const workerOrigin = JSON.stringify({ platform: "work", kind: "task", conversationId: "a" });
			// An AFTER fault proves both assignments roll back even after the UPDATE executes.
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record))).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: null });
			expect(f.database.work.workAttemptGet(runtime.opRef)).toBeUndefined();
			expect(f.database.work.laneJobJson(runtime.jobId)).toBeUndefined();
			f.raw.exec("DROP TRIGGER fault");
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: START });
			// Simulate missing legacy metadata to prove settlement restores it too.
			f.raw.query("UPDATE sessions SET origin_ref_json = NULL WHERE origin_key = ?").run(runtime.sessionKey);
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() =>
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission),
				),
			).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: START });
			expect(f.database.work.workAttemptGet(runtime.opRef)?.decision).toBe("undecided");
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(
				f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission))
					?.runtime.decision,
			).toBe(expectedDecision);
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: END });
		});

		test(`${mode} stale epoch settlement cannot retag or refresh a successor binding`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, parent: mode === "start" ? f.runtime.parent : null };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const successorOrigin = JSON.stringify(LOOPBACK_ORIGIN);
			// Keep sessionId identical: the epoch predicate must independently fence the write.
			f.raw
				.query("UPDATE sessions SET epoch = epoch + 1, origin_ref_json = ?, last_activity_at = ? WHERE origin_key = ?")
				.run(successorOrigin, START, runtime.sessionKey);
			const before = f.database.bindings.sessionIdentityRows();
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record))).toThrow();
			const requested = mode === "start" ? ("report" as const) : ("no_target" as const);
			const expected = mode === "start" ? "fallback" : "no_target";
			expect(
				f.database.transaction((tx) =>
					settleAttempt(
						f.database,
						tx,
						runtime.opRef,
						0,
						f.closed,
						{ ...f.settlement, decision: requested },
						mode === "start" ? f.admission : undefined,
					),
				)?.runtime.decision,
			).toBe(expected);
			expect(f.database.bindings.sessionIdentityRows()).toEqual(before);
		});
	}
	for (const table of ["lane_jobs", "work_attempt_runtime", "sessions"]) {
		test(`prepare rolls back history/runtime/activity when ${table} write fails`, async () => {
			const f = await fixture();
			const action = table === "sessions" ? "UPDATE" : "INSERT";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record))).toThrow();
			expect(f.database.work.laneJobJson(f.runtime.jobId)).toBeUndefined();
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toBeUndefined();
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBeNull();
			f.raw.exec("DROP TRIGGER fault");
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
		});
	}

	for (const table of ["work_attempt_runtime", "lane_jobs", "sessions", "deliveries"]) {
		test(`settlement rolls back all effects when ${table} write fails`, async () => {
			const f = await fixture();
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			const action = table === "deliveries" ? "INSERT" : "UPDATE";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() =>
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
				),
			).toThrow();
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(parseLaneJobRecord(f.database.work.laneJobJson(f.runtime.jobId)!).attempts[0]?.endedAt).toBeUndefined();
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
				)?.runtime.decision,
			).toBe("fallback");
			expect(f.database.outbox.rows()).toHaveLength(1);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(END);
		});
	}

	test("CAS refuses stale settlement and duplicate decisions; pruning does not regenerate", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: START },
			}),
		);
		expect(accepted?.version).toBe(1);
		expect(
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toBeUndefined();
		expect(f.database.outbox.rows()).toHaveLength(0);
		expect(
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 1, f.closed, f.settlement, f.admission),
			)?.runtime.version,
		).toBe(2);
		const ledger = new DeliveryService(f.database);
		expect(ledger.confirm(f.runtime.deliveryId)).toBe("transitioned");
		expect(ledger.prune(0, Date.now() + 1000)).toBe(1);
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(
			reopened.transaction((tx) =>
				settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, f.settlement, f.admission),
			),
		).toBeUndefined();
		expect(reopened.transaction((tx) => updateAttempt(reopened, tx, f.runtime.opRef, 2, {}))).toBeUndefined();
		expect(reopened.work.workAttemptGet(f.runtime.opRef)?.decision).toBe("fallback");
		expect(reopened.outbox.rows()).toHaveLength(0);
	});

	test("duplicate op and different open attempt cannot rewrite lane history", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record))).toThrow();
		const opRef = "gw-work-store-b";
		const other = {
			...f.runtime,
			opRef,
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
		};
		const record = { ...f.record, attempts: [{ ...f.record.attempts[0]!, opRef }] };
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, other, record))).toThrow();
		expect(f.database.work.workAttemptOpen()).toEqual([f.runtime]);
		expect(f.database.work.workAttemptOpen(1, f.runtime.opRef)).toEqual([]);
		expect(() => f.database.work.workAttemptOpen(1001)).toThrow();
		// SQL also protects against a second writer bypassing the accessor.
		expect(() =>
			f.raw
				.query(
					"INSERT INTO work_attempt_runtime SELECT ?, job_id, lane_key, session_id, version, settled_at, ?, record_json FROM work_attempt_runtime",
				)
				.run(opRef, other.deliveryId),
		).toThrow();
	});

	test("conflicting deterministic ledger identity aborts the entire settlement", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		new DeliveryService(f.database).prepare(
			"other-turn",
			LOOPBACK_ORIGIN,
			"other obligation",
			undefined,
			f.runtime.deliveryId,
		);
		expect(() =>
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toThrow();
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.decision).toBe("undecided");
		expect(f.database.outbox.rows()[0]?.turn_id).toBe("other-turn");
	});

	test("same-transaction delivery seam requires ownership, suppresses silence and checks duplicate payloads", async () => {
		const f = await fixture();
		const delivery = new DeliveryService(f.database);
		expect(() => delivery.persistInTransaction(undefined as never, f.payload)).toThrow("inactive_tx");
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(true);
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(false);
		expect(() =>
			f.database.transaction((tx) => delivery.persistInTransaction(tx, { ...f.payload, text: "changed" })),
		).toThrow();
		expect(buildDeliveryPayload("turn", LOOPBACK_ORIGIN, "NO_REPLY", "silent")).toBeUndefined();
	});

	test("known silence survives reopen and suppresses exactly once", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const proof = {
			opRef: f.runtime.opRef,
			sessionId: SESSION,
			epoch: 0,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: f.runtime.opRef,
			repo: "/work",
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: 8,
		};
		const output = { ...f.runtime.output, disposition: "silent" as const, knownSilence: proof, proof, excerpt: null };
		expect(f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output }))?.version).toBe(
			1,
		);
		expect(
			f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output })),
		).toBeUndefined();
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(reopened.work.workAttemptGet(f.runtime.opRef)?.output.knownSilence).toEqual(proof);
		expect(() =>
			reopened.transaction((tx) => updateAttempt(reopened, tx, f.runtime.opRef, 1, { output: f.runtime.output })),
		).toThrow();
		const suppressed = { ...f.settlement, output, decision: "suppressed" as const };
		expect(
			reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 1, f.closed, suppressed))?.runtime
				.decision,
		).toBe("suppressed");
		expect(
			reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, suppressed)),
		).toBeUndefined();
		expect(reopened.outbox.rows()).toHaveLength(0);
	});

	test("output claims persist three-read budget; stale/wrong proof and oversized excerpt fail closed", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		let runtime = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: f.settlement.terminal }),
		)!;
		for (let reads = 1; reads <= 3; reads++)
			runtime = f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					output: { ...runtime.output, reads, nextReadAt: END },
				}),
			)!;
		expect(runtime.output.reads).toBe(3);
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 4 } }),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 0 } }),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					output: { ...runtime.output, excerpt: "界".repeat(1000) },
				}),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					output: {
						...runtime.output,
						knownSilence: {
							opRef: "gw-other",
							sessionId: SESSION,
							epoch: 0,
							observedAtMs: Date.parse(END),
							source: "turn.result",
							attribution: "operation_ref",
							fullness: "original",
							clientRef: runtime.opRef,
							repo: "/work",
							terminalAt: Date.parse(END),
							contentVersion: 1,
							byteLength: 8,
						},
					},
				}),
			),
		).toThrow();
	});

	test("terminal evidence is write-once except a proven missing-to-present receipt (#248)", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const missing = {
			kind: "broker" as const,
			observedAt: END,
			reasonCode: "terminal_missing_receipt",
			status: {
				status: "terminal_ok" as const,
				receiptState: "missing" as const,
				clientRef: f.runtime.opRef,
				terminalAt: Date.parse(END),
				outcome: { reason: "end_turn" },
			},
		};
		const present = {
			...missing,
			reasonCode: "end_turn",
			status: { ...missing.status, receiptState: "present" as const },
		};
		const runtime = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: missing }),
		)!;
		const proof = {
			opRef: f.runtime.opRef,
			sessionId: SESSION,
			epoch: 0,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: f.runtime.opRef,
			repo: "/work",
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: 8,
		};
		const available = { ...runtime.output, disposition: "available" as const, excerpt: "PR ready", proof };
		// No proven body: the receipt cannot be upgraded.
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { terminal: present }),
			),
		).toThrow();
		// Any other terminal rewrite remains forbidden, even with a proven body.
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					terminal: { ...present, status: { ...present.status, outcome: { reason: "refusal" } } },
					output: available,
				}),
			),
		).toThrow();
		const upgraded = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				terminal: present,
				output: available,
			}),
		)!;
		expect(upgraded.terminal).toEqual(present);
		// present never regresses to missing.
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, upgraded.opRef, upgraded.version, { terminal: missing }),
			),
		).toThrow();
	});

	test("corrupt runtime projection or history throws rather than returning empty", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw.query("UPDATE work_attempt_runtime SET version = 8 WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.work.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(() => f.database.work.workAttemptOpen()).toThrow(WorkAttemptStateError);
		f.raw.query("UPDATE work_attempt_runtime SET version = 0, record_json = '{' WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.work.workAttemptOpenByLane(f.runtime.laneKey)).toThrow(WorkAttemptStateError);
	});

	test("run is response-only and final decisions cannot bypass settlement", async () => {
		const f = await fixture();
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, mode: "run" }, f.record)),
		).toThrow();
		const runtime = { ...f.runtime, mode: "run" as const, parent: null };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, f.settlement)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toThrow();
		const patch = { ...f.settlement, decision: "no_target" as const };
		expect(() =>
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch, f.admission)),
		).toThrow();
		expect(
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch))?.runtime
				.decision,
		).toBe("no_target");
		expect(f.database.outbox.rows()).toHaveLength(0);
	});

	test("wake_unaccepted validator rejects fabricated upstream decisions", async () => {
		for (const decision of ["reported", "fallback", "no_target", "suppressed"] as const) {
			const f = await fixture();
			const linkedId = seedLinkedWakeReport(f, {
				originKey: originKey(LOOPBACK_ORIGIN),
				origin: LOOPBACK_ORIGIN,
			});
			const runtime = { ...f.runtime, wakeReportId: linkedId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const terminal = { kind: "local" as const, observedAt: END, reasonCode: "session_dead" };
			const invalid = {
				...runtime,
				parent: decision === "no_target" ? null : runtime.parent,
				version: 1,
				terminal,
				output: { ...runtime.output, disposition: "unavailable" as const },
				decision,
				settledAt: END,
			};
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = ?, version = 1, settled_at = ? WHERE op_ref = ?")
				.run(JSON.stringify(invalid), END, runtime.opRef);
			expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		}

		const noLink = await fixture();
		noLink.database.transaction((tx) => prepareAttempt(noLink.database, tx, noLink.runtime, noLink.record));
		const noLinkInvalid = {
			...noLink.runtime,
			version: 1,
			terminal: { kind: "local" as const, observedAt: END, reasonCode: "session_dead" },
			output: { ...noLink.runtime.output, disposition: "unavailable" as const },
			decision: "wake_unaccepted" as const,
			settledAt: END,
		};
		noLink.raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, version = 1, settled_at = ? WHERE op_ref = ?")
			.run(JSON.stringify(noLinkInvalid), END, noLink.runtime.opRef);
		expect(() => noLink.database.work.workAttemptGet(noLink.runtime.opRef)).toThrow(WorkAttemptStateError);
	});

	test("wake_unaccepted settles only the linked child transition", async () => {
		for (const [root, reasonCode, expectedState] of [
			[{ originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN }, "send_rejected", "fallback"],
			[null, "send_rejected", "undeliverable"],
			[{ originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN }, "session_dead", "held"],
		] as const) {
			const f = await fixture();
			const linkedId = seedLinkedWakeReport(f, root);
			const runtime = { ...f.runtime, parent: null, wakeReportId: linkedId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const terminal = { kind: "local" as const, observedAt: END, reasonCode };
			const staged = f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, 0, {
					terminal,
					output: { ...runtime.output, disposition: "unavailable" },
				}),
			)!;
			const closed = closeAttempt({
				record: f.record,
				opRef: runtime.opRef,
				endState: reasonCode === "send_rejected" ? "failed" : "terminal_uncertain",
				errorCode: reasonCode,
				endedAt: END,
			});
			const result = f.database.transaction((tx) =>
				settleAttempt(f.database, tx, staged.opRef, staged.version, closed, {
					terminal,
					output: staged.output,
					decision: "wake_unaccepted",
					settledAt: END,
				}),
			);
			expect(result?.runtime.decision).toBe("wake_unaccepted");
			expect(f.database.work.workAttemptGet(runtime.opRef)?.decision).toBe("wake_unaccepted");
			expect(f.database.inbox.turnRow(runtime.opRef)).toBeUndefined();
			expect(f.raw.query("SELECT 1 FROM lane_reports WHERE child_op_ref = ?").get(runtime.opRef)).toBeNull();
			expect(f.database.outbox.rows().some((row) => row.delivery_id === runtime.deliveryId)).toBe(false);
			const child = f.database.work.laneReportGet(linkedId)!;
			expect(child.state).toBe(expectedState);
			if (expectedState === "fallback") {
				const childDeliveryId = workAttemptDeliveryId(
					f.database.instanceId,
					`lanejob-${Buffer.from("child", "utf8").toString("hex")}`,
					"gw-child-op",
				);
				expect(result?.childFallback?.deliveryId).toBe(childDeliveryId);
				expect(f.database.outbox.rows()).toMatchObject([
					{ delivery_id: childDeliveryId, turn_id: "gw-child-op", origin_key: originKey(LOOPBACK_ORIGIN) },
				]);
			} else {
				expect(result?.childFallback).toBeUndefined();
				expect(f.database.outbox.rows()).toHaveLength(0);
			}
			if (expectedState === "held") expect(child.hold_reason).toBe("wake_acceptance_uncertain");
		}
	});

	test("accepted wake consumes its child and reports normally to its inherited persona", async () => {
		const f = await fixture();
		const root = { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN };
		const linkedId = seedLinkedWakeReport(f, root);
		const personaSessionId = crypto.randomUUID();
		expect(
			f.database.transaction((tx) =>
				f.database.bindings.recordOwnedBinding(tx, {
					authority: f.authority,
					sessionId: personaSessionId,
					originKey: root.originKey,
					epoch: 0,
					repo: "/work",
				}),
			),
		).toBe(true);
		const runtime = { ...f.runtime, wakeReportId: linkedId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: START },
			}),
		)!;
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const result = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, f.settlement, f.admission),
		)!;
		expect(result.runtime.decision).toBe("reported");
		expect(f.database.work.laneReportGet(linkedId)?.consumed_op_ref).toBe(runtime.opRef);
		expect(f.database.inbox.pendingOldest(root.originKey)).toMatchObject({
			message_id: runtime.reportId,
			source: "lane_report",
		});
		expect(result.childFallback).toBeUndefined();
		expect(f.database.outbox.rows()).toHaveLength(0);
	});
	test("wake with proven output and missing receipt is accepted and reports normally", async () => {
		const f = await fixture();
		const root = { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN };
		const linkedId = seedLinkedWakeReport(f, root);
		const personaSessionId = crypto.randomUUID();
		f.database.transaction((tx) =>
			f.database.bindings.recordOwnedBinding(tx, {
				authority: f.authority,
				sessionId: personaSessionId,
				originKey: root.originKey,
				epoch: 0,
				repo: "/work",
			}),
		);
		const runtime = { ...f.runtime, wakeReportId: linkedId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const proof = {
			opRef: runtime.opRef,
			sessionId: runtime.sessionId,
			epoch: runtime.epoch,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: runtime.opRef,
			repo: runtime.cwd,
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: Buffer.byteLength("proven wake output"),
		};
		const provenOutput = {
			...runtime.output,
			disposition: "available" as const,
			excerpt: "proven wake output",
			proof,
		};
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, 0, { output: provenOutput }),
		)!;
		expect(accepted.sendPhase).toBe("prepared");
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const patch = {
			...f.settlement,
			output: { ...provenOutput, disposition: "unavailable" as const, excerpt: null },
			decision: "report" as const,
		};
		const result = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, patch, f.admission),
		)!;
		expect(result.runtime.decision).toBe("reported");
		expect(result.runtime.output.proof).toEqual(proof);
		expect(f.database.work.laneReportGet(linkedId)?.consumed_op_ref).toBe(runtime.opRef);
		expect(f.database.inbox.pendingOldest(root.originKey)).toMatchObject({
			message_id: runtime.reportId,
			source: "lane_report",
		});
	});
	test("wrong bound identity and history cannot be prepared", async () => {
		const f = await fixture();
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, epoch: 1 }, f.record)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, cwd: "/different" }, f.record)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				prepareAttempt(f.database, tx, { ...f.runtime, deliveryId: "arbitrary" }, f.record),
			),
		).toThrow();
		expect(f.database.work.workAttemptOpen()).toHaveLength(0);
		expect(f.database.work.laneJobJson(f.runtime.jobId)).toBeUndefined();
	});

	test("v24 migration rewrites a quarantined v23 runtime and restores quarantine triggers", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const settled = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		)!;
		expect(settled.runtime.decision).toBe("fallback");
		const targetAuthority = {
			canonicalAgentDir: join(f.path, "..", "target-agent"),
			identity: "target-broker",
		};
		f.database.transaction((tx) =>
			f.database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: f.authority,
				targetAuthority,
				evidence: "test v23 quarantine migration",
				disposition: "quarantine",
			}),
		);
		const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
		const legacy: Record<string, unknown> = {
			...current,
			target: current.parent?.kind === "persona" ? current.parent.origin : null,
			decision: "enqueued",
		};
		delete legacy.parent;
		delete legacy.reportId;
		delete legacy.wakeReportId;
		delete legacy.noticeHash;
		f.raw.exec(
			"DROP TRIGGER work_attempt_runtime_quarantine_update; DROP TRIGGER work_attempt_runtime_quarantine_delete;",
		);
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(legacy), f.runtime.opRef);
		f.raw.exec(`
			CREATE TRIGGER work_attempt_runtime_quarantine_update BEFORE UPDATE ON work_attempt_runtime
			WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
			BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
			CREATE TRIGGER work_attempt_runtime_quarantine_delete BEFORE DELETE ON work_attempt_runtime
			WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
			BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
		`);
		f.raw.exec(
			"DROP TABLE firstmate_release_receipts; DROP TABLE firstmate_release_intents; DROP TABLE firstmate_approvals; DROP TABLE firstmate_attempts; DROP TABLE firstmate_assignments; DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; DELETE FROM schema_migrations WHERE version >= 24",
		);

		const migrated = await Store.open(f.path);
		handles.push(migrated);
		expect(migrated.schemaVersion).toBe(41);
		expect(migrated.work.workAttemptGet(f.runtime.opRef)).toMatchObject({
			decision: "fallback",
			parent: { kind: "persona", origin: LOOPBACK_ORIGIN, originKey: originKey(LOOPBACK_ORIGIN) },
			reportId: workAttemptReportId(migrated.instanceId, f.runtime.jobId, f.runtime.opRef),
			wakeReportId: null,
			noticeHash: null,
		});
		const triggers = new Set(
			f.raw
				.query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'trigger'")
				.all()
				.map((row) => row.name),
		);
		expect(triggers.has("work_attempt_runtime_quarantine_update")).toBe(true);
		expect(triggers.has("work_attempt_runtime_quarantine_delete")).toBe(true);
		expect(() =>
			f.raw.query("UPDATE work_attempt_runtime SET version = version + 1 WHERE op_ref = ?").run(f.runtime.opRef),
		).toThrow();
	});

	test("v24 corrupt runtime migration rolls schema changes back", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw.exec(
			"DROP TABLE firstmate_release_receipts; DROP TABLE firstmate_release_intents; DROP TABLE firstmate_approvals; DROP TABLE firstmate_attempts; DROP TABLE firstmate_assignments; DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; DELETE FROM schema_migrations WHERE version >= 24",
		);
		f.raw.query("UPDATE work_attempt_runtime SET record_json = '{' WHERE op_ref = ?").run(f.runtime.opRef);
		await expect(Store.open(f.path)).rejects.toBeInstanceOf(DatabaseStartupError);
		expect(f.database.schemaVersion).toBe(23);
		expect(f.raw.query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lane_reports'").get()).toBeNull();
		expect(
			f.raw
				.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
				.all()
				.some((row) => row.name === "source"),
		).toBe(false);
	});
	test("v35 migration adds awaiting_checks and restores lane-job quarantine triggers", async () => {
		const f = await fixture();
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) }),
		);
		f.raw.exec(`
CREATE TABLE lane_jobs_v34 (
	job_id TEXT PRIMARY KEY,
	lane_key TEXT NOT NULL UNIQUE,
	branch TEXT NOT NULL,
	worktree_path TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','stalled','done','aborted')),
	record_json TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
INSERT INTO lane_jobs_v34 SELECT * FROM lane_jobs;
DROP TABLE lane_jobs;
ALTER TABLE lane_jobs_v34 RENAME TO lane_jobs;
CREATE TRIGGER lane_jobs_quarantine_update BEFORE UPDATE ON lane_jobs
	WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
	BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
CREATE TRIGGER lane_jobs_quarantine_delete BEFORE DELETE ON lane_jobs
	WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
	BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
INSERT INTO broker_quarantine (kind, subject_id, cutover_id) VALUES ('work', '${f.record.jobId}', 'cutover-v35');
DROP TABLE firstmate_release_receipts;
DROP TABLE firstmate_release_intents;
DROP TABLE firstmate_approvals;
DROP TABLE firstmate_attempts;
DROP TABLE firstmate_assignments;
DELETE FROM schema_migrations WHERE version > 34;
`);

		const migrated = await Store.open(f.path);
		handles.push(migrated);
		expect(migrated.schemaVersion).toBe(41);
		expect(migrated.work.laneJobJson(f.record.jobId)).toBe(JSON.stringify(f.record));
		expect(() =>
			f.raw.query("UPDATE lane_jobs SET updated_at = ? WHERE job_id = ?").run(END, f.record.jobId),
		).toThrow();
		expect(() => f.raw.query("DELETE FROM lane_jobs WHERE job_id = ?").run(f.record.jobId)).toThrow();
		const syntheticOpRef = "gw-checks-a-abababababab";
		f.raw
			.query(
				"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NULL, 'pending', NULL, NULL, NULL, 0, NULL, NULL, ?, ?)",
			)
			.run(`lane-report-${"a".repeat(64)}`, "parent", "a", syntheticOpRef, "checks passed", END, END);
		expect(migrated.work.readOpenWorkRows("replayable").laneReports).toEqual([]);
		expect(migrated.work.readOpenWorkRows("quarantined").laneReports).toMatchObject([
			{ child_name: "a", cutover_id: "cutover-v35", state: "pending" },
		]);
		f.raw
			.query(
				"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				"lanejob-awaiting-checks",
				"work/task/awaiting-checks",
				"fix/awaiting-checks",
				f.runtime.cwd,
				"awaiting_checks",
				"{}",
				END,
				END,
			);
	});
	test("v21 migration preserves historical open history and adopts without parent", async () => {
		const f = await fixture();
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) }),
		);
		// Preserve the fixture's explicit provenance while replaying v21 through v26.
		// Restoring a snapshot is not initialization/adoption of a populated database.
		f.raw.exec(`CREATE TEMP TABLE saved_authority AS SELECT * FROM broker_authority;
			CREATE TEMP TABLE saved_bindings AS SELECT * FROM broker_owned_bindings;`);
		for (const table of ["inbound_messages", "lane_jobs", "work_attempt_runtime", "monitor_events", "authored_outputs"])
			for (const action of ["update", "delete"]) f.raw.exec(`DROP TRIGGER ${table}_quarantine_${action}`);
		for (const table of ["broker_owned_bindings", "broker_cutovers", "broker_quarantine", "broker_retired_sessions"])
			for (const action of ["update", "delete"]) f.raw.exec(`DROP TRIGGER ${table}_immutable_${action}`);
		for (const table of [
			"broker_authority",
			"broker_owned_bindings",
			"broker_tail_cursors",
			"broker_cutovers",
			"broker_quarantine",
			"broker_retired_sessions",
		])
			f.raw.exec(`DROP TABLE ${table}`);
		f.raw.exec(`
ALTER TABLE memory_intents DROP COLUMN quarantine_reason;
ALTER TABLE memory_intents DROP COLUMN attempts;
DROP TABLE work_attempt_runtime;
ALTER TABLE inbound_messages DROP COLUMN source;
DROP TABLE lane_reports;
DROP TABLE firstmate_release_receipts;
DROP TABLE firstmate_release_intents;
DROP TABLE firstmate_approvals;
DROP TABLE firstmate_attempts;
DROP TABLE firstmate_assignments;
DELETE FROM schema_migrations WHERE version >= 21;
`);
		const migrated = await Store.open(f.path);
		handles.push(migrated);
		f.raw.exec(`INSERT INTO broker_authority SELECT * FROM saved_authority;
			INSERT INTO broker_owned_bindings SELECT * FROM saved_bindings;
			DROP TABLE saved_authority;
			DROP TABLE saved_bindings;`);
		migrated.bindings.assertAuthority(f.authority);
		expect(migrated.bindings.assertOwnedSession(SESSION, f.runtime.cwd, f.authority)).toMatchObject({
			originKey: f.runtime.sessionKey,
			epoch: f.runtime.epoch,
		});
		expect(migrated.schemaVersion).toBe(41);
		expect(migrated.work.laneJobJson(f.runtime.jobId)).toBe(JSON.stringify(f.record));
		const historical = { ...f.runtime, mode: "historical" as const, sendPhase: "uncertain" as const, parent: null };
		migrated.transaction((tx) => prepareAttempt(migrated, tx, historical, f.record));
		expect(migrated.work.workAttemptGet(f.runtime.opRef)?.parent).toBeNull();
		expect(
			migrated.transaction((tx) =>
				settleAttempt(migrated, tx, f.runtime.opRef, 0, f.closed, { ...f.settlement, decision: "no_target" }),
			)?.runtime.decision,
		).toBe("no_target");
		expect(migrated.outbox.rows()).toHaveLength(0);
	});

	test("no-op update does not journal attempt_updated and does not bump version", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));

		// Attempt a no-op patch (all fields already set to same values)
		const patched = f.database.transaction((tx) => {
			const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
			const patch: WorkAttemptPatch = {
				sendPhase: current.sendPhase,
				output: current.output,
			};
			return updateAttempt(f.database, tx, f.runtime.opRef, current.version, patch);
		});

		// No-op update should return the current runtime without bumping version
		expect(patched).toBeTruthy();
		expect(patched!.version).toBe(0);

		// No journal entry should be created for no-op update
		const journalEvents = f.raw
			.query<{ event_type: string }, [string]>(
				"SELECT event_type FROM work_journal WHERE entity = 'attempt' AND entity_key = ? ORDER BY seq",
			)
			.all(f.runtime.opRef)
			.map((row) => row.event_type);

		// Should only have the initial "attempt_started" event
		expect(journalEvents).toEqual(["attempt_started"]);
	});

	test("real field change journals attempt_updated and bumps version", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));

		// Attempt an actual update (changing a field)
		const patched = f.database.transaction((tx) => {
			const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
			const patch: WorkAttemptPatch = {
				sendPhase: "accepted",
				sendEvidence: { source: "status", observedAt: START },
			};
			return updateAttempt(f.database, tx, f.runtime.opRef, current.version, patch);
		});

		// Update should succeed and bump version
		expect(patched).toBeTruthy();
		expect(patched!.version).toBe(1);
		expect(patched!.sendPhase).toBe("accepted");

		// Journal should have the acceptance event (not update, since this changes acceptance status)
		const journalEventsReal = f.raw
			.query<{ event_type: string }, [string]>(
				"SELECT event_type FROM work_journal WHERE entity = 'attempt' AND entity_key = ? ORDER BY seq",
			)
			.all(f.runtime.opRef)
			.map((row) => row.event_type);

		expect(journalEventsReal).toEqual(["attempt_started", "attempt_accepted"]);
	});

	test("stale expected version on no-op patch still conflicts", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));

		// First, bump the version with a real update
		f.database.transaction((tx) => {
			const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
			const patch: WorkAttemptPatch = {
				sendPhase: "accepted",
				sendEvidence: { source: "status", observedAt: START },
			};
			updateAttempt(f.database, tx, f.runtime.opRef, current.version, patch);
		});

		// Now try a no-op update with the stale version (0, but current is 1)
		const patched = f.database.transaction((tx) => {
			const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
			const patch: WorkAttemptPatch = {
				sendPhase: current.sendPhase,
			};
			// Use stale version
			return updateAttempt(f.database, tx, f.runtime.opRef, 0, patch);
		});

		// Should fail due to version mismatch
		expect(patched).toBeUndefined();
	});

	test("boot replay with no-op updates still reconstructs state correctly", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));

		// Do some mixed updates: real changes and no-ops
		f.database.transaction((tx) => {
			let current = f.database.work.workAttemptGet(f.runtime.opRef)!;
			const realPatch: WorkAttemptPatch = {
				sendPhase: "accepted",
				sendEvidence: { source: "status", observedAt: START },
			};
			current = updateAttempt(f.database, tx, f.runtime.opRef, current.version, realPatch)!;

			// No-op update (should not journal)
			const noopPatch: WorkAttemptPatch = {
				sendPhase: current.sendPhase,
			};
			current = updateAttempt(f.database, tx, f.runtime.opRef, current.version, noopPatch) || current;

			// Another no-op update
			const anotherNoop: WorkAttemptPatch = {
				output: current.output,
			};
			updateAttempt(f.database, tx, f.runtime.opRef, current.version, anotherNoop);
		});

		// Store the runtime before wiping
		const beforeWipe = f.database.work.workAttemptGet(f.runtime.opRef)!;

		// Wipe the runtime table and replay from journal
		f.raw.exec("DELETE FROM work_attempt_runtime");
		const replayResult = f.database.replayJournal();
		expect(replayResult.attempts).toBe(1);

		// Verify replayed state matches what we had before
		const afterReplay = f.database.work.workAttemptGet(f.runtime.opRef)!;
		expect(afterReplay.version).toBe(beforeWipe.version);
		expect(afterReplay.sendPhase).toBe(beforeWipe.sendPhase);
		expect(afterReplay.sendEvidence).toEqual(beforeWipe.sendEvidence);
	});
});

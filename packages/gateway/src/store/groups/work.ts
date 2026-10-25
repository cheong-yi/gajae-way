import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import {
	type ChatMessagePayload,
	type FirstmateApproval,
	type FirstmateDecisionBinding,
	type FirstmateReleaseIntent,
	type FirstmateReleaseReceipt,
	isSilentOutput,
	type OriginRef,
	originKey,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import {
	assertValidOpRef,
	type LaneJobRecord,
	type LaneJobState,
	type PromptStatusBody,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import { buildDeliveryPayload } from "../../delivery/delivery";
import type { GroupDb, StoreCore } from "../core";
import { BrokerAuthorityError } from "../errors";
import {
	assertCause,
	assertFirstmateWorktree,
	assignmentJson,
	type FirstmateAssignment,
	type FirstmateAttempt,
	type FirstmateCause,
	type FirstmatePrepare,
	firstmateAssert,
	identityText,
} from "../firstmate";
import type { OpenWorkRows, OpenWorkScope } from "../open-work";
import type { WriteTx } from "../tx";
import { BindingsGroup } from "./bindings";
import { JOURNAL_PROJECTIONS, JournalGroup, type JournalRows, staleJournalKeys } from "./journal";

/** Compare JSON values without treating object insertion order as request content. */
function receiptJson(value: unknown): string {
	return JSON.stringify(value, (_key, item: unknown) =>
		item && typeof item === "object" && !Array.isArray(item)
			? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)))
			: item,
	);
}

function approvalRequest(approval: FirstmateApproval) {
	const { approvalId: _id, recordedAt: _at, ...request } = approval;
	return request;
}

/** Immutable policy/evidence stays under the existing work table owner, outside journal replay. */
export class FirstmateStore {
	readonly #db: GroupDb;
	readonly #work: WorkGroup;
	readonly #bindings: BindingsGroup;

	constructor(core: StoreCore, work: WorkGroup) {
		this.#db = core.groupDb("work");
		this.#work = work;
		this.#bindings = new BindingsGroup(core);
	}

	approval(approvalId: string): FirstmateApproval | undefined {
		const row = this.#db
			.prepare("SELECT record_json FROM firstmate_approvals WHERE approval_id = ?")
			.get(approvalId) as {
			record_json: string;
		} | null;
		return row ? (JSON.parse(row.record_json) as FirstmateApproval) : undefined;
	}

	approvalForMessage(ownerId: string, messageId: string): FirstmateApproval | undefined {
		const row = this.#db
			.prepare("SELECT approval_id FROM firstmate_approvals WHERE owner_id = ? AND message_id = ?")
			.get(ownerId, messageId) as { approval_id: string } | null;
		return row ? this.approval(row.approval_id) : undefined;
	}

	/** Freshness only; acknowledgement remains incapable of authorizing release. */
	approvalCurrent(approvalId: string): boolean {
		const approval = this.approval(approvalId);
		if (!approval) return false;
		const row = this.#db
			.prepare("SELECT op_ref FROM firstmate_attempts WHERE task_id = ? ORDER BY ordinal DESC LIMIT 1")
			.get(approval.identity.taskId) as { op_ref: string } | null;
		return row?.op_ref === approval.identity.opRef;
	}

	#assertDecisionBinding(binding: FirstmateDecisionBinding): FirstmateAssignment {
		firstmateAssert(
			binding &&
				[binding.name, binding.ownerId, binding.messageId, binding.mandateId].every(identityText) &&
				typeof binding.scope === "string" &&
				binding.scope.trim().length > 0,
			"decision_binding_invalid",
		);
		validateOriginRef(binding.thread);
		const assignment = this.assignment(binding.name);
		firstmateAssert(
			assignment &&
				assignment.ownerId === binding.ownerId &&
				assignment.mandate.id === binding.mandateId &&
				receiptJson(assignment.thread) === receiptJson(binding.thread),
			"decision_authority_mismatch",
		);
		const identity = binding.identity;
		firstmateAssert(identity && identity.taskId === assignment.taskId, "decision_task_mismatch");
		const attempt = this.attempt(identity.opRef);
		firstmateAssert(
			attempt &&
				attempt.taskId === identity.taskId &&
				attempt.jobId === identity.jobId &&
				attempt.opRef === identity.opRef &&
				attempt.ordinal === identity.ordinal &&
				attempt.sessionId === identity.sessionId &&
				attempt.epoch === identity.epoch,
			"decision_attempt_mismatch",
		);
		firstmateAssert(binding.messageId !== assignment.messageId, "decision_message_reused");
		const attempts = this.#db
			.prepare("SELECT record_json FROM firstmate_attempts WHERE task_id = ?")
			.all(identity.taskId) as { record_json: string }[];
		for (const row of attempts) {
			const cause = (JSON.parse(row.record_json) as FirstmateAttempt).cause;
			firstmateAssert(cause.kind !== "correction" || cause.messageId !== binding.messageId, "decision_message_reused");
		}
		return assignment;
	}

	#assertSettled(binding: FirstmateDecisionBinding): void {
		const assignment = this.#assertDecisionBinding(binding);
		const identity = binding.identity;
		if (this.#bindings.isQuarantined("work", identity.jobId)) throw new BrokerAuthorityError("quarantined");
		const latest = this.#db
			.prepare("SELECT op_ref FROM firstmate_attempts WHERE task_id = ? ORDER BY ordinal DESC LIMIT 1")
			.get(identity.taskId) as { op_ref: string } | null;
		firstmateAssert(latest?.op_ref === identity.opRef, "decision_not_current");
		const nativeJson = this.#work.laneJobJson(identity.jobId);
		const native = nativeJson === undefined ? undefined : parseLaneJobRecord(nativeJson);
		const runtime = this.#work.workAttemptGet(identity.opRef);
		const attempt = native?.attempts[identity.ordinal];
		firstmateAssert(
			native &&
				(native.state === "done" || native.state === "attempt_ended") &&
				native.awaitingChecks === undefined &&
				native.jobId === identity.jobId &&
				native.lane.branch === assignment.worktree.branch &&
				native.lane.worktreePath === assignment.worktree.path &&
				native.attempts.length === identity.ordinal + 1 &&
				native.attempts.every((item) => item.endedAt !== undefined) &&
				attempt?.opRef === identity.opRef &&
				attempt.sessionId === identity.sessionId &&
				attempt.endState === "completed",
			"decision_execution_unsettled",
		);
		firstmateAssert(
			runtime &&
				runtime.opRef === identity.opRef &&
				runtime.jobId === identity.jobId &&
				runtime.sessionId === identity.sessionId &&
				runtime.epoch === identity.epoch &&
				runtime.sessionKey === `work/task/${binding.name}` &&
				runtime.laneKey === `work-${binding.name}` &&
				runtime.cwd === assignment.worktree.path &&
				runtime.mode !== "historical" &&
				runtime.parent?.kind === "persona" &&
				runtime.parent.originKey === originKey(assignment.thread) &&
				receiptJson(runtime.parent.origin) === receiptJson(assignment.thread) &&
				runtime.startedAt === attempt.startedAt &&
				this.attempt(identity.opRef)?.preparedAt === attempt.startedAt &&
				runtime.settledAt === attempt.endedAt &&
				runtime.settledAt !== null &&
				workAccepted(runtime) &&
				runtime.terminal?.reasonCode === "end_turn" &&
				runtime.output.disposition !== "pending" &&
				!["undecided", "wake_unaccepted", "recovery_indeterminate"].includes(runtime.decision),
			"decision_runtime_unsettled",
		);
	}

	#assertLiveBinding(binding: FirstmateDecisionBinding): void {
		const row = this.#bindings.getSessionRecord(`work/task/${binding.name}`);
		firstmateAssert(
			row?.sessionId === binding.identity.sessionId && row.epoch === binding.identity.epoch,
			"decision_binding_stale",
		);
	}

	/**
	 * Manager must authenticate the owner and validate the exact observed snapshot under
	 * the native lock. This owner checks durable attribution, shape and settlement only.
	 */
	recordApproval(tx: WriteTx, approval: FirstmateApproval): void {
		firstmateAssert(
			identityText(approval.approvalId) && identityText(approval.recordedAt),
			"approval_identity_invalid",
		);
		this.#assertDecisionBinding(approval);
		const evidence = approval.evidence;
		firstmateAssert(
			evidence && Array.isArray(evidence.reasons) && evidence.reasons.every(identityText),
			"approval_evidence_invalid",
		);
		if (approval.decision === "acknowledge_unavailable") {
			firstmateAssert(
				evidence.completeness === "unavailable" &&
					evidence.text === undefined &&
					evidence.source === undefined &&
					evidence.provenance === undefined &&
					evidence.reasons.length > 0,
				"approval_acknowledgement_invalid",
			);
		} else {
			firstmateAssert(
				approval.decision === "accept_outcome" &&
					(evidence.completeness === "complete" || evidence.completeness === "partial") &&
					typeof evidence.text === "string" &&
					(evidence.source === "retained" || evidence.source === "native"),
				"approval_evidence_unsupported",
			);
			// No logs reconstruction service or caller-asserted reconstructed evidence admission.
			const provenance = evidence.provenance;
			firstmateAssert(
				provenance &&
					provenance.source === "turn.result" &&
					provenance.opRef === approval.identity.opRef &&
					provenance.sessionId === approval.identity.sessionId &&
					provenance.clientRef === approval.identity.opRef &&
					provenance.repo === this.assignment(approval.name)!.worktree.path &&
					provenance.contentVersion === 1 &&
					Number.isFinite(provenance.terminalAt) &&
					Number.isSafeInteger(provenance.byteLength) &&
					provenance.byteLength >= Buffer.byteLength(evidence.text) &&
					(provenance.fullness === "original" || provenance.fullness === "partial") &&
					(!("epoch" in provenance) || provenance.epoch === approval.identity.epoch) &&
					(evidence.completeness !== "partial" || evidence.reasons.length > 0) &&
					(evidence.completeness !== "complete" ||
						(provenance.fullness === "original" && provenance.byteLength === Buffer.byteLength(evidence.text))),
				"approval_provenance_invalid",
			);
		}
		const previous = this.approvalForMessage(approval.ownerId, approval.messageId);
		if (previous) {
			firstmateAssert(
				receiptJson(approvalRequest(previous)) === receiptJson(approvalRequest(approval)),
				"approval_conflict",
			);
			return;
		}
		firstmateAssert(!this.approval(approval.approvalId), "approval_conflict");
		firstmateAssert(!this.releaseForMessage(approval.ownerId, approval.messageId), "decision_message_reused");
		this.#assertSettled(approval);
		this.#assertLiveBinding(approval);
		this.#db
			.prepare(
				"INSERT INTO firstmate_approvals (approval_id, task_id, op_ref, owner_id, message_id, record_json) VALUES (?, ?, ?, ?, ?, ?)",
			)
			.run(
				tx,
				approval.approvalId,
				approval.identity.taskId,
				approval.identity.opRef,
				approval.ownerId,
				approval.messageId,
				receiptJson(approval),
			);
	}

	releaseIntent(releaseId: string): FirstmateReleaseIntent | undefined {
		const row = this.#db
			.prepare("SELECT record_json FROM firstmate_release_intents WHERE release_id = ?")
			.get(releaseId) as { record_json: string } | null;
		return row ? (JSON.parse(row.record_json) as FirstmateReleaseIntent) : undefined;
	}

	releaseReceipt(releaseId: string): FirstmateReleaseReceipt | undefined {
		const row = this.#db
			.prepare("SELECT record_json FROM firstmate_release_receipts WHERE release_id = ?")
			.get(releaseId) as { record_json: string } | null;
		return row ? (JSON.parse(row.record_json) as FirstmateReleaseReceipt) : undefined;
	}

	releaseForMessage(ownerId: string, messageId: string): FirstmateReleaseIntent | undefined {
		const row = this.#db
			.prepare("SELECT release_id FROM firstmate_release_intents WHERE owner_id = ? AND message_id = ?")
			.get(ownerId, messageId) as { release_id: string } | null;
		return row ? this.releaseIntent(row.release_id) : undefined;
	}

	releaseForAttempt(opRef: string): FirstmateReleaseIntent | undefined {
		const row = this.#db.prepare("SELECT release_id FROM firstmate_release_intents WHERE op_ref = ?").get(opRef) as {
			release_id: string;
		} | null;
		return row ? this.releaseIntent(row.release_id) : undefined;
	}

	completedReleaseForSession(taskId: string, sessionId: string, epoch: number): FirstmateReleaseReceipt | undefined {
		const rows = this.#db
			.prepare(
				"SELECT r.record_json FROM firstmate_release_receipts r JOIN firstmate_release_intents i ON i.release_id = r.release_id JOIN firstmate_attempts a ON a.op_ref = i.op_ref WHERE a.task_id = ? ORDER BY a.ordinal",
			)
			.all(taskId) as { record_json: string }[];
		for (const row of rows) {
			const receipt = JSON.parse(row.record_json) as FirstmateReleaseReceipt;
			const identity = receipt.request.identity;
			if (identity.taskId === taskId && identity.sessionId === sessionId && identity.epoch === epoch) return receipt;
		}
		return undefined;
	}

	#assertRelease(intent: FirstmateReleaseIntent): void {
		const request = intent.request;
		firstmateAssert(identityText(intent.releaseId) && identityText(intent.requestedAt), "release_identity_invalid");
		this.#assertDecisionBinding(request);
		const approval = this.approval(request.approvalId);
		firstmateAssert(
			request.action === "release_session_preserve_work" &&
				approval?.decision === "accept_outcome" &&
				approval.name === request.name &&
				receiptJson(approval.identity) === receiptJson(request.identity) &&
				approval.ownerId === request.ownerId &&
				approval.mandateId === request.mandateId &&
				receiptJson(approval.thread) === receiptJson(request.thread) &&
				approval.scope === request.scope &&
				approval.messageId !== request.messageId,
			"release_approval_mismatch",
		);
	}

	/** Intent is not confirmation and never closes a session. Obligations remain manager-owned. */
	recordReleaseIntent(tx: WriteTx, intent: FirstmateReleaseIntent): void {
		this.#assertRelease(intent);
		const request = intent.request;
		const previous = this.releaseForMessage(request.ownerId, request.messageId);
		if (previous) {
			firstmateAssert(receiptJson(previous.request) === receiptJson(request), "release_conflict");
			return;
		}
		firstmateAssert(
			!this.releaseIntent(intent.releaseId) &&
				!this.releaseForAttempt(request.identity.opRef) &&
				!this.approvalForMessage(request.ownerId, request.messageId),
			"release_conflict",
		);
		this.#assertSettled(request);
		this.#assertLiveBinding(request);
		firstmateAssert(this.approvalCurrent(request.approvalId), "release_approval_stale");
		this.#db
			.prepare(
				"INSERT INTO firstmate_release_intents (release_id, approval_id, op_ref, owner_id, message_id, record_json) VALUES (?, ?, ?, ?, ?, ?)",
			)
			.run(
				tx,
				intent.releaseId,
				request.approvalId,
				request.identity.opRef,
				request.ownerId,
				request.messageId,
				receiptJson(intent),
			);
	}

	/** Caller supplies confirmed native close, atomically with its binding update. No close inference. */
	recordReleaseReceipt(tx: WriteTx, receipt: FirstmateReleaseReceipt): void {
		const intent = this.releaseIntent(receipt.releaseId);
		firstmateAssert(
			intent &&
				receipt.requestedAt === intent.requestedAt &&
				receiptJson(receipt.request) === receiptJson(intent.request) &&
				identityText(receipt.releasedAt) &&
				receipt.preserved === true &&
				receipt.sessionKey === `work/task/${intent.request.name}`,
			"release_receipt_mismatch",
		);
		const previous = this.releaseReceipt(receipt.releaseId);
		if (previous) {
			firstmateAssert(receiptJson(previous) === receiptJson(receipt), "release_receipt_conflict");
			return;
		}
		this.#assertRelease(intent);
		this.#assertSettled(intent.request);
		firstmateAssert(this.approvalCurrent(intent.request.approvalId), "release_approval_stale");
		this.#db
			.prepare("INSERT INTO firstmate_release_receipts (release_id, record_json) VALUES (?, ?)")
			.run(tx, receipt.releaseId, receiptJson(receipt));
	}

	assignment(name: string): FirstmateAssignment | undefined {
		const row = this.#db
			.prepare("SELECT task_id, assignment_json FROM firstmate_assignments WHERE lane_name = ?")
			.get(name) as { task_id: string; assignment_json: string } | null;
		if (!row) {
			firstmateAssert(
				!this.#db.prepare("SELECT op_ref FROM firstmate_attempts WHERE lane_name = ? LIMIT 1").get(name),
				"assignment_missing",
			);
			return undefined;
		}
		const assignment = JSON.parse(row.assignment_json) as FirstmateAssignment;
		assignmentJson(assignment);
		firstmateAssert(assignment.name === name && assignment.taskId === row.task_id, "assignment_corrupt");
		return assignment;
	}

	attempt(opRef: string): FirstmateAttempt | undefined {
		const row = this.#db
			.prepare("SELECT task_id, lane_name, ordinal, record_json FROM firstmate_attempts WHERE op_ref = ?")
			.get(opRef) as { task_id: string; lane_name: string; ordinal: number; record_json: string } | null;
		if (!row) return undefined;
		const attempt = JSON.parse(row.record_json) as FirstmateAttempt;
		firstmateAssert(
			attempt.opRef === opRef &&
				attempt.taskId === row.task_id &&
				attempt.ordinal === row.ordinal &&
				Number.isSafeInteger(attempt.ordinal) &&
				attempt.ordinal >= 0 &&
				identityText(attempt.jobId) &&
				identityText(attempt.sessionId) &&
				Number.isSafeInteger(attempt.epoch) &&
				attempt.epoch >= 0 &&
				typeof attempt.text === "string" &&
				attempt.text.length > 0 &&
				identityText(attempt.preparedAt),
			"attempt_corrupt",
		);
		const assignment = this.assignment(row.lane_name);
		firstmateAssert(assignment?.taskId === attempt.taskId, "assignment_missing");
		assertCause(attempt.cause, assignment);
		return attempt;
	}

	/** All immutable preparations, including uncertain and superseded attempts. */
	attemptsForTask(taskId: string): readonly FirstmateAttempt[] {
		const rows = this.#db
			.prepare("SELECT op_ref FROM firstmate_attempts WHERE task_id = ? ORDER BY ordinal")
			.all(taskId) as { op_ref: string }[];
		return rows.map((row) => this.attempt(row.op_ref)!);
	}

	/** Exact task/message replay lookup; changed attribution or text is never a new correction. */
	correction(
		taskId: string,
		cause: Extract<FirstmateCause, { kind: "correction" }>,
		text: string,
	): FirstmateAttempt | undefined {
		const row = this.#db.prepare("SELECT lane_name FROM firstmate_assignments WHERE task_id = ?").get(taskId) as {
			lane_name: string;
		} | null;
		const assignment = row ? this.assignment(row.lane_name) : undefined;
		firstmateAssert(assignment, "initial_attribution_missing");
		firstmateAssert(cause.kind === "correction", "correction_cause_invalid");
		assertCause(cause, assignment);
		firstmateAssert(typeof text === "string" && text.length > 0, "input_missing");
		const rows = this.#db
			.prepare("SELECT op_ref FROM firstmate_attempts WHERE task_id = ? ORDER BY ordinal")
			.all(taskId) as { op_ref: string }[];
		let match: FirstmateAttempt | undefined;
		for (const row of rows) {
			const attempt = this.attempt(row.op_ref)!;
			const persisted = attempt.cause;
			if (persisted.kind !== "correction" || persisted.messageId !== cause.messageId) continue;
			firstmateAssert(
				!match &&
					persisted.priorOpRef === cause.priorOpRef &&
					persisted.ownerId === cause.ownerId &&
					persisted.mandateId === cause.mandateId &&
					attempt.text === text,
				"correction_conflict",
			);
			match = attempt;
		}
		return match;
	}

	initial(taskId: string): FirstmateAttempt | undefined {
		const row = this.#db
			.prepare("SELECT op_ref FROM firstmate_attempts WHERE task_id = ? AND ordinal = 0")
			.get(taskId) as { op_ref: string } | null;
		const attempt = row ? this.attempt(row.op_ref) : undefined;
		firstmateAssert(
			!attempt || (attempt.taskId === taskId && attempt.ordinal === 0 && attempt.cause.kind === "assignment"),
			"initial_attribution_mismatch",
		);
		return attempt;
	}

	/** Repeated immutable policy may match; conflicting reservations never authorize adoption. */
	assertAssignment(assignment: FirstmateAssignment): void {
		const json = assignmentJson(assignment);
		const rows = this.#db
			.prepare(
				"SELECT assignment_json FROM firstmate_assignments WHERE task_id = ? OR lane_name = ? OR worktree_path = ? OR git_dir = ? OR thread_key = ? OR (owner_id = ? AND message_id = ?)",
			)
			.all(
				assignment.taskId,
				assignment.name,
				assignment.worktree.path,
				assignment.worktree.gitDir,
				originKey(assignment.thread),
				assignment.ownerId,
				assignment.messageId,
			) as { assignment_json: string }[];
		for (const row of rows) {
			firstmateAssert(
				assignmentJson(JSON.parse(row.assignment_json) as FirstmateAssignment) === json,
				"immutable_assignment_conflict",
			);
		}
		assertFirstmateWorktree(assignment);
	}

	assertUnmapped(name: string, cwd?: string): void {
		firstmateAssert(!this.assignment(name), "mapped_lane_requires_owner_admission");
		if (cwd === undefined) return;
		let path = cwd;
		try {
			path = realpathSync(cwd);
		} catch {
			// Native cwd validation still owns missing unmapped paths.
		}
		firstmateAssert(
			!this.#db.prepare("SELECT task_id FROM firstmate_assignments WHERE worktree_path = ?").get(path),
			"worktree_already_assigned",
		);
	}

	/** Called only by native prepare, on its same synchronous WriteTx, before native/journal writes. */
	recordPrepare(tx: WriteTx, runtime: WorkAttemptRuntime, record: LaneJobRecord, evidence: FirstmatePrepare): void {
		const { assignment, cause, text } = evidence;
		this.assertAssignment(assignment);
		assertCause(cause, assignment);
		firstmateAssert(typeof text === "string" && text.length > 0, "input_missing");
		// Intent survives restart and remains after completion. Neither state admits fresh work.
		firstmateAssert(
			!this.#db
				.prepare(
					"SELECT 1 FROM firstmate_release_intents i JOIN firstmate_attempts a ON a.op_ref = i.op_ref WHERE a.task_id = ? LIMIT 1",
				)
				.get(assignment.taskId),
			"task_release_pending_or_completed",
		);
		firstmateAssert(
			runtime.mode !== "historical" &&
				runtime.sessionKey === `work/task/${assignment.name}` &&
				runtime.cwd === assignment.worktree.path,
			"attempt_binding_mismatch",
		);
		firstmateAssert(
			record.lane.branch === assignment.worktree.branch &&
				record.lane.worktreePath === assignment.worktree.path &&
				record.lane.repository === undefined &&
				record.lane.repositoryPath === undefined,
			"lane_binding_mismatch",
		);
		firstmateAssert(
			runtime.parent?.kind === "persona" && runtime.parent.originKey === originKey(assignment.thread),
			"thread_binding_mismatch",
		);
		const assigned = this.assignment(assignment.name);
		let ordinal = 0;
		if (cause.kind === "assignment") {
			firstmateAssert(
				record.attempts.length === 1 &&
					!assigned &&
					!this.initial(assignment.taskId) &&
					text === assignment.mandate.text,
				"assignment_already_attempted",
			);
		} else {
			const prior = record.attempts.at(-2);
			const previous = this.attempt(cause.priorOpRef);
			firstmateAssert(assigned && this.initial(assignment.taskId), "initial_attribution_missing");
			firstmateAssert(
				prior?.opRef === cause.priorOpRef && previous?.taskId === assignment.taskId && prior.endedAt !== undefined,
				"retry_lineage_mismatch",
			);
			firstmateAssert(
				previous.jobId === runtime.jobId &&
					previous.sessionId === prior.sessionId &&
					previous.ordinal === record.attempts.length - 2,
				"retry_attempt_mismatch",
			);
			if (cause.kind === "checks") {
				firstmateAssert(assignment.mandate.checkRetries, "retry_lineage_mismatch");
				firstmateAssert(
					record.awaitingChecks?.retries === cause.retryOrdinal &&
						record.awaitingChecks.prNumber === cause.prNumber &&
						record.awaitingChecks.headSha === cause.headSha,
					"retry_cycle_mismatch",
				);
			} else {
				firstmateAssert(!this.correction(assignment.taskId, cause, text), "correction_already_attempted");
				const nativeJson = this.#work.laneJobJson(runtime.jobId);
				const native = nativeJson === undefined ? undefined : parseLaneJobRecord(nativeJson);
				const priorRuntime = this.#work.workAttemptGet(cause.priorOpRef);
				firstmateAssert(
					(native?.state === "done" || native?.state === "attempt_ended") &&
						native.awaitingChecks === undefined &&
						native.attempts.at(-1)?.opRef === cause.priorOpRef &&
						native.attempts.every((attempt) => attempt.endedAt !== undefined) &&
						prior.endState === "completed" &&
						JSON.stringify(native.attempts) === JSON.stringify(record.attempts.slice(0, -1)) &&
						record.state === "running" &&
						record.awaitingChecks === undefined,
					"correction_lineage_mismatch",
				);
				firstmateAssert(
					JSON.stringify(parseLaneJobRecord(JSON.stringify(record))) ===
						JSON.stringify(
							parseLaneJobRecord(
								JSON.stringify({
									...native,
									state: "running",
									attempts: record.attempts,
									updatedAt: runtime.startedAt,
								}),
							),
						),
					"correction_native_history_changed",
				);
				firstmateAssert(
					priorRuntime?.opRef === cause.priorOpRef &&
						priorRuntime.jobId === runtime.jobId &&
						priorRuntime.sessionId === previous.sessionId &&
						priorRuntime.epoch === previous.epoch &&
						priorRuntime.sessionKey === runtime.sessionKey &&
						priorRuntime.laneKey === runtime.laneKey &&
						priorRuntime.cwd === runtime.cwd &&
						priorRuntime.startedAt === prior.startedAt &&
						previous.preparedAt === prior.startedAt &&
						priorRuntime.settledAt === prior.endedAt &&
						workAccepted(priorRuntime) &&
						priorRuntime.terminal?.reasonCode === "end_turn" &&
						runtime.sessionId === previous.sessionId &&
						runtime.epoch === previous.epoch,
					"correction_predecessor_mismatch",
				);
			}
			ordinal = previous.ordinal + 1;
		}
		if (!assigned) {
			this.#db
				.prepare(
					"INSERT INTO firstmate_assignments (task_id, lane_name, owner_id, message_id, thread_key, worktree_path, git_dir, assignment_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				)
				.run(
					tx,
					assignment.taskId,
					assignment.name,
					assignment.ownerId,
					assignment.messageId,
					originKey(assignment.thread),
					assignment.worktree.path,
					assignment.worktree.gitDir,
					assignmentJson(assignment),
				);
		}
		const attempt: FirstmateAttempt = {
			opRef: runtime.opRef,
			taskId: assignment.taskId,
			jobId: runtime.jobId,
			sessionId: runtime.sessionId,
			epoch: runtime.epoch,
			ordinal,
			cause,
			text,
			preparedAt: runtime.startedAt,
		};
		this.#db
			.prepare(
				"INSERT INTO firstmate_attempts (op_ref, task_id, lane_name, ordinal, record_json) VALUES (?, ?, ?, ?, ?)",
			)
			.run(tx, runtime.opRef, assignment.taskId, assignment.name, ordinal, JSON.stringify(attempt));
	}
}

export type WorkAttemptMode = "start" | "run" | "historical";
export type WorkAttemptDecision =
	| "undecided"
	| "reported"
	| "fallback"
	| "suppressed"
	| "no_target"
	| "wake_unaccepted"
	| "recovery_indeterminate";
export type WorkAttemptRequestedDecision = "report" | "suppressed" | "no_target" | "wake_unaccepted";

export function workAttemptEndState(
	reason: string,
): "completed" | "terminal_missing_receipt" | "terminal_uncertain" | "failed" | "attempt_ended" {
	return reason === "end_turn"
		? "completed"
		: reason === "terminal_missing_receipt"
			? "terminal_missing_receipt"
			: ["terminal_uncertain", "session_dead", "session_disowned", "recovery_indeterminate"].includes(reason)
				? "terminal_uncertain"
				: ["sdk_failed", "send_rejected"].includes(reason)
					? "failed"
					: "attempt_ended";
}

export interface WorkReportRoot {
	readonly originKey: string;
	readonly origin: OriginRef;
}

export type WorkParent =
	| {
			readonly kind: "persona";
			readonly originKey: string;
			readonly origin: OriginRef;
			/** Trigger message ID for threading Slack lane reports (channel root origins only). */
			readonly triggerMessageId?: string;
	  }
	| { readonly kind: "lane"; readonly name: string; readonly root: WorkReportRoot | null };

export interface WorkAttemptOutputProof {
	readonly opRef: string;
	readonly sessionId: string;
	readonly epoch: number;
	readonly observedAtMs: number;
	readonly source: "turn.result";
	readonly attribution: "operation_ref";
	readonly fullness: "original";
	readonly clientRef: string;
	readonly repo: string;
	readonly terminalAt: number;
	readonly contentVersion: 1;
	readonly byteLength: number;
	readonly turnId?: string;
	readonly commandId?: string;
}

export interface WorkAttemptOutput {
	readonly disposition: "pending" | "available" | "unavailable" | "silent";
	readonly reads: number;
	readonly nextReadAt: string | null;
	readonly excerpt: string | null;
	readonly proof: WorkAttemptOutputProof | null;
	/** Proven original attempt-final silence survives output loss and ledger pruning. */
	readonly knownSilence: WorkAttemptOutputProof | null;
	/** Transport failure details for failed attempts with terminal_missing_receipt. */
	readonly transportCause?: {
		readonly kind: string;
		readonly nativeErrorCode?: string;
		readonly http2RstCode?: number;
		readonly status?: number;
		readonly requestBytes?: number;
		readonly retryMaxAttempts?: number;
		readonly endpointClass?: string;
	};
}

export interface WorkAttemptTerminalEvidence {
	readonly kind: "broker" | "local";
	readonly observedAt: string;
	readonly reasonCode: string;
	readonly status?: PromptStatusBody;
}

/** The caller must first validate and sanitize the correlated status. */
export function terminalEvidence(
	status: PromptStatusBody,
	observedAt: string,
): WorkAttemptTerminalEvidence | undefined {
	if (status.status !== "terminal_ok" && status.status !== "failed") return undefined;
	const reasonCode =
		status.receiptState === "missing"
			? "terminal_missing_receipt"
			: status.receiptState === "unknown"
				? "terminal_uncertain"
				: status.status === "failed"
					? status.error?.code === "prompt_deadline_exceeded"
						? "prompt_deadline_exceeded"
						: "sdk_failed"
					: status.receiptState === "present" && status.outcome?.reason === "end_turn"
						? "end_turn"
						: ["cancelled", "max_tokens", "max_turn_requests", "refusal"].includes(status.outcome?.reason ?? "")
							? status.outcome!.reason!
							: "stopped_incomplete";
	return { kind: "broker", observedAt, reasonCode, status };
}

export interface WorkAttemptRuntime {
	readonly opRef: string;
	readonly jobId: string;
	readonly laneKey: string;
	readonly sessionKey: string;
	readonly sessionId: string;
	readonly epoch: number;
	readonly cwd: string;
	readonly startedAt: string;
	readonly mode: WorkAttemptMode;
	readonly sendPhase: "prepared" | "accepted" | "uncertain";
	readonly sendEvidence: { readonly source: "receipt" | "status"; readonly observedAt: string } | null;
	/** Turn-end proof from lifecycle or live idle reconciliation; a missing receipt alone is not proof. */
	readonly turnEndObservedAt?: string;
	readonly terminal: WorkAttemptTerminalEvidence | null;
	readonly output: WorkAttemptOutput;
	readonly parent: WorkParent | null;
	readonly reportId: string;
	readonly wakeReportId: string | null;
	readonly noticeHash: string | null;
	readonly deliveryId: string;
	readonly decision: WorkAttemptDecision;
	readonly settledAt: string | null;
	readonly version: number;
}

export type WorkAttemptPatch = Partial<
	Pick<WorkAttemptRuntime, "sendPhase" | "sendEvidence" | "turnEndObservedAt" | "terminal" | "output">
>;
export interface WorkAttemptSettlement extends WorkAttemptPatch {
	readonly decision: WorkAttemptRequestedDecision;
	readonly settledAt: string;
}

export interface WorkAttemptAdmissionPersona {
	readonly kind: "persona";
	readonly row: {
		readonly messageId: string;
		readonly originKey: string;
		readonly originRefJson: string;
		readonly body: string;
		readonly receivedAt: string;
	};
	readonly fallbackPayload: ChatMessagePayload;
	readonly holdReason?: string;
}

export interface WorkAttemptAdmissionLane {
	readonly kind: "lane";
	readonly report: {
		readonly reportId: string;
		readonly parentName: string;
		readonly childName: string;
		readonly childOpRef: string;
		readonly body: string;
		readonly root: WorkReportRoot | null;
	};
	readonly fallbackPayload: ChatMessagePayload | null;
}

export type WorkAttemptAdmission = WorkAttemptAdmissionPersona | WorkAttemptAdmissionLane;
export interface WorkAttemptSettleResult {
	readonly runtime: WorkAttemptRuntime;
	readonly fallbackPayload?: ChatMessagePayload;
	readonly childFallback?: ChatMessagePayload;
	readonly requeuedParent?: string;
}

export interface WorkAttemptSettleChecks {
	readonly personaSessionExists?: boolean;
	readonly personaMessageExists?: boolean;
	readonly laneParentAvailable?: boolean;
}

export type LaneReportState = "pending" | "claimed" | "consumed" | "fallback" | "held" | "undeliverable";
export interface LaneReportRow {
	readonly report_id: string;
	readonly parent_name: string;
	readonly child_name: string;
	readonly child_op_ref: string;
	readonly body: string;
	readonly root_json: string | null;
	readonly state: LaneReportState;
	readonly claim_kind: "steer" | "wake" | null;
	readonly claim_ref: string | null;
	readonly claim_target_op_ref: string | null;
	readonly claim_seq: number;
	readonly hold_reason: string | null;
	readonly consumed_op_ref: string | null;
	readonly created_at: string;
	readonly updated_at: string;
}

export class WorkAttemptStateError extends Error {
	readonly opRef: string | undefined;
	readonly assertion: string;
	constructor(opRef?: string, assertion = "work attempt payload validation") {
		super(
			`work lane state unavailable${opRef === undefined ? "" : ` opRef=${JSON.stringify(opRef)}`} assertion=${assertion}`,
		);
		this.name = "WorkAttemptStateError";
		this.opRef = opRef;
		this.assertion = assertion;
	}
}

/** Length-delimited identity hashing; independent of target, output and recovery time. */
export function workAttemptDeliveryId(instanceId: string, jobId: string, opRef: string): string {
	return `work-${createHash("sha256")
		.update(JSON.stringify([instanceId, jobId, opRef]))
		.digest("hex")}`;
}

/** Stable report identity, using length-delimited UTF-8 components. */
export function workAttemptReportId(instanceId: string, jobId: string, opRef: string): string {
	const hash = createHash("sha256");
	for (const value of [instanceId, jobId, opRef]) {
		const length = Buffer.allocUnsafe(4);
		length.writeUInt32BE(Buffer.byteLength(value, "utf8"));
		hash.update(length).update(value, "utf8");
	}
	return `lane-report-${hash.digest("hex")}`;
}

export function workAccepted(runtime: WorkAttemptRuntime): boolean {
	return runtime.sendPhase === "accepted" || runtime.output.proof !== null || runtime.output.knownSilence !== null;
}

const WORK_REASON_CODES = new Set([
	"end_turn",
	"prompt_deadline_exceeded",
	"cancelled",
	"max_tokens",
	"max_turn_requests",
	"refusal",
	"stopped_incomplete",
	"sdk_failed",
	"send_rejected",
	"terminal_missing_receipt",
	"terminal_uncertain",
	"session_dead",
	"session_disowned",
	"host_lost",
	"recovery_indeterminate",
	"output_unavailable",
]);

export function workAssert(condition: unknown, assertion = "work attempt payload validation"): asserts condition {
	if (!condition) throw new WorkAttemptStateError(undefined, assertion);
}

export function workTime(value: unknown): boolean {
	return typeof value === "string" && value.length <= 40 && Number.isFinite(Date.parse(value));
}

export function workString(value: unknown, max = 512): value is string {
	if (typeof value !== "string" || value.length === 0 || value.length > max) return false;
	for (let index = 0; index < value.length; index++) {
		if (value.charCodeAt(index) < 32) return false;
	}
	return true;
}

export function validateWorkRoot(root: WorkReportRoot): void {
	workAssert(root && typeof root === "object");
	validateOriginRef(root.origin);
	workAssert(originKey(root.origin) === root.originKey);
	workAssert(["discord", "slack", "telegram", "loopback"].includes(root.origin.platform));
}

export function validateWorkParent(parent: WorkParent, mode: WorkAttemptMode): void {
	workAssert(mode === "start");
	if (parent.kind === "persona") {
		validateOriginRef(parent.origin);
		workAssert(originKey(parent.origin) === parent.originKey);
		workAssert(["discord", "slack", "telegram", "loopback"].includes(parent.origin.platform));
		return;
	}
	workAssert(parent.kind === "lane" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(parent.name));
	if (parent.root !== null) validateWorkRoot(parent.root);
}

export function validateWorkRuntime(value: WorkAttemptRuntime, instanceId: string): void {
	workAssert(value && typeof value === "object");
	workAssert(workString(value.opRef));
	assertValidOpRef(value.opRef);
	workAssert(/^lanejob-[a-z0-9-]{1,128}$/.test(value.jobId));
	workAssert(workString(value.laneKey) && /^work\/task\/[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(value.sessionKey));
	workAssert(value.laneKey === `work-${value.sessionKey.slice("work/task/".length)}`);
	workAssert(/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value.sessionId));
	workAssert(Number.isSafeInteger(value.epoch) && value.epoch >= 0);
	workAssert(workString(value.cwd, 4096) && value.cwd.startsWith("/") && workTime(value.startedAt));
	workAssert(["start", "run", "historical"].includes(value.mode));
	workAssert(["prepared", "accepted", "uncertain"].includes(value.sendPhase));
	workAssert(Number.isSafeInteger(value.version) && value.version >= 0);
	workAssert(value.deliveryId === workAttemptDeliveryId(instanceId, value.jobId, value.opRef));
	workAssert(!Object.hasOwn(value, "target"));
	workAssert(value.parent === null || (value.parent && typeof value.parent === "object"));
	if (value.parent !== null) validateWorkParent(value.parent, value.mode);
	workAssert(value.mode !== "run" || value.parent === null);
	workAssert(value.reportId === workAttemptReportId(instanceId, value.jobId, value.opRef));
	workAssert(value.wakeReportId === null || /^lane-report-[0-9a-f]{64}$/.test(value.wakeReportId));
	workAssert(value.wakeReportId === null || value.mode === "start");
	workAssert(value.noticeHash === null || /^[0-9a-f]{64}$/.test(value.noticeHash));
	if (value.sendEvidence !== null) {
		workAssert(["receipt", "status"].includes(value.sendEvidence.source) && workTime(value.sendEvidence.observedAt));
	}
	workAssert(value.sendPhase !== "accepted" || value.sendEvidence !== null);
	workAssert(value.turnEndObservedAt === undefined || workTime(value.turnEndObservedAt));
	if (value.terminal !== null) {
		const terminal = value.terminal;
		workAssert(["broker", "local"].includes(terminal.kind) && workTime(terminal.observedAt));
		workAssert(WORK_REASON_CODES.has(terminal.reasonCode));
		if (terminal.kind === "broker") workAssert(terminal.status !== undefined);
		if (terminal.status !== undefined) {
			const status = terminal.status;
			workAssert(["terminal_ok", "failed"].includes(status.status));
			for (const id of [status.turnId, status.commandId, status.clientRef])
				workAssert(id === undefined || workString(id));
			for (const at of [status.acceptedAt, status.startedAt, status.terminalAt])
				workAssert(at === undefined || Number.isFinite(at));
			workAssert(
				status.receiptState === undefined || ["absent", "present", "missing", "unknown"].includes(status.receiptState),
			);
			workAssert(
				status.error === undefined ||
					(status.error.message === undefined && WORK_REASON_CODES.has(status.error.code ?? "")),
			);
			if (status.outcome) {
				workAssert(status.outcome.reason === undefined || WORK_REASON_CODES.has(status.outcome.reason));
				workAssert(status.outcome.kind === undefined || workString(status.outcome.kind, 64));
				workAssert(status.outcome.provenance === undefined || workString(status.outcome.provenance, 64));
			}
		}
	}
	const output = value.output;
	workAssert(output && ["pending", "available", "unavailable", "silent"].includes(output.disposition));
	workAssert(Number.isSafeInteger(output.reads) && output.reads >= 0 && output.reads <= 3);
	workAssert(output.nextReadAt === null || workTime(output.nextReadAt));
	workAssert(
		output.excerpt === null ||
			(typeof output.excerpt === "string" && Buffer.byteLength(output.excerpt, "utf8") <= 2048),
	);
	for (const proof of [output.proof, output.knownSilence]) {
		if (proof === null) continue;
		workAssert(
			proof && proof.opRef === value.opRef && proof.sessionId === value.sessionId && proof.epoch === value.epoch,
		);
		workAssert(Number.isFinite(proof.observedAtMs) && proof.observedAtMs >= Date.parse(value.startedAt));
		workAssert(
			proof.source === "turn.result" && proof.attribution === "operation_ref" && proof.fullness === "original",
		);
		workAssert(proof.clientRef === value.opRef && proof.repo === value.cwd && proof.contentVersion === 1);
		workAssert(Number.isFinite(proof.terminalAt) && proof.terminalAt >= Date.parse(value.startedAt));
		workAssert(Number.isSafeInteger(proof.byteLength) && proof.byteLength >= 0);
		for (const id of [proof.turnId, proof.commandId]) workAssert(id === undefined || workString(id));
	}
	workAssert(output.disposition !== "available" || (output.proof !== null && output.excerpt !== null));
	workAssert(output.disposition !== "silent" || output.knownSilence !== null);
	workAssert(
		[
			"undecided",
			"reported",
			"fallback",
			"suppressed",
			"no_target",
			"wake_unaccepted",
			"recovery_indeterminate",
		].includes(value.decision),
	);
	workAssert(value.decision === "undecided" ? value.settledAt === null : workTime(value.settledAt));
	if (value.decision !== "undecided") {
		workAssert(value.terminal !== null && output.disposition !== "pending");
		workAssert(
			!["reported", "fallback"].includes(value.decision) || (value.parent !== null && output.knownSilence === null),
		);
		workAssert(
			value.decision !== "fallback" ||
				value.parent?.kind === "persona" ||
				(value.parent?.kind === "lane" && value.parent.root !== null),
		);
		workAssert(
			value.decision !== "no_target" ||
				value.parent === null ||
				(value.parent.kind === "lane" && value.parent.root === null),
		);
		workAssert(value.decision !== "suppressed" || output.knownSilence !== null);
		workAssert(
			value.decision !== "wake_unaccepted" ||
				(value.wakeReportId !== null && value.mode === "start" && value.terminal !== null && !workAccepted(value)),
		);
		workAssert(value.wakeReportId === null || workAccepted(value) || value.decision === "wake_unaccepted");
		if (value.decision === "recovery_indeterminate") workAssert(output.knownSilence === null);
	}
	workAssert(Buffer.byteLength(JSON.stringify(value), "utf8") <= 16384);
}

function lateReceiptReconciled(current: WorkAttemptRuntime, next: WorkAttemptRuntime): boolean {
	const before = current.terminal;
	const after = next.terminal;
	const proof = next.output.proof;
	return (
		before?.kind === "broker" &&
		after?.kind === "broker" &&
		before.observedAt === after.observedAt &&
		before.status?.receiptState === "missing" &&
		after.status?.receiptState === "present" &&
		after.status.status === "terminal_ok" &&
		after.reasonCode === terminalEvidence(after.status, after.observedAt)?.reasonCode &&
		after.status.clientRef === current.opRef &&
		(before.status.commandId === undefined || before.status.commandId === after.status.commandId) &&
		(before.status.turnId === undefined || before.status.turnId === after.status.turnId) &&
		current.output.proof === null &&
		proof !== null &&
		after.status.commandId === proof.commandId &&
		after.status.turnId === proof.turnId &&
		after.status.terminalAt === proof.terminalAt &&
		(next.output.disposition === "available" || next.output.disposition === "silent")
	);
}

function laneJobDatabaseId(name: string): string {
	return `lanejob-${Buffer.from(name, "utf8").toString("hex")}`;
}

function laneReportRoot(row: LaneReportRow): WorkReportRoot | null {
	if (row.root_json === null) return null;
	const root = JSON.parse(row.root_json) as WorkReportRoot;
	validateWorkRoot(root);
	return root;
}

/** Resolve Slack channel roots to trigger threads; Discord reports keep their admitted origin. */
export function resolveLaneReportOrigin(parent: WorkParent | null, admissionOriginRefJson: string): string {
	if (parent?.kind !== "persona" || !parent.triggerMessageId) return admissionOriginRefJson;
	// Synthetic lane report ids are internal row ids, not platform messages or thread ids.
	if (/^lane-report-[0-9a-f]{64}$/.test(parent.triggerMessageId)) return admissionOriginRefJson;
	try {
		const origin = JSON.parse(admissionOriginRefJson) as OriginRef;
		if (origin.kind !== "channel" || origin.platform !== "slack") return admissionOriginRefJson;
		if (!parent.triggerMessageId.startsWith(`${origin.conversationId}:`)) return admissionOriginRefJson;
		const threadOrigin: OriginRef = {
			platform: origin.platform,
			kind: "thread",
			conversationId: parent.triggerMessageId,
			parentId: origin.conversationId,
		};
		validateOriginRef(threadOrigin);
		return JSON.stringify(threadOrigin);
	} catch {
		return admissionOriginRefJson;
	}
}

function validateLaneReportRow(row: LaneReportRow): void {
	workAssert(/^lane-report-[0-9a-f]{64}$/.test(row.report_id));
	workAssert(/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(row.parent_name));
	workAssert(/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(row.child_name));
	assertValidOpRef(row.child_op_ref);
	workAssert(typeof row.body === "string" && Buffer.byteLength(row.body, "utf8") <= 2048);
	workAssert(["pending", "claimed", "consumed", "fallback", "held", "undeliverable"].includes(row.state));
	workAssert(row.claim_kind === null || ["steer", "wake"].includes(row.claim_kind));
	workAssert(Number.isSafeInteger(row.claim_seq) && row.claim_seq >= 0);
	workAssert(row.claim_ref === null || workString(row.claim_ref, 128));
	workAssert(row.claim_target_op_ref === null || workString(row.claim_target_op_ref, 128));
	for (const ref of [row.claim_ref, row.claim_target_op_ref, row.consumed_op_ref])
		if (ref !== null) assertValidOpRef(ref);
	workAssert((row.state !== "claimed" && row.state !== "held") || (row.claim_kind !== null && row.claim_ref !== null));
	workAssert(row.claim_kind !== "steer" || row.claim_target_op_ref !== null);
	workAssert(row.claim_kind !== "wake" || row.claim_target_op_ref === null);
	workAssert(row.hold_reason === null || workString(row.hold_reason, 128));
	workAssert(row.state === "consumed" ? workString(row.consumed_op_ref, 128) : row.consumed_op_ref === null);
	workAssert(workTime(row.created_at) && workTime(row.updated_at));
	laneReportRoot(row);
}

export interface WorkAttemptDbRow {
	readonly op_ref: string;
	readonly job_id: string;
	readonly lane_key: string;
	readonly session_id: string;
	readonly version: number;
	readonly settled_at: string | null;
	readonly delivery_id: string;
	readonly record_json: string;
}

export interface LaneJobDbRow {
	readonly job_id: string;
	readonly lane_key: string;
	readonly branch: string;
	readonly worktree_path: string;
	readonly state: string;
	readonly record_json: string;
	readonly created_at: string;
	readonly updated_at: string;
}

export interface WorkLaneJobInput {
	readonly jobId: string;
	readonly laneKey: string;
	readonly state: string;
	readonly createdAt: string;
	readonly updatedAt: string;
	readonly lane: { readonly branch: string; readonly worktreePath: string };
	readonly json: string;
}

export class WorkGroup {
	readonly #core: StoreCore;
	readonly #db: GroupDb;
	readonly #journal: JournalGroup;
	/** FirstMate policy evidence shares the work table owner and its native readers. */
	readonly firstmate: FirstmateStore;

	constructor(core: StoreCore) {
		this.#core = core;
		this.#db = core.groupDb("work");
		this.#journal = new JournalGroup(core);
		this.firstmate = new FirstmateStore(core, this);
	}

	get instanceId(): string {
		return this.#core.instanceId;
	}

	#assertNotQuarantined(jobId: string): void {
		if (this.#db.prepare("SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = ?").get(jobId))
			throw new BrokerAuthorityError("quarantined");
	}

	workAttemptGet(opRef: string): WorkAttemptRuntime | undefined {
		const row = this.#db
			.prepare("SELECT * FROM work_attempt_runtime WHERE op_ref = ?")
			.get(opRef) as WorkAttemptDbRow | null;

		if (!row) return undefined;
		try {
			const runtime = JSON.parse(row.record_json) as WorkAttemptRuntime;
			validateWorkRuntime(runtime, this.instanceId);
			workAssert(
				runtime.opRef === row.op_ref && runtime.jobId === row.job_id && runtime.laneKey === row.lane_key,
				"runtime identity matches database columns",
			);
			workAssert(
				runtime.sessionId === row.session_id && runtime.version === row.version,
				"runtime session and version match database columns",
			);
			workAssert(
				runtime.settledAt === row.settled_at && runtime.deliveryId === row.delivery_id,
				"runtime settlement and delivery id match database columns",
			);
			this.#workHistory(runtime);
			return runtime;
		} catch (error) {
			throw new WorkAttemptStateError(
				opRef,
				error instanceof WorkAttemptStateError ? error.assertion : "runtime record parsing",
			);
		}
	}

	/** Keyset pagination keeps recovery reads bounded; invalid rows can be quarantined by the caller. */
	workAttemptOpen(
		limit = 100,
		afterOpRef = "",
		onInvalid?: (error: WorkAttemptStateError, jobId: string) => void,
	): readonly WorkAttemptRuntime[] {
		workAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 1000);
		const rows = this.#db
			.prepare(
				"SELECT op_ref, job_id FROM work_attempt_runtime WHERE settled_at IS NULL AND op_ref > ? AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id) ORDER BY op_ref LIMIT ?",
			)
			.all(afterOpRef, limit) as Array<{ op_ref: string; job_id: string }>;

		if (!onInvalid) return rows.map((row) => this.workAttemptGet(row.op_ref)!);
		const attempts: WorkAttemptRuntime[] = [];
		for (const row of rows) {
			try {
				const runtime = this.workAttemptGet(row.op_ref);
				if (runtime) attempts.push(runtime);
			} catch (error) {
				if (!(error instanceof WorkAttemptStateError)) throw error;
				onInvalid(error, row.job_id);
			}
		}
		return attempts;
	}

	workAttemptOpenByLane(laneKey: string): WorkAttemptRuntime | undefined {
		const row = this.#db
			.prepare(
				"SELECT op_ref FROM work_attempt_runtime WHERE lane_key = ? AND settled_at IS NULL AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id)",
			)
			.get(laneKey) as { op_ref: string } | null;

		return row ? this.workAttemptGet(row.op_ref) : undefined;
	}

	workAttemptJobId(opRef: string): string | undefined {
		return (
			this.#db.prepare("SELECT job_id FROM work_attempt_runtime WHERE op_ref = ?").get(opRef) as {
				job_id: string;
			} | null
		)?.job_id;
	}

	workAttemptRows(): readonly { readonly op_ref: string; readonly job_id: string }[] {
		return this.#db.prepare("SELECT op_ref, job_id FROM work_attempt_runtime ORDER BY op_ref").all() as Array<{
			op_ref: string;
			job_id: string;
		}>;
	}

	workAttemptPrepare(
		tx: WriteTx,
		runtime: WorkAttemptRuntime,
		record: LaneJobRecord,
		bindingMatches = true,
		firstmate?: FirstmatePrepare,
	): void {
		this.#assertNotQuarantined(runtime.jobId);
		validateWorkRuntime(runtime, this.instanceId);
		workAssert(runtime.version === 0 && runtime.decision === "undecided" && runtime.terminal === null);
		workAssert(runtime.output.reads === 0 && runtime.output.disposition === "pending");
		workAssert(
			runtime.output.proof === null &&
				runtime.output.knownSilence === null &&
				runtime.output.excerpt === null &&
				runtime.output.nextReadAt === null,
		);
		workAssert(runtime.sendEvidence === null);
		workAssert(runtime.sendPhase === (runtime.mode === "historical" ? "uncertain" : "prepared"));
		this.#workValidateHistory(runtime, record);
		if (runtime.wakeReportId !== null) {
			const report = this.#laneReportGetInside(runtime.wakeReportId);
			workAssert(report?.state === "claimed" && report.claim_kind === "wake" && report.claim_ref === runtime.opRef);
		}
		const previousJson = this.laneJobJson(runtime.jobId);
		const previous = previousJson === undefined ? undefined : parseLaneJobRecord(previousJson);
		if (runtime.mode === "historical") {
			workAssert(previous && JSON.stringify(previous) === JSON.stringify(parseLaneJobRecord(JSON.stringify(record))));
		} else {
			workAssert(bindingMatches, "work session binding matches runtime");
			workAssert(!previous?.attempts.some((attempt) => attempt.endedAt === undefined));
			workAssert(record.attempts.length === (previous?.attempts.length ?? 0) + 1);
			workAssert(JSON.stringify(record.attempts.slice(0, -1)) === JSON.stringify(previous?.attempts ?? []));
		}
		if (firstmate) this.firstmate.recordPrepare(tx, runtime, record, firstmate);
		else this.firstmate.assertUnmapped(runtime.sessionKey.slice("work/task/".length), runtime.cwd);
		this.#workPutHistory(tx, runtime, record);
		this.#db
			.prepare(
				"INSERT INTO work_attempt_runtime (op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				tx,
				runtime.opRef,
				runtime.jobId,
				runtime.laneKey,
				runtime.sessionId,
				runtime.version,
				null,
				runtime.deliveryId,
				JSON.stringify(runtime),
			);
		this.#journal.record(tx, "attempt", runtime.opRef, "attempt_started");
	}

	workAttemptUpdate(
		tx: WriteTx,
		opRef: string,
		expectedVersion: number,
		patch: WorkAttemptPatch,
	): WorkAttemptRuntime | undefined {
		const current = this.workAttemptGet(opRef);
		if (!current || current.version !== expectedVersion || current.settledAt !== null) return undefined;
		this.#assertNotQuarantined(current.jobId);
		// Skip no-op updates where patch contains no actual field changes (only version bumps).
		const patchedFields = ["sendPhase", "sendEvidence", "turnEndObservedAt", "terminal", "output"] as const;
		const changed = patchedFields.some((field) => {
			if (!Object.hasOwn(patch, field)) return false;
			return JSON.stringify(current[field]) !== JSON.stringify(patch[field as keyof typeof patch]);
		});
		if (!changed) return current;
		const next = this.#workPatched(current, patch);
		workAssert(next.decision === "undecided" && next.settledAt === null);
		if (!this.#workCas(tx, current, next)) return undefined;
		this.#journal.record(
			tx,
			"attempt",
			opRef,
			current.terminal === null && next.terminal !== null
				? "attempt_terminal"
				: !workAccepted(current) && workAccepted(next)
					? "attempt_accepted"
					: "attempt_updated",
		);
		if (next.wakeReportId !== null && workAccepted(next)) this.#consumeWakeReport(tx, next);
		return next;
	}

	workAttemptSettle(
		tx: WriteTx,
		opRef: string,
		expectedVersion: number,
		record: LaneJobRecord,
		patch: WorkAttemptSettlement,
		admission?: WorkAttemptAdmission,
		checks: WorkAttemptSettleChecks = {},
	): WorkAttemptSettleResult | undefined {
		const current = this.workAttemptGet(opRef);
		if (!current || current.version !== expectedVersion || current.settledAt !== null) return undefined;
		this.#assertNotQuarantined(current.jobId);
		const { decision: requested, settledAt, ...attemptPatch } = patch;
		const staged = this.#workPatched(current, attemptPatch);
		const draft = { ...staged, settledAt };
		let decision: WorkAttemptDecision;
		let fallbackPayload: ChatMessagePayload | undefined;
		let childFallback: ChatMessagePayload | undefined;
		let requeuedParent: string | undefined;
		let linkedReport: LaneReportRow | undefined;

		if (current.wakeReportId !== null && !workAccepted(draft)) {
			workAssert(requested === "wake_unaccepted" && admission === undefined);
			decision = "wake_unaccepted";
			linkedReport = this.#laneReportGetInside(current.wakeReportId);
			workAssert(
				linkedReport?.state === "claimed" &&
					linkedReport.claim_kind === "wake" &&
					linkedReport.claim_ref === current.opRef,
			);
		} else {
			workAssert(requested !== "wake_unaccepted");
			if (requested === "report") {
				workAssert(admission !== undefined && draft.parent !== null && draft.output.knownSilence === null);
				if (admission.kind === "persona") {
					workAssert(draft.parent.kind === "persona");
					workAssert(
						admission.row.messageId === draft.reportId &&
							admission.row.originKey === draft.parent.originKey &&
							originKey(draft.parent.origin) === draft.parent.originKey &&
							Buffer.byteLength(admission.row.body, "utf8") <= 2048 &&
							admission.fallbackPayload.turnId === opRef &&
							admission.fallbackPayload.deliveryId === draft.deliveryId &&
							originKey(admission.fallbackPayload.origin) === draft.parent.originKey &&
							admission.fallbackPayload.text === admission.row.body,
					);
					const refused =
						Boolean(admission.holdReason) || !checks.personaSessionExists || Boolean(checks.personaMessageExists);
					decision = refused ? "fallback" : "reported";
					if (refused) fallbackPayload = admission.fallbackPayload;
				} else {
					workAssert(draft.parent.kind === "lane");
					const report = admission.report;
					workAssert(
						report.reportId === draft.reportId &&
							report.parentName === draft.parent.name &&
							report.childOpRef === opRef &&
							report.childName === draft.sessionKey.slice("work/task/".length) &&
							Buffer.byteLength(report.body, "utf8") <= 2048 &&
							JSON.stringify(report.root) === JSON.stringify(draft.parent.root),
					);
					if (report.root === null) workAssert(admission.fallbackPayload === null);
					else
						workAssert(
							admission.fallbackPayload !== null &&
								admission.fallbackPayload.turnId === opRef &&
								admission.fallbackPayload.deliveryId === draft.deliveryId &&
								originKey(admission.fallbackPayload.origin) === report.root.originKey &&
								admission.fallbackPayload.text === report.body,
						);
					decision = checks.laneParentAvailable ? "reported" : report.root ? "fallback" : "no_target";
					if (decision === "fallback") fallbackPayload = admission.fallbackPayload!;
				}
			} else {
				workAssert(admission === undefined);
				decision = requested;
			}
		}

		const next: WorkAttemptRuntime = { ...draft, decision };
		validateWorkRuntime(next, this.instanceId);
		this.#workValidateHistory(next, record);
		const previous = this.#workHistory(current);
		workAssert(record.attempts.length === previous.attempts.length);
		workAssert(JSON.stringify(record.attempts.slice(0, -1)) === JSON.stringify(previous.attempts.slice(0, -1)));
		if (!this.#workCas(tx, current, next)) return undefined;
		this.#journal.record(tx, "attempt", opRef, "attempt_settled");
		this.#workPutHistory(tx, next, record);
		if (next.wakeReportId !== null && workAccepted(next)) this.#consumeWakeReport(tx, next);
		if (next.decision === "reported" && admission?.kind === "lane")
			this.#putLaneReport(tx, admission.report, next.settledAt!);
		if (fallbackPayload) {
			const expectedOrigin =
				admission?.kind === "persona"
					? admission.row.originKey
					: admission?.kind === "lane"
						? admission.report.root?.originKey
						: undefined;
			workAssert(
				fallbackPayload.deliveryId === next.deliveryId &&
					fallbackPayload.turnId === opRef &&
					expectedOrigin !== undefined &&
					originKey(fallbackPayload.origin) === expectedOrigin &&
					fallbackPayload.role === "assistant" &&
					fallbackPayload.final === true &&
					!fallbackPayload.reaction &&
					!isSilentOutput(fallbackPayload.text),
			);
		}
		if (next.decision === "wake_unaccepted") {
			workAssert(linkedReport !== undefined);
			const wakeEffects = this.#settleWakeReport(tx, next, record, linkedReport);
			childFallback = wakeEffects.childFallback;
			requeuedParent = wakeEffects.requeuedParent;
		}
		return {
			runtime: next,
			...(fallbackPayload ? { fallbackPayload } : {}),
			...(childFallback ? { childFallback } : {}),
			...(requeuedParent ? { requeuedParent } : {}),
		};
	}

	/** Admits one pending child report for its parent lane; a matching existing report is a no-op. */
	laneReportInsert(tx: WriteTx, report: WorkAttemptAdmissionLane["report"], createdAt: string): void {
		this.#putLaneReport(tx, report, createdAt);
	}

	laneParentAvailable(name: string): boolean {
		const jobId = laneJobDatabaseId(name);
		if (this.#db.prepare("SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = ?").get(jobId))
			return false;
		const json = this.laneJobJsonByLaneKey(`work-${name}`);
		if (json === undefined) return false;
		try {
			const parent = parseLaneJobRecord(json);
			return (
				parent.jobId === jobId &&
				parent.state !== "aborted" &&
				parent.state !== "awaiting_checks" &&
				parent.state !== "awaiting_operator" &&
				parent.state !== "stalled" &&
				parent.state !== "done"
			);
		} catch {
			return false;
		}
	}

	#settleWakeReport(
		tx: WriteTx,
		runtime: WorkAttemptRuntime,
		record: LaneJobRecord,
		row: LaneReportRow,
	): { readonly childFallback?: ChatMessagePayload; readonly requeuedParent?: string } {
		workAssert(
			runtime.wakeReportId === row.report_id &&
				row.claim_kind === "wake" &&
				row.claim_ref === runtime.opRef &&
				(row.state === "claimed" || row.state === "held") &&
				(row.state !== "held" || row.hold_reason === "wake_acceptance_uncertain"),
		);
		workAssert(runtime.settledAt !== null);
		const definitiveRefusal = runtime.terminal?.reasonCode === "send_rejected";
		const isHeld = record.state === "awaiting_operator" || record.state === "stalled";
		let state: LaneReportState;
		let holdReason: string | null = null;
		let clearClaim = false;
		let childFallback: ChatMessagePayload | undefined;
		let requeuedParent: string | undefined;
		if (!definitiveRefusal) {
			state = "held";
			holdReason = "wake_acceptance_uncertain";
		} else if (!isHeld) {
			state = "pending";
			clearClaim = true;
			requeuedParent = row.parent_name;
		} else if (row.root_json !== null) {
			state = "fallback";
			const root = laneReportRoot(row);
			workAssert(root !== null);
			const deliveryId = workAttemptDeliveryId(this.instanceId, laneJobDatabaseId(row.child_name), row.child_op_ref);
			childFallback = buildDeliveryPayload(row.child_op_ref, root.origin, row.body, deliveryId);
			workAssert(childFallback !== undefined);
		} else {
			state = "undeliverable";
		}
		const changed = this.#db
			.prepare(
				"UPDATE lane_reports SET state = ?, claim_kind = ?, claim_ref = ?, claim_target_op_ref = ?, hold_reason = ?, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_kind = 'wake' AND claim_ref = ? AND claim_seq = ?",
			)
			.run(
				tx,
				state,
				clearClaim ? null : row.claim_kind,
				clearClaim ? null : row.claim_ref,
				clearClaim ? null : row.claim_target_op_ref,
				holdReason,
				runtime.settledAt,
				row.report_id,
				row.state,
				runtime.opRef,
				row.claim_seq,
			).changes;
		workAssert(changed === 1);
		return {
			...(childFallback ? { childFallback } : {}),
			...(requeuedParent ? { requeuedParent } : {}),
		};
	}

	#consumeWakeReport(tx: WriteTx, runtime: WorkAttemptRuntime): void {
		const reportId = runtime.wakeReportId;
		if (reportId === null) return;
		const row = this.#laneReportGetInside(reportId);
		workAssert(row !== undefined && row.claim_kind === "wake" && row.claim_ref === runtime.opRef);
		if (row.state === "consumed") {
			workAssert(row.consumed_op_ref === runtime.opRef);
			return;
		}
		workAssert(row.state === "claimed" || row.state === "held");
		const changed = this.#db
			.prepare(
				"UPDATE lane_reports SET state = 'consumed', consumed_op_ref = ?, hold_reason = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_kind = 'wake' AND claim_ref = ? AND claim_seq = ?",
			)
			.run(tx, runtime.opRef, new Date().toISOString(), reportId, row.state, runtime.opRef, row.claim_seq).changes;
		workAssert(changed === 1);
	}

	#workPatched(current: WorkAttemptRuntime, patch: WorkAttemptPatch): WorkAttemptRuntime {
		const allowed = new Set(["sendPhase", "sendEvidence", "turnEndObservedAt", "terminal", "output"]);
		workAssert(Object.keys(patch).every((key) => allowed.has(key)));
		const next = { ...current, ...patch, version: current.version + 1 };
		validateWorkRuntime(next, this.instanceId);
		workAssert(current.sendPhase !== "accepted" || next.sendPhase === "accepted");
		workAssert(current.sendPhase === "prepared" || next.sendPhase !== "prepared");
		workAssert(current.turnEndObservedAt === undefined || next.turnEndObservedAt === current.turnEndObservedAt);
		workAssert(
			current.terminal === null ||
				JSON.stringify(current.terminal) === JSON.stringify(next.terminal) ||
				lateReceiptReconciled(current, next),
		);
		workAssert(
			current.output.knownSilence === null ||
				JSON.stringify(current.output.knownSilence) === JSON.stringify(next.output.knownSilence),
		);
		workAssert(next.output.reads >= current.output.reads && next.output.reads <= current.output.reads + 1);
		workAssert(next.output.reads === current.output.reads || next.terminal !== null);
		return next;
	}

	#workCas(tx: WriteTx, current: WorkAttemptRuntime, next: WorkAttemptRuntime): boolean {
		return (
			this.#db
				.prepare(
					"UPDATE work_attempt_runtime SET record_json = ?, version = ?, settled_at = ? WHERE op_ref = ? AND version = ? AND settled_at IS NULL",
				)
				.run(tx, JSON.stringify(next), next.version, next.settledAt, current.opRef, current.version).changes === 1
		);
	}

	#workValidateHistory(runtime: WorkAttemptRuntime, input: LaneJobRecord): void {
		const record = parseLaneJobRecord(JSON.stringify(input));
		workAssert(
			record.jobId === runtime.jobId && record.lane.worktreePath === runtime.cwd,
			"history job identity matches runtime",
		);
		const attempt = record.attempts.find((item) => item.opRef === runtime.opRef);
		workAssert(
			attempt && attempt.sessionId === runtime.sessionId && attempt.startedAt === runtime.startedAt,
			"history attempt identity matches runtime",
		);
		workAssert(
			runtime.settledAt === null ? attempt.endedAt === undefined : attempt.endedAt === runtime.settledAt,
			"attempt.endedAt matches runtime.settledAt",
		);
		if (runtime.settledAt === null)
			workAssert(record.attempts.at(-1)?.opRef === runtime.opRef, "open runtime belongs to latest history attempt");
	}

	#workHistory(runtime: WorkAttemptRuntime): LaneJobRecord {
		const json = this.laneJobJson(runtime.jobId);
		workAssert(
			json !== undefined && this.laneJobJsonByLaneKey(runtime.laneKey) === json,
			"lane history rows share one serialized record",
		);
		const record = parseLaneJobRecord(json);
		this.#workValidateHistory(runtime, record);
		return record;
	}

	#workPutHistory(tx: WriteTx, runtime: WorkAttemptRuntime, record: LaneJobRecord): void {
		this.putLaneJob(tx, { ...record, laneKey: runtime.laneKey, json: JSON.stringify(record) });
	}

	putLaneJob(tx: WriteTx, job: WorkLaneJobInput): void {
		this.#assertNotQuarantined(job.jobId);
		this.#db
			.prepare(
				"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET lane_key = excluded.lane_key, branch = excluded.branch, worktree_path = excluded.worktree_path, state = excluded.state, record_json = excluded.record_json, updated_at = excluded.updated_at",
			)
			.run(
				tx,
				job.jobId,
				job.laneKey,
				job.lane.branch,
				job.lane.worktreePath,
				job.state,
				job.json,
				job.createdAt,
				job.updatedAt,
			);
		this.#journal.record(tx, "lane_job", job.jobId, `lane_job_${job.state as LaneJobState}`);
	}

	/** Every column of every attempt and lane-job row, as the journal snapshots them. */
	projectionRows(): { readonly attempts: WorkAttemptDbRow[]; readonly laneJobs: LaneJobDbRow[] } {
		return {
			attempts: this.#db
				.prepare(`SELECT ${JOURNAL_PROJECTIONS.attempt.columns.join(", ")} FROM work_attempt_runtime ORDER BY op_ref`)
				.all() as WorkAttemptDbRow[],
			laneJobs: this.#db
				.prepare(`SELECT ${JOURNAL_PROJECTIONS.lane_job.columns.join(", ")} FROM lane_jobs ORDER BY job_id`)
				.all() as LaneJobDbRow[],
		};
	}

	/**
	 * Make the attempt and lane-job projections equal to the journaled snapshots: rows that differ
	 * or have no journal entry are removed, then journaled rows are written back verbatim.
	 */
	replayProjection(
		tx: WriteTx,
		attempts: ReadonlyMap<string, JournalRows["attempt"] | null>,
		laneJobs: ReadonlyMap<string, JournalRows["lane_job"] | null>,
	): { readonly attempts: number; readonly laneJobs: number } {
		const current = this.projectionRows();
		const staleAttempts = staleJournalKeys("attempt", current.attempts, attempts);
		const staleJobs = staleJournalKeys("lane_job", current.laneJobs, laneJobs);
		for (const opRef of staleAttempts)
			this.#db.prepare("DELETE FROM work_attempt_runtime WHERE op_ref = ?").run(tx, opRef);
		for (const jobId of staleJobs) this.#db.prepare("DELETE FROM lane_jobs WHERE job_id = ?").run(tx, jobId);
		for (const jobId of staleJobs) {
			const row = laneJobs.get(jobId);
			if (row)
				this.#db
					.prepare(
						"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
					)
					.run(
						tx,
						row.job_id,
						row.lane_key,
						row.branch,
						row.worktree_path,
						row.state,
						row.record_json,
						row.created_at,
						row.updated_at,
					);
		}
		for (const opRef of staleAttempts) {
			const row = attempts.get(opRef);
			if (row)
				this.#db
					.prepare(
						"INSERT INTO work_attempt_runtime (op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
					)
					.run(
						tx,
						row.op_ref,
						row.job_id,
						row.lane_key,
						row.session_id,
						row.version,
						row.settled_at,
						row.delivery_id,
						row.record_json,
					);
		}
		return { attempts: staleAttempts.length, laneJobs: staleJobs.length };
	}

	laneJobJson(jobId: string): string | undefined {
		return (
			this.#db.prepare("SELECT record_json FROM lane_jobs WHERE job_id = ?").get(jobId) as {
				record_json: string;
			} | null
		)?.record_json;
	}

	laneJobJsonByLaneKey(laneKey: string): string | undefined {
		return (
			this.#db.prepare("SELECT record_json FROM lane_jobs WHERE lane_key = ?").get(laneKey) as {
				record_json: string;
			} | null
		)?.record_json;
	}

	laneJobHistoryRows(): readonly {
		readonly job_id: string;
		readonly lane_key: string;
		readonly state: string;
		readonly record_json: string;
	}[] {
		return this.#db
			.prepare("SELECT job_id, lane_key, state, record_json FROM lane_jobs ORDER BY job_id")
			.all() as Array<{
			job_id: string;
			lane_key: string;
			state: string;
			record_json: string;
		}>;
	}

	laneJobRows(includeQuarantined = false): Array<{
		job_id: string;
		lane_key: string;
		state: string;
		branch: string;
		worktree_path: string;
		updated_at: string;
	}> {
		return this.#db
			.prepare(
				`SELECT job_id, lane_key, state, branch, worktree_path, updated_at FROM lane_jobs WHERE ${includeQuarantined ? "1=1" : "NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = lane_jobs.job_id)"} ORDER BY updated_at DESC, job_id DESC`,
			)
			.all() as Array<{
			job_id: string;
			lane_key: string;
			state: string;
			branch: string;
			worktree_path: string;
			updated_at: string;
		}>;
	}

	readOpenWorkRows(scope: OpenWorkScope): Pick<OpenWorkRows, "laneJobs" | "attempts" | "laneReports"> {
		const quarantine = (table: string, id: string): string => {
			if (scope === "all") return "1 = 1";
			const exists = scope === "quarantined" ? "EXISTS" : "NOT EXISTS";
			return `${exists} (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = ${table}.${id})`;
		};
		const reportQuarantine =
			scope === "all"
				? "1 = 1"
				: `${scope === "quarantined" ? "EXISTS" : "NOT EXISTS"} (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = 'lanejob-' || lower(hex(lane_reports.child_name)))`;
		const read = <T>(sql: string): T[] => {
			try {
				return this.#db.prepare(sql).all() as T[];
			} catch (error) {
				if ((error as Error).message.includes("no such table:")) return [];
				throw error;
			}
		};
		return {
			laneJobs: read<OpenWorkRows["laneJobs"][number]>(
				`SELECT job_id, lane_key, branch, worktree_path, state, record_json, (SELECT cutover_id FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = lane_jobs.job_id) AS cutover_id FROM lane_jobs WHERE ${quarantine("lane_jobs", "job_id")} ORDER BY job_id`,
			),
			attempts: read<OpenWorkRows["attempts"][number]>(
				`SELECT op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json, (SELECT cutover_id FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id) AS cutover_id FROM work_attempt_runtime WHERE ${quarantine("work_attempt_runtime", "job_id")} ORDER BY op_ref`,
			),
			laneReports: read<OpenWorkRows["laneReports"][number]>(
				`SELECT report_id, parent_name, child_name, state, body, (SELECT q.cutover_id FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = 'lanejob-' || lower(hex(lane_reports.child_name)) LIMIT 1) AS cutover_id FROM lane_reports WHERE state IN ('pending','claimed') AND ${reportQuarantine} ORDER BY report_id`,
			),
		};
	}

	laneReportOpenCount(): number {
		return (
			(
				this.#db.prepare("SELECT COUNT(*) AS n FROM lane_reports WHERE state IN ('pending','claimed')").get() as {
					n: number;
				} | null
			)?.n ?? 0
		);
	}

	sweep(tx: WriteTx, cutoffs: { historyCutoff: string; batch: number }): number {
		return this.#db
			.prepare(
				"DELETE FROM lane_reports WHERE rowid IN (SELECT rowid FROM lane_reports WHERE state IN ('consumed','fallback','undeliverable') AND updated_at < ? LIMIT ?)",
			)
			.run(tx, cutoffs.historyCutoff, cutoffs.batch).changes;
	}

	#laneReportGetInside(reportId: string): LaneReportRow | undefined {
		const row = this.#db
			.prepare("SELECT * FROM lane_reports WHERE report_id = ?")
			.get(reportId) as LaneReportRow | null;

		if (!row) return undefined;
		try {
			validateLaneReportRow(row);
			return row;
		} catch {
			throw new WorkAttemptStateError();
		}
	}

	#putLaneReport(tx: WriteTx, report: WorkAttemptAdmissionLane["report"], createdAt: string): void {
		if (this.#assertLaneReportMatches(report)) return;
		this.#db
			.prepare(
				"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, 0, NULL, NULL, ?, ?)",
			)
			.run(
				tx,
				report.reportId,
				report.parentName,
				report.childName,
				report.childOpRef,
				report.body,
				report.root === null ? null : JSON.stringify(report.root),
				createdAt,
				createdAt,
			);
	}

	#assertLaneReportMatches(report: WorkAttemptAdmissionLane["report"]): boolean {
		const existing = this.#laneReportGetInside(report.reportId);
		if (!existing) return false;
		const root = laneReportRoot(existing);
		workAssert(
			existing.parent_name === report.parentName &&
				existing.child_name === report.childName &&
				existing.child_op_ref === report.childOpRef &&
				(root === null ? report.root === null : report.root !== null && root.originKey === report.root.originKey),
		);
		return true;
	}

	laneReportGet(reportId: string): LaneReportRow | undefined {
		return this.#laneReportGetInside(reportId);
	}

	laneReportsByParent(parentName: string): readonly LaneReportRow[] {
		return (
			this.#db
				.prepare("SELECT * FROM lane_reports WHERE parent_name = ? ORDER BY created_at, rowid")
				.all(parentName) as LaneReportRow[]
		).map((row) => {
			try {
				validateLaneReportRow(row);
				return row;
			} catch {
				throw new WorkAttemptStateError();
			}
		});
	}

	laneReportParentNames(): readonly string[] {
		return (
			this.#db
				.prepare(
					"SELECT DISTINCT parent_name FROM lane_reports WHERE state IN ('pending','claimed','held') ORDER BY parent_name",
				)
				.all() as Array<{ parent_name: string }>
		).map((row) => row.parent_name);
	}

	laneReportCounts(parentName: string): { pending: number; claimed: number; held: number; undeliverable: number } {
		const row = this.#db
			.prepare(
				"SELECT SUM(state = 'pending') AS pending, SUM(state = 'claimed') AS claimed, SUM(state = 'held') AS held, SUM(state = 'undeliverable') AS undeliverable FROM lane_reports WHERE parent_name = ?",
			)
			.get(parentName) as {
			pending: number | null;
			claimed: number | null;
			held: number | null;
			undeliverable: number | null;
		} | null;
		return {
			pending: row?.pending ?? 0,
			claimed: row?.claimed ?? 0,
			held: row?.held ?? 0,
			undeliverable: row?.undeliverable ?? 0,
		};
	}

	laneReportClaim(
		tx: WriteTx,
		reportId: string,
		kind: "steer" | "wake",
		ref: string,
		targetOpRef: string | null,
	): LaneReportRow | undefined {
		assertValidOpRef(ref);
		workAssert(kind === "steer" ? targetOpRef !== null : targetOpRef === null);
		if (targetOpRef !== null) assertValidOpRef(targetOpRef);
		const current = this.#laneReportGetInside(reportId);
		if (!current || current.state !== "pending") return undefined;
		const changed = this.#db
			.prepare(
				"UPDATE lane_reports SET state = 'claimed', claim_kind = ?, claim_ref = ?, claim_target_op_ref = ?, claim_seq = claim_seq + 1, hold_reason = NULL, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
			)
			.run(tx, kind, ref, targetOpRef, new Date().toISOString(), reportId, current.claim_seq).changes;
		return changed === 1 ? this.#laneReportGetInside(reportId) : undefined;
	}

	laneReportConsume(tx: WriteTx, reportId: string, ref: string): boolean {
		const current = this.#laneReportGetInside(reportId);
		if (!current) return false;
		if (current.state === "consumed") return current.consumed_op_ref === ref;
		if ((current.state !== "claimed" && current.state !== "held") || current.claim_ref !== ref) return false;
		return (
			this.#db
				.prepare(
					"UPDATE lane_reports SET state = 'consumed', consumed_op_ref = ?, hold_reason = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_seq = ? AND claim_ref = ?",
				)
				.run(tx, ref, new Date().toISOString(), reportId, current.state, current.claim_seq, ref).changes === 1
		);
	}

	laneReportRequeue(tx: WriteTx, reportId: string, ref: string): boolean {
		const current = this.#laneReportGetInside(reportId);
		if (!current || (current.state !== "claimed" && current.state !== "held") || current.claim_ref !== ref)
			return false;
		return (
			this.#db
				.prepare(
					"UPDATE lane_reports SET state = 'pending', claim_kind = NULL, claim_ref = NULL, claim_target_op_ref = NULL, hold_reason = NULL, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_seq = ? AND claim_ref = ?",
				)
				.run(tx, new Date().toISOString(), reportId, current.state, current.claim_seq, ref).changes === 1
		);
	}

	laneReportHold(tx: WriteTx, reportId: string, reason: string): boolean {
		if (!workString(reason, 128)) throw new WorkAttemptStateError();
		const current = this.#laneReportGetInside(reportId);
		if (!current) return false;
		if (current.state === "held") return current.hold_reason === reason;
		if (current.state !== "claimed") return false;
		return (
			this.#db
				.prepare(
					"UPDATE lane_reports SET state = 'held', hold_reason = ?, updated_at = ? WHERE report_id = ? AND state = 'claimed' AND claim_seq = ?",
				)
				.run(tx, reason, new Date().toISOString(), reportId, current.claim_seq).changes === 1
		);
	}

	laneReportFallback(tx: WriteTx, reportId: string, payload: ChatMessagePayload): boolean {
		const current = this.#laneReportGetInside(reportId);
		if (!current || current.state !== "pending") return false;
		const root = laneReportRoot(current);
		workAssert(root !== null);
		const deliveryId = workAttemptDeliveryId(
			this.instanceId,
			laneJobDatabaseId(current.child_name),
			current.child_op_ref,
		);
		workAssert(
			payload.deliveryId === deliveryId &&
				payload.turnId === current.child_op_ref &&
				originKey(payload.origin) === root.originKey &&
				payload.text === current.body &&
				payload.role === "assistant" &&
				payload.final === true &&
				!payload.reaction &&
				!isSilentOutput(payload.text),
		);
		const changed = this.#db
			.prepare(
				"UPDATE lane_reports SET state = 'fallback', updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
			)
			.run(tx, new Date().toISOString(), reportId, current.claim_seq).changes;
		return changed === 1;
	}

	laneReportUndeliverable(tx: WriteTx, reportId: string): boolean {
		const current = this.#laneReportGetInside(reportId);
		if (!current || current.state !== "pending") return false;
		return (
			this.#db
				.prepare(
					"UPDATE lane_reports SET state = 'undeliverable', updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
				)
				.run(tx, new Date().toISOString(), reportId, current.claim_seq).changes === 1
		);
	}
}

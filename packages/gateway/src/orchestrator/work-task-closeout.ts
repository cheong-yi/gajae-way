import {
	type WorkTaskCloseoutEvidence,
	type WorkTaskCloseoutSerializationStatement,
	type WorkTaskCloseoutVerificationStatement,
	type WorkTaskCodingCompletion,
	type WorkTaskCodingCompletionPendingReason,
	type WorkTaskKind,
	type WorkTaskOwnerApproval,
	validateWorkTaskCloseoutSerializationStatement,
	validateWorkTaskCloseoutVerificationStatement,
	validateWorkTaskOwnerApproval,
	WORK_TASK_CODING_COMPLETION_PENDING_REASONS,
} from "@gajae-gateway/protocol";

/** Narrow view of the durable task rows the closeout derivation reads. */
export interface WorkTaskCloseoutTaskView {
	readonly taskId: string;
	readonly opRef: string;
	readonly kind: WorkTaskKind;
	readonly obligation: "awaiting_final" | "final_admitted" | "held";
	readonly terminalReportId: string | null;
	/** The original attempt settled with exact terminal identity; false when uncertain. */
	readonly terminalSettled: boolean;
	/** Retained worktree admission proof common directory, or null when no proof exists. */
	readonly repositoryCommonDir: string | null;
	/** Authenticated owner origins (task coordinator and mapped task surface). */
	readonly ownerOriginKeys: readonly string[];
	/**
	 * Caller-supplied authenticated mapping of configured human principals.
	 * `local-ipc:owner` is accepted without mapping; `coordinator:*` and
	 * `local-ipc:persona` are never owner approval principals.
	 */
	readonly authenticatedHumanPrincipals: readonly string[];
	readonly ownerQuestions: readonly {
		readonly reviewId: string;
		readonly sourceId: string;
		readonly contentHash: string;
	}[];
	/** An unresolved or abandoned administrative disposition keeps completion pending. */
	readonly administrativeUnresolved: boolean;
}

/** Narrow view of an immutable retained source record. */
export interface WorkTaskCloseoutSourceView {
	readonly sourceId: string;
	readonly taskId: string;
	readonly contentHash: string;
	readonly kind: "assignment" | "instruction" | "decision" | "observation";
	readonly body: string;
	readonly completeness: "complete" | "incomplete";
	readonly controlId: string | null;
	readonly principalId: string;
	readonly originKey: string;
	readonly eventId: string;
	readonly editId: string | null;
}

/** Narrow view of an admitted control record proving serialized ownership and order. */
export interface WorkTaskCloseoutControlView {
	readonly controlId: string;
	readonly taskId: string;
	readonly opRef: string;
	readonly sequence: number;
	readonly principalId: string;
	readonly body: string;
	readonly originKey: string;
	readonly eventId: string;
	readonly editId: string | null;
}

/** The retained review record, including optional typed closeout evidence. */
export interface WorkTaskCloseoutReviewView {
	readonly taskId: string;
	readonly expectedOpRef: string;
	readonly reportId: string;
	readonly sourceId: string;
	readonly contentHash: string;
	readonly fullRead: boolean;
	readonly disposition: "no_exception" | "owner_question";
	readonly closeout?: WorkTaskCloseoutEvidence;
}

/** Injected records only: the derivation never reads Git, the store, or the clock. */
export interface WorkTaskCloseoutInput {
	readonly task: WorkTaskCloseoutTaskView;
	readonly review: WorkTaskCloseoutReviewView | null;
	readonly sources: readonly WorkTaskCloseoutSourceView[];
	readonly controls: readonly WorkTaskCloseoutControlView[];
}

type CloseoutTarget = { readonly ref: string; readonly baseCommit: string; readonly resultCommit: string };

function pendingState(pending: ReadonlySet<WorkTaskCodingCompletionPendingReason>): WorkTaskCodingCompletion {
	if (pending.size === 0) return { state: "evidence_admitted", pendingReasons: [] };
	return {
		state: "pending",
		pendingReasons: WORK_TASK_CODING_COMPLETION_PENDING_REASONS.filter((reason) => pending.has(reason)),
	};
}

function sameTarget(left: CloseoutTarget, right: CloseoutTarget): boolean {
	return left.ref === right.ref && left.baseCommit === right.baseCommit && left.resultCommit === right.resultCommit;
}

function sameChecks(
	left: readonly { readonly name: string; readonly outcome: "pass" | "fail" }[],
	right: readonly { readonly name: string; readonly outcome: "pass" | "fail" }[],
): boolean {
	return (
		left.length === right.length &&
		left.every((check, index) => check.name === right[index]?.name && check.outcome === right[index]?.outcome)
	);
}

/** Owner approval principals: the local owner, or a mapped configured human. Never coordinator/persona. */
function approvalPrincipalAllowed(principalId: string, authenticatedHumans: readonly string[]): boolean {
	if (principalId === "local-ipc:owner") return true;
	if (
		principalId.startsWith("coordinator:") ||
		principalId.startsWith("local-ipc:persona") ||
		principalId.startsWith("gateway")
	)
		return false;
	return authenticatedHumans.includes(principalId);
}

function authenticatedInstruction(record: WorkTaskCloseoutSourceView, input: WorkTaskCloseoutInput): boolean {
	return (
		record.kind === "instruction" &&
		record.taskId === input.task.taskId &&
		Boolean(record.principalId && record.eventId) &&
		approvalPrincipalAllowed(record.principalId, input.task.authenticatedHumanPrincipals) &&
		input.task.ownerOriginKeys.includes(record.originKey) &&
		record.controlId !== null &&
		record.sourceId === `instruction-${record.controlId}` &&
		input.controls.some(
			(entry) =>
				entry.controlId === record.controlId &&
				entry.taskId === input.task.taskId &&
				entry.opRef === input.task.opRef &&
				entry.body === record.body &&
				entry.principalId === record.principalId &&
				entry.originKey === record.originKey &&
				entry.eventId === record.eventId &&
				entry.editId === record.editId,
		)
	);
}

function parseBody(body: string, parse: (value: unknown) => unknown): unknown | undefined {
	try {
		return parse(JSON.parse(body) as unknown);
	} catch {
		return undefined;
	}
}

/**
 * Derive the coding closeout projection from existing task, source, control and
 * review records. Admission of retained evidence only: this never runs Git,
 * never executes integration, and never independently proves that integration
 * happened. Read-only and noncode tasks are independent of Git integration.
 *
 * Every evidence claim is checked against a strict structured statement retained
 * in the referenced source body; a bare source reference, an ordinary control
 * sequence, prose, or a reviewer disposition can never certify verification,
 * serialization or owner approval.
 */
export function deriveWorkTaskCodingCompletion(input: WorkTaskCloseoutInput): WorkTaskCodingCompletion {
	const { task, review } = input;
	if (task.kind !== "code_mutating") return { state: "not_applicable", pendingReasons: [] };
	const pending = new Set<WorkTaskCodingCompletionPendingReason>();
	if (task.obligation !== "final_admitted" || task.terminalReportId === null) pending.add("report_not_admitted");
	if (!task.terminalSettled) pending.add("terminal_uncertain");
	if (task.obligation === "held" || task.administrativeUnresolved) pending.add("administrative_unresolved");
	const closeout = review?.closeout;
	if (!review || !closeout) {
		pending.add("evidence_missing");
		return pendingState(pending);
	}
	if (review.disposition === "owner_question" || task.ownerQuestions.length) pending.add("owner_question");
	if (review.taskId !== task.taskId || review.expectedOpRef !== task.opRef || review.reportId !== task.terminalReportId)
		pending.add("mismatch");
	if (review.fullRead !== true) pending.add("evidence_incomplete");
	const reviewed = input.sources.find((source) => source.sourceId === review.sourceId);
	if (!reviewed) pending.add("evidence_incomplete");
	else {
		if (reviewed.taskId !== task.taskId || reviewed.contentHash !== review.contentHash) pending.add("mismatch");
		if (reviewed.completeness !== "complete") pending.add("evidence_incomplete");
	}
	const target = closeout.target;
	const result = closeout.result;
	if (!target) pending.add("target_missing");
	if (!result) pending.add("evidence_incomplete");
	if (!closeout.repository || !task.repositoryCommonDir || closeout.repository.commonDir !== task.repositoryCommonDir)
		pending.add("repository_mismatch");
	const verification = closeout.verification;
	if (!verification || !verification.checks || verification.checks.length === 0) pending.add("verification_incomplete");
	if (verification) {
		if (verification.checks?.some((check) => check.outcome !== "pass")) pending.add("failed_checks");
		if (verification.conflicts === "detected") pending.add("conflicts");
		// The combined verification must cover the exact integrated identity, not
		// the worker result: merge and cherry-pick change the commit.
		if (target && verification.resultingCommit !== target.resultCommit) pending.add("mismatch");
		const record = input.sources.find((source) => source.sourceId === verification.sourceId);
		if (!record) pending.add("verification_incomplete");
		else {
			if (record.taskId !== task.taskId || record.contentHash !== verification.contentHash) pending.add("mismatch");
			if (record.completeness !== "complete") pending.add("evidence_incomplete");
			const statement = parseBody(record.body, validateWorkTaskCloseoutVerificationStatement) as
				| WorkTaskCloseoutVerificationStatement
				| undefined;
			if (!statement) pending.add("verification_unstructured");
			else {
				if (statement.taskId !== task.taskId || statement.opRef !== task.opRef) pending.add("mismatch");
				if (closeout.repository && statement.repository.commonDir !== closeout.repository.commonDir)
					pending.add("mismatch");
				if (target && !sameTarget(statement.target, target)) pending.add("mismatch");
				if (
					result &&
					(statement.result.baseCommit !== result.baseCommit ||
						statement.result.commit !== result.commit ||
						statement.result.diffHash !== result.diffHash)
				)
					pending.add("mismatch");
				if (statement.resultingCommit !== verification.resultingCommit) pending.add("mismatch");
				if (!sameChecks(statement.checks, verification.checks)) pending.add("mismatch");
				if (statement.conflicts !== verification.conflicts) pending.add("mismatch");
			}
		}
	}
	const serialization = closeout.serialization;
	const control = serialization
		? input.controls.find((entry) => entry.controlId === serialization.controlId)
		: undefined;
	if (!serialization || !control) pending.add("serialization_mismatch");
	if (serialization && control) {
		if (
			control.taskId !== task.taskId ||
			control.opRef !== task.opRef ||
			control.sequence !== serialization.sequence ||
			!control.principalId
		)
			pending.add("serialization_mismatch");
		const record = input.sources.find((source) => source.sourceId === serialization.sourceId);
		if (!record) pending.add("serialization_mismatch");
		else {
			if (record.taskId !== task.taskId || record.contentHash !== serialization.contentHash) pending.add("mismatch");
			if (record.controlId !== serialization.controlId) pending.add("serialization_mismatch");
			if (record.completeness !== "complete") pending.add("evidence_incomplete");
			if (!authenticatedInstruction(record, input)) pending.add("serialization_mismatch");
			const statement = parseBody(record.body, validateWorkTaskCloseoutSerializationStatement) as
				| WorkTaskCloseoutSerializationStatement
				| undefined;
			if (!statement) pending.add("serialization_unstructured");
			else {
				if (statement.taskId !== task.taskId || statement.opRef !== task.opRef) pending.add("mismatch");
				if (closeout.repository && statement.repository.commonDir !== closeout.repository.commonDir)
					pending.add("mismatch");
				if (target && !sameTarget(statement.target, target)) pending.add("mismatch");
				if (
					statement.integrationOwner !== control.principalId ||
					(target &&
						(statement.order.baseCommit !== target.baseCommit || statement.order.resultCommit !== target.resultCommit))
				)
					pending.add("serialization_mismatch");
			}
		}
	}
	const approval = closeout.ownerApproval;
	if (!approval) pending.add("approval_missing");
	else {
		const record = input.sources.find((source) => source.sourceId === approval.sourceId);
		if (!record) pending.add("approval_missing");
		else if (record.taskId !== task.taskId || record.contentHash !== approval.contentHash)
			pending.add("approval_mismatch");
		else if (record.completeness !== "complete") pending.add("evidence_incomplete");
		else if (!authenticatedInstruction(record, input)) pending.add("approval_provenance");
		else {
			const selected = input.controls.find((entry) => entry.controlId === record.controlId)!;
			const superseded = input.sources.some((candidate) => {
				const control = input.controls.find((entry) => entry.controlId === candidate.controlId);
				if (!control || control.sequence <= selected.sequence || !authenticatedInstruction(candidate, input))
					return false;
				try {
					const statement = JSON.parse(candidate.body);
					return (
						statement?.kind === "work_task_closeout_approval" &&
						statement.taskId === task.taskId &&
						statement.opRef === task.opRef
					);
				} catch {
					return false;
				}
			});
			if (superseded) pending.add("approval_mismatch");
			const statement = parseBody(record.body, validateWorkTaskOwnerApproval) as WorkTaskOwnerApproval | undefined;
			if (!statement) pending.add("approval_unstructured");
			else {
				if (statement.taskId !== task.taskId || statement.opRef !== task.opRef || statement.action !== closeout.action)
					pending.add("approval_mismatch");
				if (closeout.repository && statement.repository.commonDir !== closeout.repository.commonDir)
					pending.add("approval_mismatch");
				// This exact authenticated owner statement establishes the
				// per-task target; a reviewer-supplied ref has no authority alone.
				if (target && !sameTarget(statement.target, target)) pending.add("approval_mismatch");
				if (result && (statement.result.commit !== result.commit || statement.result.diffHash !== result.diffHash))
					pending.add("approval_mismatch");
				if (result && statement.result.baseCommit !== result.baseCommit) pending.add("approval_mismatch");
				if (
					statement.verification.sourceId !== verification?.sourceId ||
					statement.verification.contentHash !== verification?.contentHash ||
					statement.serialization.sourceId !== serialization?.sourceId ||
					statement.serialization.contentHash !== serialization?.contentHash
				)
					pending.add("approval_mismatch");
				if (
					statement.reviewed.sourceId !== review.sourceId ||
					statement.reviewed.contentHash !== review.contentHash ||
					statement.reviewed.reportId !== review.reportId
				)
					pending.add("approval_mismatch");
				if (
					statement.answers.some(
						(answer) =>
							!task.ownerQuestions.some(
								(question) =>
									question.reviewId === answer.reviewId &&
									question.sourceId === answer.sourceId &&
									question.contentHash === answer.contentHash,
							),
					)
				)
					pending.add("approval_mismatch");
				if (
					review.disposition === "no_exception" &&
					!pending.has("approval_mismatch") &&
					task.ownerQuestions.every((question) =>
						statement.answers.some(
							(answer) =>
								answer.resolution === "resolved" &&
								answer.reviewId === question.reviewId &&
								answer.sourceId === question.sourceId &&
								answer.contentHash === question.contentHash,
						),
					)
				)
					pending.delete("owner_question");
			}
		}
	}
	return pendingState(pending);
}

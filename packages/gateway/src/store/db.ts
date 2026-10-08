import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { realpathSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname, isAbsolute, normalize } from "node:path";
import {
	type ChatMessagePayload,
	isSilentOutput,
	type MonitorEventRecovery,
	type MonitorProtocolFailureRecord,
	type OriginRef,
	originKey,
	PROTOCOL_FAILURE_REASONS,
	type ProtocolFailureReason,
	parseOriginKey,
	validateDeliveryConfirmParams,
	validateOriginRef,
	validateWorkTaskDispositionBasisResult,
	validateWorkTaskDispositionParams,
	validateWorkTaskDispositionRecord,
	validateWorkTaskDispositionResult,
	validateWorkTaskMessageMetadata,
	validateWorkTaskReviewParams,
	type WorkTaskDispositionBasis,
	type WorkTaskDispositionBasisResult,
	type WorkTaskDispositionParams,
	type WorkTaskDispositionRecord,
	type WorkTaskDispositionResult,
	type WorkTaskReviewLocator,
	type WorkTaskReviewParams,
	type WorkTaskReviewPendingResult,
	type WorkTaskReviewResult,
} from "@gajae-gateway/protocol";
import {
	assertValidOpRef,
	type LaneJobRecord,
	type PromptStatusBody,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import { buildDeliveryPayload } from "../delivery/delivery";

export interface BrokerAuthority {
	readonly canonicalAgentDir: string;
	readonly identity: string;
}

export interface OwnedBrokerBinding {
	readonly sessionId: string;
	readonly originKey: string;
	readonly epoch: number;
	readonly repo: string;
	readonly authority: BrokerAuthority;
}

export type BrokerQuarantineKind = "inbound" | "work" | "monitor";

export class BrokerAuthorityError extends Error {
	constructor(
		readonly code:
			| "cutover_required"
			| "authority_mismatch"
			| "unowned_session"
			| "old_work_open"
			| "quarantined"
			| "invalid_authority",
	) {
		super(`broker authority: ${code}`);
		this.name = "BrokerAuthorityError";
	}
}

/**
 * Two spellings of one directory are the same repo. Bindings recorded before the
 * persona workspace was canonicalized carry the symlink path, while the runtime now
 * asks with the resolved one; comparing resolved paths keeps those rows owned
 * without rewriting immutable broker provenance.
 */
function sameRepo(recorded: string, requested: string): boolean {
	if (recorded === requested) return true;
	try {
		return realpathSync(recorded) === realpathSync(requested);
	} catch {
		return false;
	}
}

function brokerAuthorityKey(authority: BrokerAuthority): string {
	if (
		!authority ||
		typeof authority.identity !== "string" ||
		!authority.identity.trim() ||
		typeof authority.canonicalAgentDir !== "string" ||
		!isAbsolute(authority.canonicalAgentDir) ||
		normalize(authority.canonicalAgentDir) !== authority.canonicalAgentDir ||
		authority.identity.includes("\0") ||
		authority.canonicalAgentDir.includes("\0")
	)
		throw new BrokerAuthorityError("invalid_authority");
	return JSON.stringify([authority.canonicalAgentDir, authority.identity]);
}

/** Durable cron catch-up diagnostics for one monitor (issue #157), stored in `meta`. */
export interface MonitorCronState {
	/** Newest slot refused under policy; the cursor never re-considers it. */
	readonly cursor: string;
	/** Slots refused under policy over the monitor's lifetime. */
	readonly skippedTotal: number;
	readonly lastSkip: {
		readonly count: number;
		readonly oldest: string;
		readonly newest: string;
		readonly recordedAt: string;
	};
}

const monitorCronMetaKey = (monitorId: string) => `monitor_cron:${monitorId}`;

const BROKER_SNAPSHOT_TABLES = [
	"sessions",
	"deliveries",
	"recall_snippets",
	"meta",
	"memory_intents",
	"monitors",
	"monitor_events",
	"authored_outputs",
	"inbound_messages",
	"conversation_context",
	"lane_jobs",
	"monitor_failures",
	"monitor_slots",
	"dispatch_leases",
	"conversation_context_state",
	"conversation_model",
	"session_tail_cursors",
	"work_attempt_runtime",
	"lane_reports",
	"work_tasks",
	"work_controls",
	"work_task_sources",
	"broker_owned_bindings",
	"broker_tail_cursors",
	"broker_retired_sessions",
	"broker_quarantine",
] as const;

const REPLAYABLE_INBOUND =
	"NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = inbound_messages.message_id)";
const REPLAYABLE_MONITOR =
	"NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'monitor' AND q.subject_id = monitor_events.event_id)";
const UNRETAINED_TASK_INBOUND =
	"NOT EXISTS (SELECT 1 FROM work_controls c WHERE c.notification_id = inbound_messages.message_id) AND inbound_messages.message_id NOT IN (SELECT value FROM json_each(?))";
const UNRETAINED_TASK_DELIVERY =
	"NOT EXISTS (SELECT 1 FROM work_tasks t WHERE t.op_ref = deliveries.turn_id) AND NOT EXISTS (SELECT 1 FROM work_task_sources s WHERE s.delivery_id = deliveries.delivery_id)";

export type WorkAttemptMode = "start" | "run" | "historical";
export type WorkAttemptDecision =
	| "undecided"
	| "reported"
	| "fallback"
	| "suppressed"
	| "no_target"
	| "wake_unaccepted";
export type WorkAttemptRequestedDecision = "report" | "suppressed" | "no_target" | "wake_unaccepted";
export interface WorkReportRoot {
	readonly originKey: string;
	readonly origin: OriginRef;
}
export type WorkParent =
	| {
			readonly kind: "persona";
			readonly originKey: string;
			readonly origin: OriginRef;
			/** Trigger message ID for threading lane reports (channel root origins only). */
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
export type WorkAttemptPatch = Partial<Pick<WorkAttemptRuntime, "sendPhase" | "sendEvidence" | "terminal" | "output">>;
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

export type WorkTaskKind = "read_only" | "code_mutating";
export interface WorkTaskEvidence {
	readonly principalId: string;
	readonly origin: OriginRef;
	readonly eventId: string;
	readonly editId: string | null;
	readonly evidenceAt: string;
	readonly observedAt: string;
}
export interface WorkTaskRequest {
	readonly text: string;
	readonly context?: string;
	readonly kind: WorkTaskKind;
	readonly cwd: string;
	readonly model?: GjcModelSelection;
	readonly coordinator: OriginRef;
	readonly surface: { readonly thread: OriginRef } | { readonly parent: OriginRef; readonly title?: string };
	readonly evidence: WorkTaskEvidence;
}
export interface WorkTask {
	readonly taskId: string;
	readonly laneName: string;
	readonly jobId: string;
	readonly opRef: string;
	readonly request: WorkTaskRequest;
	readonly requestHash: string;
	readonly thread: OriginRef | null;
	readonly surfacePhase: "pending" | "claimed" | "bound" | "held";
	readonly surfaceClaimId: string | null;
	readonly surfaceHoldReason: string | null;
	readonly dispatchPhase: "pending" | "prepared";
	readonly sessionId: string | null;
	readonly epoch: number | null;
	readonly obligationState: "awaiting_final" | "final_admitted" | "held";
	readonly terminalReportId: string | null;
	readonly reportDisposition: WorkAttemptDecision | null;
	readonly reportAdmittedAt: string | null;
	readonly holdReason: string | null;
	readonly createdAt: string;
	readonly updatedAt: string;
	readonly version: number;
}
/** Independently retained admission scope, never a validated runtime task or execution capability. */
export interface WorkTaskQualifiedAdmissionScope {
	readonly qualification: "retained_admission";
	readonly taskId: string;
	readonly requestHash: string;
	readonly request: Pick<WorkTaskRequest, "kind" | "cwd" | "coordinator">;
}
export interface WorkTaskAttemptIdentity {
	readonly opRef: string;
	readonly sessionId: string;
	readonly epoch: number;
}
export interface WorkControlRequest {
	readonly taskId: string;
	readonly expectedOpRef: string;
	readonly kind: "steer" | "cancel_request" | "reset_notice";
	readonly scope: WorkTaskKind;
	readonly body: string;
	readonly evidence: WorkTaskEvidence;
}
/** Internal manager proof after owner authentication and dedicated-worktree validation.
 * This is not a wire capability; the store checks identity, never Git/OS facts.
 */
export interface WorkControlScopeAdmission {
	readonly taskId: string;
	readonly opRef: string;
	readonly cwd: string;
	readonly kind: "code_mutating";
	readonly eventId: string;
	readonly sourceId: string;
	readonly validatedAt: string;
}
/** Internal proof of an authenticated, explicitly named target. Source origin remains
 * the actual caller's origin; this is not a public routing or authority override.
 */
export interface WorkControlRouteAdmission {
	readonly taskId: string;
	readonly opRef: string;
	readonly thread: OriginRef;
	readonly sourceId: string;
}
export interface WorkControl {
	readonly controlId: string;
	readonly request: WorkControlRequest;
	readonly requestHash: string;
	readonly sequence: number;
	readonly clientRef: string | null;
	readonly notificationId: string;
	readonly notificationDisposition: { readonly kind: "inbound" | "delivery"; readonly id: string } | null;
	readonly sessionId: string | null;
	readonly epoch: number | null;
	readonly phase: "pending" | "sending" | "accepted" | "refused" | "held";
	readonly reason: string | null;
	readonly sendingAt: string | null;
	readonly receipt: WorkControlReceipt | null;
	readonly createdAt: string;
	readonly updatedAt: string;
	readonly version: number;
}
/** A caller-validated transport receipt, not proof of mandate completion. */
export interface WorkControlReceipt extends WorkTaskAttemptIdentity {
	readonly source: "turn.steer" | "turn.steer_status";
	readonly clientRef: string;
	readonly eventId: string;
	readonly outcome: "accepted" | "refused";
	readonly observedAt: string;
	readonly evidence: string;
}
export interface WorkTaskSourceInput {
	readonly sourceId: string;
	readonly taskId: string;
	readonly kind: "assignment" | "instruction" | "decision" | "observation";
	readonly body: string;
	readonly evidence: WorkTaskEvidence;
	readonly supersedes: string | null;
	readonly completeness: "complete" | "incomplete";
	readonly controlId: string | null;
	readonly reportId: string | null;
	/** Written only by the identity-fenced administrative transaction. */
	readonly administrative?: WorkTaskDispositionRecord;
	/** Coordinator-authored judgment; never owner evidence or execution success. */
	readonly review?: WorkTaskReviewParams;
}
/** Immutable retained source material; no consuming inbox read is needed. */
export interface WorkTaskSource extends WorkTaskSourceInput {
	readonly sequence: number;
	readonly contentHash: string;
	readonly deliveryId: string | null;
}
export type WorkTaskAdmission<T> =
	| { readonly disposition: "created" | "duplicate"; readonly record: T }
	| { readonly disposition: "conflict"; readonly record: T };
export class WorkTaskStateError extends Error {
	constructor(readonly code: "invalid" | "identity" | "transition" | "order" | "conflict" | "unavailable") {
		super(`work task: ${code}`);
		this.name = "WorkTaskStateError";
	}
}

function taskAssert(condition: unknown, code: WorkTaskStateError["code"] = "invalid"): asserts condition {
	if (!condition) throw new WorkTaskStateError(code);
}
function taskText(value: unknown, bytes: number): value is string {
	return (
		typeof value === "string" &&
		value.trim().length > 0 &&
		!value.includes("\0") &&
		Buffer.byteLength(value, "utf8") <= bytes
	);
}
/** Canonical JSON makes property order irrelevant to immutable request deduplication. */
function taskJson(value: unknown): string {
	if (Array.isArray(value)) return `[${value.map(taskJson).join(",")}]`;
	if (value !== null && typeof value === "object")
		return `{${Object.entries(value)
			.filter(([, item]) => item !== undefined)
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
			.map(([key, item]) => `${JSON.stringify(key)}:${taskJson(item)}`)
			.join(",")}}`;
	const json = JSON.stringify(value);
	taskAssert(json !== undefined);
	return json;
}
function taskHash(domain: string, value: unknown): string {
	return createHash("sha256")
		.update(taskJson([domain, value]), "utf8")
		.digest("hex");
}
/** Transport observations are attribution, not a new assignment. Keep the first evidence verbatim. */
function taskRequestHash(request: WorkTaskRequest): string {
	return taskHash("work-task-assignment", {
		text: request.text,
		context: request.context,
		kind: request.kind,
		cwd: request.cwd,
		model: request.model,
		coordinator: request.coordinator,
		surface: request.surface,
		principalId: request.evidence.principalId,
		origin: request.evidence.origin,
	});
}
/** The event/edit identity dedupes delivery; receipt time cannot change its semantic payload. */
function controlRequestHash(request: WorkControlRequest): string {
	return taskHash("work-control-payload", {
		taskId: request.taskId,
		expectedOpRef: request.expectedOpRef,
		kind: request.kind,
		scope: request.scope,
		body: request.body,
		principalId: request.evidence.principalId,
		origin: request.evidence.origin,
		eventId: request.evidence.eventId,
		editId: request.evidence.editId,
	});
}
function workControlEventKey(request: WorkControlRequest): string {
	return taskHash("work-control-event", [
		request.taskId,
		originKey(request.evidence.origin),
		request.evidence.eventId,
		request.evidence.editId,
	]);
}
export function workTaskSourceDeliveryId(taskId: string, sourceId: string, thread: OriginRef): string {
	return `work-task-${taskHash("work-task-delivery", [taskId, sourceId, originKey(thread)])}`;
}
export function workTaskDispositionId(taskId: string, eventId: string, origin: OriginRef): string {
	return `disposition-${taskHash("work-task-disposition", [taskId, eventId, originKey(origin)])}`;
}

export function workTaskDispositionText(request: WorkTaskDispositionParams): string {
	return (
		`Administrative ${request.outcome}: ${request.reason}\n` +
		`Evidence (${request.evidence.availability}): ${request.evidence.detail}\n` +
		(request.validationUnavailable
			? `Retained validation-unavailable evidence ${request.validationUnavailable.sourceId}; stored admission only, not SDK proof.\n`
			: "") +
		"Execution unchanged; original uncertainty and obligations retained."
	);
}

function validateAdministrativeSource(source: WorkTaskSourceInput): void {
	if (source.administrative === undefined) {
		taskAssert(!/^disposition-[0-9a-f]{64}$/.test(source.sourceId), "identity");
		return;
	}
	const record = validateWorkTaskDispositionRecord(source.administrative);
	taskAssert(
		source.kind === "decision" &&
			source.supersedes === null &&
			source.completeness === "incomplete" &&
			source.body === workTaskDispositionText(record.request) &&
			record.request.taskId === source.taskId &&
			source.sourceId === workTaskDispositionId(source.taskId, record.request.eventId, record.origin) &&
			record.principalId === source.evidence.principalId &&
			taskJson(record.origin) === taskJson(source.evidence.origin) &&
			record.request.eventId === source.evidence.eventId &&
			source.evidence.editId === null &&
			record.recordedAt === source.evidence.observedAt &&
			source.controlId === (record.request.target.kind === "control" ? record.request.target.controlId : null) &&
			source.reportId === (record.request.target.kind === "report" ? record.request.target.reportId : null),
		"identity",
	);
}
function validateTaskEvidence(value: WorkTaskEvidence): void {
	taskAssert(value && typeof value === "object");
	taskAssert(
		workString(value.principalId) &&
			workString(value.eventId, 1024) &&
			Buffer.byteLength(value.eventId, "utf8") <= 1024,
	);
	taskAssert(value.editId === null || workString(value.editId));
	taskAssert(workTime(value.evidenceAt) && workTime(value.observedAt));
	validateOriginRef(value.origin);
}
function validateTaskRequest(value: WorkTaskRequest): void {
	taskAssert(value && typeof value === "object" && taskText(value.text, 16 * 1024));
	taskAssert(value.context === undefined || taskText(value.context, 16 * 1024));
	taskAssert(["read_only", "code_mutating"].includes(value.kind));
	taskAssert(workString(value.cwd, 4096) && isAbsolute(value.cwd) && normalize(value.cwd) === value.cwd);
	taskAssert(value.model === undefined || parseModelSelection(value.model) !== undefined);
	validateOriginRef(value.coordinator);
	taskAssert(["discord", "slack", "telegram", "loopback"].includes(value.coordinator.platform));
	validateTaskEvidence(value.evidence);
	taskAssert(value.surface && typeof value.surface === "object");
	taskAssert("thread" in value.surface !== "parent" in value.surface);
	const surface = "thread" in value.surface ? value.surface.thread : value.surface.parent;
	validateOriginRef(surface);
	taskAssert(surface.platform === "discord");
	if ("thread" in value.surface) taskAssert(surface.kind === "thread");
	else
		taskAssert(
			surface.kind === "channel" &&
				(value.surface.title === undefined ||
					(taskText(value.surface.title, 400) && value.surface.title.length <= 100)),
		);
	taskAssert(Buffer.byteLength(taskJson(value), "utf8") <= 96 * 1024);
}
function validateTask(value: WorkTask): void {
	taskAssert(value && typeof value === "object");
	taskAssert(/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value.taskId));
	taskAssert(value.laneName === `fm-${value.taskId}` && value.jobId === laneJobDatabaseId(value.laneName), "identity");
	assertValidOpRef(value.opRef);
	validateTaskRequest(value.request);
	taskAssert(value.requestHash === taskRequestHash(value.request), "identity");
	taskAssert(["pending", "claimed", "bound", "held"].includes(value.surfacePhase));
	taskAssert(value.surfaceClaimId === null || workString(value.surfaceClaimId));
	taskAssert(value.surfaceHoldReason === null || taskText(value.surfaceHoldReason, 2048));
	taskAssert((value.surfacePhase === "held") === (value.surfaceHoldReason !== null));
	taskAssert(value.surfacePhase !== "claimed" || value.surfaceClaimId !== null);
	taskAssert((value.surfacePhase === "bound") === (value.thread !== null));
	if (value.thread !== null) {
		validateOriginRef(value.thread);
		taskAssert(value.thread.platform === "discord" && value.thread.kind === "thread");
		if ("thread" in value.request.surface)
			taskAssert(taskJson(value.thread) === taskJson(value.request.surface.thread), "identity");
	}
	taskAssert(["pending", "prepared"].includes(value.dispatchPhase));
	taskAssert(
		value.dispatchPhase === "pending"
			? value.sessionId === null && value.epoch === null
			: workString(value.sessionId) && Number.isSafeInteger(value.epoch) && value.epoch! >= 0 && value.thread !== null,
	);
	taskAssert(["awaiting_final", "final_admitted", "held"].includes(value.obligationState));
	taskAssert((value.obligationState === "held") === (value.holdReason !== null));
	taskAssert(value.holdReason === null || taskText(value.holdReason, 2048));
	taskAssert(value.terminalReportId === null || workString(value.terminalReportId));
	taskAssert(
		value.reportDisposition === null ||
			["undecided", "reported", "fallback", "suppressed", "no_target", "wake_unaccepted"].includes(
				value.reportDisposition,
			),
	);
	taskAssert(value.reportAdmittedAt === null || workTime(value.reportAdmittedAt));
	taskAssert(
		value.obligationState !== "final_admitted" ||
			(value.terminalReportId !== null &&
				value.reportAdmittedAt !== null &&
				value.reportDisposition !== null &&
				value.reportDisposition !== "undecided"),
	);
	taskAssert(workTime(value.createdAt) && workTime(value.updatedAt));
	taskAssert(Number.isSafeInteger(value.version) && value.version >= 0);
}

/**
 * The only permitted terminal rewrite (#248): broker evidence observed with
 * receiptState=missing is upgraded to present, in the same write that stores
 * this op's proven final body. The SDK receipt state is monotonic, so the
 * rest of the terminal status must be unchanged.
 */
function lateReceiptReconciled(current: WorkAttemptRuntime, next: WorkAttemptRuntime): boolean {
	const before = current.terminal;
	const after = next.terminal;
	return (
		before?.kind === "broker" &&
		after?.kind === "broker" &&
		before.observedAt === after.observedAt &&
		before.status?.receiptState === "missing" &&
		JSON.stringify({ ...before.status, receiptState: "present" }) === JSON.stringify(after.status) &&
		current.output.proof === null &&
		next.output.proof !== null &&
		(next.output.disposition === "available" || next.output.disposition === "silent")
	);
}

/**
 * Exact original terminal status re-observed for a locally settled attempt:
 * explicit present receipt on this op's clientRef, coherent times bounded by
 * the attempt start and the observation, and stable command/turn identity for
 * binding the stored output. Missing evidence never qualifies.
 */
export function localSettledTerminalStatus(
	runtime: Pick<WorkAttemptRuntime, "opRef" | "startedAt">,
	status: PromptStatusBody | undefined,
	observedAtMs: number,
): status is PromptStatusBody {
	if (!status) return false;
	const startMs = Date.parse(runtime.startedAt);
	const terminalAt = status.terminalAt;
	if (
		!Number.isFinite(startMs) ||
		!Number.isFinite(observedAtMs) ||
		status.status !== "terminal_ok" ||
		status.receiptState !== "present" ||
		status.clientRef !== runtime.opRef ||
		typeof status.commandId !== "string" ||
		status.commandId.length === 0 ||
		typeof status.turnId !== "string" ||
		status.turnId.length === 0 ||
		terminalAt === undefined ||
		!Number.isFinite(terminalAt) ||
		terminalAt < startMs ||
		terminalAt > observedAtMs
	)
		return false;
	for (const time of [status.acceptedAt, status.startedAt]) {
		if (time !== undefined && (!Number.isFinite(time) || time < startMs || time > terminalAt)) return false;
	}
	return status.acceptedAt === undefined || status.startedAt === undefined || status.acceptedAt <= status.startedAt;
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

function workAccepted(runtime: WorkAttemptRuntime): boolean {
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
function workAssert(condition: unknown, assertion = "work attempt payload validation"): asserts condition {
	if (!condition) throw new WorkAttemptStateError(undefined, assertion);
}
function workTime(value: unknown): boolean {
	return typeof value === "string" && value.length <= 40 && Number.isFinite(Date.parse(value));
}
function workString(value: unknown, max = 512): value is string {
	if (typeof value !== "string" || value.length === 0 || value.length > max) return false;
	for (let index = 0; index < value.length; index++) {
		if (value.charCodeAt(index) < 32) return false;
	}
	return true;
}
function validateWorkRoot(root: WorkReportRoot): void {
	workAssert(root && typeof root === "object");
	validateOriginRef(root.origin);
	workAssert(originKey(root.origin) === root.originKey);
	workAssert(["discord", "slack", "telegram", "loopback"].includes(root.origin.platform));
}

function validateWorkParent(parent: WorkParent, mode: WorkAttemptMode): void {
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
function validateWorkRuntime(value: WorkAttemptRuntime, instanceId: string): void {
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
		["undecided", "reported", "fallback", "suppressed", "no_target", "wake_unaccepted"].includes(value.decision),
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
	}
	workAssert(Buffer.byteLength(JSON.stringify(value), "utf8") <= 16384);
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

/**
 * Resolve the lane report origin for a persona parent. If the parent is a channel root
 * origin with a triggerMessageId, route the report to a thread instead of the channel root.
 */
function resolveLaneReportOrigin(parent: WorkParent | null, admissionOriginRefJson: string): string {
	if (parent?.kind !== "persona" || !parent.triggerMessageId) return admissionOriginRefJson;

	try {
		const origin = JSON.parse(admissionOriginRefJson) as OriginRef;
		if (origin.kind !== "channel") return admissionOriginRefJson;
		// Slack message ids are already `<channel>:<ts>`, which is exactly the thread conversation id;
		// a Discord thread started from a message shares that message's id.
		if (origin.platform === "slack" && !parent.triggerMessageId.startsWith(`${origin.conversationId}:`)) {
			return admissionOriginRefJson;
		}
		if (origin.platform !== "slack" && origin.platform !== "discord") return admissionOriginRefJson;
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
/**
 * A gjc model selection: either an explicit selector string or a model profile
 * preset. Structurally identical to the config type; duplicated as a local
 * shape so the store layer does not import the config module.
 */
export type GjcModelSelection = string | { readonly preset: string };

export interface ConversationModelRecord {
	readonly selection: GjcModelSelection;
	readonly setBy?: string;
	readonly updatedAt: string;
}

/**
 * Validates a stored selection. Fails closed on anything unexpected: a bad row
 * must not become argv for a gjc spawn.
 */
export function parseModelSelection(value: unknown): GjcModelSelection | undefined {
	if (typeof value === "string") return value.trim() === "" ? undefined : value;
	if (typeof value !== "object" || value === null) return undefined;
	const keys = Object.keys(value);
	if (keys.length !== 1 || keys[0] !== "preset") return undefined;
	const preset = (value as { preset?: unknown }).preset;
	if (typeof preset !== "string" || preset.trim() === "") return undefined;
	return { preset };
}

export const INBOUND_TURN_STATES = ["bound", "accepted", "done"] as const;
export type InboundTurnState = (typeof INBOUND_TURN_STATES)[number];
export const INBOUND_TURN_ROLES = ["trigger", "steer"] as const;
export type InboundTurnRole = (typeof INBOUND_TURN_ROLES)[number];

/**
 * One inbound platform message. Canonical flow: a message is either steered
 * into the origin's running turn (role `steer`, done on receipt) or becomes
 * the trigger of the next turn (role `trigger`); there is no coalescing, no
 * settle window and no expiry - an unanswered row stays `pending` until a
 * turn takes it.
 */
export interface InboundMessageRow {
	readonly message_id: string;
	readonly source: "platform" | "lane_report";
	readonly origin_key: string;
	readonly origin_ref_json: string;
	readonly body: string;
	readonly engagement_json: string | null;
	readonly state: "pending" | "processing" | "done";
	readonly received_at: string;
	readonly turn_role: InboundTurnRole | null;
	readonly turn_epoch: number | null;
	/**
	 * trigger: bound (session chosen, send not acknowledged) -> accepted (the
	 * runtime holds the op) -> done. steer: bound (the steer was ISSUED but the
	 * transport tore before an answer; the message may be inside the turn) ->
	 * done (recorded acceptance), or back to NULL (definitive refusal). A
	 * `bound` steer is never dispatched as a turn of its own.
	 */
	readonly turn_state: InboundTurnState | null;
	readonly turn_op_ref: string | null;
	/** Bound before send so an accepted retired turn can reattach after restart. */
	readonly bound_session_id: string | null;
	/** Stamped at bind, before the send; the earliest wall clock this turn's answer can carry. */
	readonly dispatched_at: string | null;
	/**
	 * JSON map part -> ledger delivery id that satisfied that terminal reply slot.
	 * A turn that closed without any delivery carries `{"none": reason}` instead;
	 * a `done` trigger with NULL here is a ledger defect (see inboundTerminalLinkAudit).
	 */
	readonly terminal_delivery_id: string | null;
}

/** Why a closed turn has no terminal delivery; recorded as `{"none": reason}`. */
export type TerminalUnlinkedReason = "silent" | "loopback" | "turn_failed" | "retired" | "no_delivery";

/** Meta key prefix of the per-origin failed-turn markers that `/new` carries over (#409). */
function failedTurnKeyPrefix(originKey: string): string {
	return `turn-failed:${originKey}:`;
}

/** Delivery ids that answered a turn, from its `terminal_delivery_id` JSON; empty for a sentinel or NULL. */
export function terminalDeliveryIds(json: string | null): string[] {
	if (!json) return [];
	try {
		const parsed: unknown = JSON.parse(json);
		if (typeof parsed !== "object" || parsed === null) return [];
		return Object.entries(parsed)
			.filter(([part, id]) => /^\d+$/.test(part) && typeof id === "string" && id.length > 0)
			.map(([, id]) => id as string);
	} catch {
		return [];
	}
}

/** A trigger row's turn, as the actor tracks it. */
export interface InboundTurn {
	readonly originKey: string;
	readonly epoch: number;
	readonly state: Extract<InboundTurnState, "bound" | "accepted">;
	readonly opRef: string;
	readonly sessionId: string | null;
	readonly triggerMessageId: string;
}

/** A second nonterminal trigger in one epoch would violate at-most-one running turn. */
export class InboundTurnConflictError extends Error {
	constructor(originKey: string, epoch: number) {
		super(`origin ${originKey} already has a nonterminal turn in epoch ${epoch}`);
		this.name = "InboundTurnConflictError";
	}
}

const LATEST_SCHEMA_VERSION = 33;

/** Maximum number of prior messages supplied to one engaged conversation turn. */
export const CONVERSATION_DIFF_MAX_ROWS = 60;
/** Maximum age of prior messages supplied to one engaged conversation turn. */
export const CONVERSATION_DIFF_MAX_AGE_MS = 6 * 60 * 60 * 1000;
/** Consumed context bodies older than this are deleted after aggregate evidence is retained. */
export const CONVERSATION_CONTEXT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** Terminal history (finished inbound turns, settled monitor events, spent lane reports, old slots) is deleted after this. */
export const HISTORY_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
/** Confirmed deliveries are only needed for recent-chat context and dedupe. */
export const CONFIRMED_DELIVERY_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** Rows deleted per table per retention batch; keeps each synchronous transaction short. */
export const RETENTION_BATCH_ROWS = 500;

export interface RetentionSweepResult {
	readonly deleted: Readonly<Record<string, number>>;
	/** True when any table hit the batch limit, so another batch may find more. */
	readonly more: boolean;
}

function freshTurnMetaKey(originKey: string, epoch: number, triggerMessageId: string): string {
	return `fresh_turn_attempt:${originKey}:${epoch}:${triggerMessageId}`;
}

function inboundTurnBlockMetaKey(opRef: string): string {
	return `inbound_turn_block:${opRef}`;
}

function failedTurnResetCapKey(originKey: string): string {
	return `failed-turn-reset-cap:${createHash("sha256")
		.update(JSON.stringify([originKey]))
		.digest("hex")}`;
}

export interface ConversationContextRow {
	readonly message_id: string;
	readonly author_id: string | null;
	readonly author_name: string | null;
	readonly body: string;
	readonly received_at: string;
}

export interface ConversationContextDiagnostics {
	readonly unread: number;
	readonly expired: number;
	readonly truncated: number;
	readonly omittedOldestAt: string | null;
	readonly omittedNewestAt: string | null;
	readonly floorAt: string | null;
}

export interface ConversationContextWindow {
	readonly rows: readonly ConversationContextRow[];
	readonly selectedMessageIds: readonly string[];
	readonly effectiveFloor: string;
	readonly expiredCount: number;
	readonly truncatedCount: number;
	readonly omittedOldestAt: string | null;
	readonly omittedNewestAt: string | null;
	readonly omissionRevision: number;
	readonly diagnostics: ConversationContextDiagnostics;
}

function parseStringList(value: string): readonly string[] {
	try {
		const parsed = JSON.parse(value);
		return Array.isArray(parsed) && parsed.every((item) => typeof item === "string") ? parsed : ["projection_corrupt"];
	} catch {
		return ["projection_corrupt"];
	}
}
/** A scheduled cron slot the gateway claimed or fired for one monitor. */
export interface MonitorSlotRow {
	readonly monitor_id: string;
	readonly slot_at: string;
	readonly created_at: string;
}

/** Public-safe dispatch failure evidence (never a raw error body). */
export interface MonitorFailureRow {
	readonly event_id: string;
	readonly code: string;
	readonly detail: string;
	readonly failed_at: string;
	readonly protocol_reason: ProtocolFailureReason | null;
	readonly response_byte_length: number | null;
	readonly response_entry_count: number | null;
}

interface MonitorFailureOptions {
	readonly protocolFailure?: Pick<MonitorProtocolFailureRecord, "reason" | "responseByteLength" | "responseEntryCount">;
}

export class DatabaseStartupError extends Error {
	readonly code: "newer_schema" | "integrity_check_failed" | "migration_corrupt";
	constructor(code: DatabaseStartupError["code"], message: string) {
		super(message);
		this.name = "DatabaseStartupError";
		this.code = code;
	}
}

/**
 * Durable monitor-event stage contract (fail-closed). `monitorEventUpdate`
 * rejects any stage outside this set, so a typo or a corrupt write cannot
 * invent a state the recovery logic does not know how to reclaim.
 *
 * - admitted: durable row exists, not yet claimed by a dispatch.
 * - batched: claimed by a dispatch awaiting its authoring turn (a live stage,
 *   minutes at most) or stranded there by a crash mid-dispatch.
 * - dispatched: authoring turn sent, output not yet parsed.
 * - authored: note authored; delivery to the target is pending or settled.
 * - delivered: the batch's ledger delivery was confirmed by the adapter.
 * - authored_no_delivery: terminal — nothing will ever be delivered: the
 *   monitor has no channel target and no owner target is configured, or the
 *   authored note is a silence token. Explicit instead of `authored`-forever
 *   so the operator sees a finished state.
 * - failed_no_retry also covers an authored note whose delivery expired
 *   (`monitor_failures` code `delivery_expired`).
 * - failed: the last dispatch attempt failed; reconcile redispatches.
 * - failed_no_retry: dispatch failed and reconcile will not retry it again
 *   (reclaim budget exhausted); operator-visible terminal state.
 * - skipped: terminal — the monitor's overlap policy is `skip` and an earlier
 *   event of the same monitor was still awaiting authoring when this one
 *   fired (`skipped_by` names it). A scheduling outcome, never a failure.
 */
export const MONITOR_EVENT_STAGES = [
	"admitted",
	"batched",
	"dispatched",
	"authored",
	"delivered",
	"authored_no_delivery",
	"failed",
	"failed_no_retry",
	"skipped",
] as const;
export type MonitorEventStage = (typeof MONITOR_EVENT_STAGES)[number];
/** Stages whose rows reconcile() may claim and redispatch. */
export const RECONCILABLE_STAGES: readonly MonitorEventStage[] = ["admitted", "batched", "dispatched", "failed"];
/** Terminal stages: no further transition will ever happen without operator action. */
export const TERMINAL_STAGES: readonly MonitorEventStage[] = [
	"delivered",
	"authored_no_delivery",
	"failed_no_retry",
	"skipped",
];

/**
 * Stages in which an event still owes an authoring turn: the overlap policy's
 * definition of "a previous event of the same monitor is still in flight".
 * `authored` is excluded: its turn is done and only delivery is pending.
 */
const IN_FLIGHT_MONITOR_STAGES = "'admitted','batched','dispatched','failed'";

export interface DeliveryDbRow {
	readonly delivery_id: string;
	readonly turn_id: string;
	readonly origin_key: string;
	readonly payload_json: string;
	readonly state: string;
	readonly attempts: number;
	readonly created_at: string;
	readonly updated_at: string;
	readonly last_error: string | null;
}

export interface MonitorEventDbRow {
	readonly event_id: string;
	readonly monitor_id: string;
	readonly event_type: string;
	readonly payload_json: string;
	readonly fired_at: string;
	readonly stage: string;
	readonly batch_id: string | null;
	readonly dispatch_attempts: number;
	readonly skipped_by: string | null;
	readonly updated_at: string;
	readonly procedure_json: string | null;
}

export interface MemoryIntentDbRow {
	readonly id: string;
	readonly kind: string;
	readonly payload_json: string;
	readonly state: "queued" | "written" | "committed" | "receipted" | "quarantined";
	readonly attempts: number;
	readonly quarantine_reason: string | null;
}

const DELIVERY_COLUMNS =
	"delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at, last_error";
/** Matches the partial index `monitor_events_open`; keep the two spellings identical. */
const OPEN_MONITOR_EVENT = "stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped')";
/** Terminal stages as SQL; must mirror TERMINAL_STAGES and the `monitor_events_gc` partial index predicate. */
const TERMINAL_MONITOR_EVENT = "stage IN ('delivered','authored_no_delivery','failed_no_retry','skipped')";

/**
 * Minimum wait, measured from the last failure (`updated_at`), before reconcile reclaims a
 * `failed` event for its (index+1)-th time. The schedule spans ~17h so a slot survives a
 * multi-hour dispatch outage instead of burning its budget on consecutive 60s sweeps (#179).
 */
export const MONITOR_EVENT_RETRY_BACKOFF_MS: readonly number[] = [
	0,
	10 * 60_000,
	60 * 60_000,
	4 * 60 * 60_000,
	12 * 60 * 60_000,
];
/** Upper bound on how often a single event may be reclaimed by reconcile. */
export const MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS = MONITOR_EVENT_RETRY_BACKOFF_MS.length;

export class GatewayDatabase {
	readonly #database: Database;
	#inTransaction = false;

	/** Read-only census; JSON corruption is an error, never evidence of quiescence. */
	inspectBrokerAuthority(): {
		authority: BrokerAuthority | null;
		populated: boolean;
		openInbound: number;
		openWork: number;
		openMonitors: number;
	} {
		const authority = this.#brokerAuthority();
		let populated = false;
		for (const table of BROKER_SNAPSHOT_TABLES) {
			const where = table === "meta" ? " WHERE key <> 'instance_id'" : "";
			try {
				if (this.#database.query(`SELECT 1 FROM ${table}${where} LIMIT 1`).get()) populated = true;
			} catch (error) {
				// Table doesn't exist yet (called before migrations complete); ignore and continue.
				if (!(error as Error).message.includes("no such table")) throw error;
			}
		}
		try {
			if (this.#database.query("SELECT 1 FROM broker_cutovers LIMIT 1").get()) populated = true;
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		try {
			for (const row of this.#database.query<{ epoch: number }, []>("SELECT epoch FROM sessions").all()) {
				if (!Number.isSafeInteger(row.epoch) || row.epoch < 0 || row.epoch >= Number.MAX_SAFE_INTEGER)
					throw new Error("invalid session epoch");
			}
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openWork = 0;
		try {
			for (const row of this.#database
				.query<{ job_id: string; record_json: string }, []>("SELECT job_id, record_json FROM lane_jobs")
				.all()) {
				const job = parseLaneJobRecord(row.record_json);
				if (job.jobId !== row.job_id) throw new WorkAttemptStateError();
				if (
					!this.isBrokerQuarantined("work", job.jobId) &&
					(job.attempts.some((attempt) => attempt.endedAt === undefined) || !["done", "aborted"].includes(job.state))
				)
					openWork++;
			}
			for (const row of this.#database.query<{ op_ref: string }, []>("SELECT op_ref FROM work_attempt_runtime").all()) {
				const runtime = this.workAttemptGet(row.op_ref)!;
				if (runtime.settledAt === null && !this.isBrokerQuarantined("work", runtime.jobId)) openWork++;
			}
			openWork +=
				this.#database
					.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM lane_reports WHERE state IN ('pending','claimed')")
					.get()?.n ?? 0;
			for (const row of this.#database.query<{ task_id: string }, []>("SELECT task_id FROM work_tasks").all()) {
				const task = this.workTaskGet(row.task_id)!;
				if (
					!this.isBrokerQuarantined("work", task.jobId) &&
					(task.dispatchPhase === "pending" ||
						task.obligationState !== "final_admitted" ||
						this.#database
							.query("SELECT 1 FROM work_controls WHERE task_id = ? AND phase NOT IN ('accepted','refused') LIMIT 1")
							.get(task.taskId))
				)
					openWork++;
			}
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openInbound = 0;
		try {
			openInbound = this.#database
				.query<{ n: number }, []>(
					`SELECT COUNT(*) AS n FROM inbound_messages WHERE (state <> 'done' OR turn_state IN ('bound','accepted')) AND ${REPLAYABLE_INBOUND}`,
				)
				.get()!.n;
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		let openMonitors = 0;
		try {
			openMonitors = this.#database
				.query<{ n: number }, []>(
					"SELECT COUNT(*) AS n FROM monitor_events WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped') AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'monitor' AND q.subject_id = monitor_events.event_id)",
				)
				.get()!.n;
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		return { authority, populated, openInbound, openWork, openMonitors };
	}

	/** Established authority checks are constant-size; initialization alone requires the full census. */
	assertBrokerAuthority(authority: BrokerAuthority, options: { initializeEmpty?: boolean } = {}): void {
		const key = brokerAuthorityKey(authority);
		const matchesEstablished = () => {
			const current = this.#brokerAuthority();
			if (current === null) return false;
			if (brokerAuthorityKey(current) !== key) throw new BrokerAuthorityError("authority_mismatch");
			return true;
		};
		if (matchesEstablished()) return;
		if (!options.initializeEmpty) throw new BrokerAuthorityError("cutover_required");
		const initialize = () => {
			if (matchesEstablished()) return;
			if (this.inspectBrokerAuthority().populated) throw new BrokerAuthorityError("cutover_required");
			this.#database.query("INSERT INTO broker_authority(singleton, authority_key) VALUES (1, ?)").run(key);
		};
		if (this.#inTransaction) initialize();
		else this.withTransaction(initialize);
	}

	#brokerAuthority(): BrokerAuthority | null {
		const stored = this.#database
			.query<{ authority_key: string }, []>("SELECT authority_key FROM broker_authority WHERE singleton = 1")
			.get();
		if (!stored) return null;
		const value: unknown = JSON.parse(stored.authority_key);
		if (!Array.isArray(value) || value.length !== 2) throw new BrokerAuthorityError("invalid_authority");
		const authority = { canonicalAgentDir: value[0], identity: value[1] };
		if (brokerAuthorityKey(authority) !== stored.authority_key) throw new BrokerAuthorityError("invalid_authority");
		return authority;
	}

	/** Resolves an untrusted GJC_SESSION_ID only when it has one unambiguous origin. */
	originForSessionId(sessionId: string): string | undefined {
		if (!/^[A-Za-z0-9-]{1,128}$/.test(sessionId)) return undefined;
		const origins = new Set<string>();
		const session = this.#database
			.query<{ origin_key: string }, [string]>("SELECT origin_key FROM sessions WHERE gjc_session_id = ?")
			.get(sessionId);
		if (session) origins.add(session.origin_key);
		let authority: BrokerAuthority | null;
		try {
			authority = this.#brokerAuthority();
		} catch {
			return undefined;
		}
		if (authority) {
			for (const row of this.#database
				.query<{ origin_key: string }, [string, string]>(
					"SELECT origin_key FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ? AND NOT EXISTS (SELECT 1 FROM broker_retired_sessions r WHERE r.session_id = broker_owned_bindings.session_id)",
				)
				.all(brokerAuthorityKey(authority), sessionId))
				origins.add(row.origin_key);
		}
		return origins.size === 1 ? origins.values().next().value : undefined;
	}

	/** Only a successful gateway session.create response may supply this provenance. */
	recordOwnedBinding(binding: OwnedBrokerBinding): boolean {
		return this.withTransaction(() => {
			this.#assertActiveAuthority(binding.authority);
			if (
				!binding.sessionId ||
				!binding.originKey ||
				!isAbsolute(binding.repo) ||
				normalize(binding.repo) !== binding.repo ||
				!Number.isSafeInteger(binding.epoch) ||
				binding.epoch < 0
			)
				throw new BrokerAuthorityError("unowned_session");
			if (this.#database.query("SELECT 1 FROM broker_retired_sessions WHERE session_id = ?").get(binding.sessionId))
				throw new BrokerAuthorityError("unowned_session");
			const key = brokerAuthorityKey(binding.authority);
			const previous = this.#database
				.query<{ origin_key: string; epoch: number; repo: string }, [string, string]>(
					"SELECT origin_key, epoch, repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?",
				)
				.get(key, binding.sessionId);
			if (
				previous &&
				(previous.origin_key !== binding.originKey ||
					previous.epoch !== binding.epoch ||
					!sameRepo(previous.repo, binding.repo))
			)
				throw new BrokerAuthorityError("unowned_session");
			const current = this.getSessionRecord(binding.originKey);
			if (current && current.epoch !== binding.epoch) return false;
			this.#database
				.query(
					"INSERT INTO broker_owned_bindings(authority_key, session_id, origin_key, epoch, repo, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(authority_key, session_id) DO NOTHING",
				)
				.run(key, binding.sessionId, binding.originKey, binding.epoch, binding.repo, new Date().toISOString());
			this.#database
				.query(
					"INSERT INTO sessions(origin_key, gjc_session_id, epoch, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id",
				)
				.run(binding.originKey, binding.sessionId, binding.epoch, new Date().toISOString());
			return true;
		});
	}

	/** Historical provenance permits retired-turn recovery, but only under the active authority. */
	assertOwnedSession(sessionId: string, repo: string, authority: BrokerAuthority): OwnedBrokerBinding {
		this.#assertActiveAuthority(authority);
		const row = this.#database
			.query<{ origin_key: string; epoch: number; repo: string }, [string, string]>(
				"SELECT origin_key, epoch, repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?",
			)
			.get(brokerAuthorityKey(authority), sessionId);
		if (!row || !sameRepo(row.repo, repo)) throw new BrokerAuthorityError("unowned_session");
		return { sessionId, originKey: row.origin_key, epoch: row.epoch, repo: row.repo, authority };
	}

	#assertActiveAuthority(authority: BrokerAuthority): void {
		const row = this.#database
			.query<{ authority_key: string }, []>("SELECT authority_key FROM broker_authority WHERE singleton = 1")
			.get();
		if (!row) throw new BrokerAuthorityError("cutover_required");
		if (row.authority_key !== brokerAuthorityKey(authority)) throw new BrokerAuthorityError("authority_mismatch");
	}

	isBrokerQuarantined(kind: BrokerQuarantineKind, id: string): boolean {
		return (
			this.#database.query("SELECT 1 FROM broker_quarantine WHERE kind = ? AND subject_id = ?").get(kind, id) !== null
		);
	}

	#assertNotQuarantined(kind: BrokerQuarantineKind, id: string): void {
		if (this.isBrokerQuarantined(kind, id)) throw new BrokerAuthorityError("quarantined");
	}

	#assertReplayableTurn(opRef: string): void {
		if (
			this.#database
				.query(
					"SELECT 1 FROM inbound_messages i JOIN broker_quarantine q ON q.kind = 'inbound' AND q.subject_id = i.message_id WHERE i.turn_op_ref = ? LIMIT 1",
				)
				.get(opRef)
		)
			throw new BrokerAuthorityError("quarantined");
	}

	/** Administrative transaction only: no broker controls, replay, or historical settlement. */
	cutoverBrokerAuthority(input: {
		expectedAuthority: BrokerAuthority | null;
		targetAuthority: BrokerAuthority;
		evidence: string;
		disposition?: "quarantine";
	}): string {
		return this.withTransaction(() => {
			const target = brokerAuthorityKey(input.targetAuthority);
			const expected = input.expectedAuthority === null ? null : brokerAuthorityKey(input.expectedAuthority);
			const before = this.inspectBrokerAuthority();
			if ((before.authority === null ? null : brokerAuthorityKey(before.authority)) !== expected || expected === target)
				throw new BrokerAuthorityError("authority_mismatch");
			if (
				this.#database
					.query("SELECT 1 FROM broker_cutovers WHERE old_authority = ? OR target_authority = ? LIMIT 1")
					.get(target, target)
			)
				throw new BrokerAuthorityError("authority_mismatch");
			if (typeof input.evidence !== "string" || !input.evidence.trim()) throw new Error("cutover evidence is required");
			if (input.disposition !== undefined && input.disposition !== "quarantine")
				throw new Error("invalid cutover disposition");
			if ((before.openInbound || before.openWork || before.openMonitors) && input.disposition !== "quarantine")
				throw new BrokerAuthorityError("old_work_open");
			const snapshot = Object.fromEntries(
				BROKER_SNAPSHOT_TABLES.map((table) => [table, this.#database.query(`SELECT * FROM ${table}`).all()]),
			);
			const id = crypto.randomUUID();
			this.#database
				.query(
					"INSERT INTO broker_cutovers(id, old_authority, target_authority, evidence, disposition, snapshot_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
				)
				.run(
					id,
					expected,
					target,
					input.evidence,
					input.disposition ?? "quiescent",
					JSON.stringify(snapshot),
					new Date().toISOString(),
				);
			// All old identities become permanent non-replayable holds, even terminal history.
			for (const [kind, table, column] of [
				["inbound", "inbound_messages", "message_id"],
				["work", "lane_jobs", "job_id"],
				["work", "work_tasks", "job_id"],
				["monitor", "monitor_events", "event_id"],
			] as const) {
				this.#database
					.query(
						`INSERT INTO broker_quarantine(kind, subject_id, cutover_id) SELECT ?, ${column}, ? FROM ${table} WHERE true ON CONFLICT(kind, subject_id) DO NOTHING`,
					)
					.run(kind, id);
			}
			this.#database
				.query(`INSERT INTO broker_retired_sessions(session_id, cutover_id)
				SELECT session_id, ? FROM (
					SELECT gjc_session_id AS session_id FROM sessions WHERE gjc_session_id <> ''
					UNION SELECT bound_session_id FROM inbound_messages WHERE bound_session_id IS NOT NULL
					UNION SELECT session_id FROM work_attempt_runtime
					UNION SELECT session_id FROM session_tail_cursors
					UNION SELECT session_id FROM broker_owned_bindings
				) WHERE true ON CONFLICT(session_id) DO NOTHING`)
				.run(id);
			for (const row of this.#database.query<{ record_json: string }, []>("SELECT record_json FROM lane_jobs").all()) {
				for (const sessionId of parseLaneJobRecord(row.record_json).sessions)
					this.#database
						.query(
							"INSERT INTO broker_retired_sessions(session_id, cutover_id) VALUES (?, ?) ON CONFLICT(session_id) DO NOTHING",
						)
						.run(sessionId, id);
			}
			this.#database.exec("UPDATE sessions SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0");
			this.#database
				.query(
					"INSERT INTO broker_authority(singleton, authority_key) VALUES (1, ?) ON CONFLICT(singleton) DO UPDATE SET authority_key = excluded.authority_key",
				)
				.run(target);
			return id;
		});
	}

	brokerCutoverSnapshot(id: string): string | undefined {
		return this.#database
			.query<{ snapshot_json: string }, [string]>("SELECT snapshot_json FROM broker_cutovers WHERE id = ?")
			.get(id)?.snapshot_json;
	}
	/** Explicit seam for operations that must participate in a caller-owned commit. */
	requireTransaction(): void {
		if (!this.#inTransaction) throw new Error("operation requires a database transaction");
	}

	workTaskGet(taskId: string): WorkTask | undefined {
		const task = this.#workTaskAdmission(taskId);
		if (task?.dispatchPhase === "prepared")
			this.#workTaskRuntime(task, { opRef: task.opRef, sessionId: task.sessionId!, epoch: task.epoch! });
		return task;
	}

	/** Private admission bookkeeping, never an execution/control fallback. */
	#workTaskAdmission(taskId: string): WorkTask | undefined {
		const row = this.#database
			.query<
				{
					task_id: string;
					lane_name: string;
					job_id: string;
					op_ref: string;
					thread_origin_key: string | null;
					version: number;
					record_json: string;
					dispatch_phase: string;
					surface_phase: string;
					obligation_state: string;
				},
				[string]
			>("SELECT * FROM work_tasks WHERE task_id = ?")
			.get(taskId);
		if (!row) return undefined;
		const task = JSON.parse(row.record_json) as WorkTask;
		validateTask(task);
		taskAssert(
			task.taskId === row.task_id &&
				task.laneName === row.lane_name &&
				task.jobId === row.job_id &&
				task.opRef === row.op_ref &&
				task.version === row.version &&
				(task.thread === null ? null : originKey(task.thread)) === row.thread_origin_key &&
				task.dispatchPhase === row.dispatch_phase &&
				task.surfacePhase === row.surface_phase &&
				task.obligationState === row.obligation_state,
			"identity",
		);
		return task;
	}

	#workTaskNegativeAnchor(taskId: string): { task: WorkTask; mapped: boolean } {
		const task = this.#workTaskAdmission(taskId);
		taskAssert(task && task.dispatchPhase === "prepared", "unavailable");
		const assignment = this.workTaskSourceGet(`assignment-${task.taskId}`);
		taskAssert(
			assignment &&
				assignment.taskId === task.taskId &&
				assignment.kind === "assignment" &&
				assignment.body === task.request.text &&
				assignment.completeness === "complete" &&
				taskJson(assignment.evidence) === taskJson(task.request.evidence) &&
				assignment.supersedes === null &&
				assignment.controlId === null &&
				assignment.reportId === null &&
				assignment.administrative === undefined &&
				assignment.deliveryId === null &&
				task.createdAt === assignment.evidence.observedAt,
			"identity",
		);
		// A locator/tombstone alone is not authenticated creation or delivery evidence.
		// Mapping failures permit only an admission-attributed source with null intent.
		let mapped = false;
		try {
			const marker = this.workTaskSourceGet(`activation-${task.taskId}`);
			taskAssert(
				task.thread &&
					task.surfacePhase === "bound" &&
					marker &&
					marker.taskId === task.taskId &&
					marker.kind === "observation" &&
					marker.completeness === "complete" &&
					marker.supersedes === null &&
					marker.controlId === null &&
					marker.reportId === null &&
					marker.administrative === undefined &&
					marker.evidence.principalId === "gateway" &&
					marker.evidence.editId === null &&
					taskJson(marker.evidence.origin) === taskJson(task.thread) &&
					marker.body ===
						`Assignment ${task.taskId}\n${task.request.text}\nScope: ${task.request.kind}; repository admission is not an OS sandbox.` &&
					marker.deliveryId === workTaskSourceDeliveryId(task.taskId, marker.sourceId, task.thread),
				"identity",
			);
			if ("parent" in task.request.surface) {
				taskAssert(
					task.surfaceClaimId !== null &&
						marker.evidence.eventId === task.surfaceClaimId &&
						task.thread.parentId === task.request.surface.parent.conversationId &&
						task.thread.boundaryId === task.request.surface.parent.boundaryId,
					"identity",
				);
			}
			const delivery = this.deliveryGet(marker.deliveryId!);
			taskAssert(
				delivery && delivery.turn_id === task.opRef && delivery.origin_key === originKey(task.thread),
				"identity",
			);
			const payload = JSON.parse(delivery.payload_json) as ChatMessagePayload;
			validateWorkTaskMessageMetadata(payload.workTask, {
				origin: task.thread,
				taskId: task.taskId,
				opRef: task.opRef,
				sourceId: marker.sourceId,
			});
			taskAssert(
				payload.deliveryId === marker.deliveryId &&
					payload.turnId === task.opRef &&
					taskJson(payload.origin) === taskJson(task.thread) &&
					payload.text === marker.body &&
					payload.role === "assistant" &&
					payload.final === false &&
					!payload.reaction &&
					!payload.voiceText,
				"identity",
			);
			mapped = true;
		} catch {
			/* No guessed destination, replacement mapping, or retroactive intent. */
		}
		return { task, mapped };
	}

	#workTaskUnavailableBody(task: WorkTask, sourceId: string, mapped: boolean): string {
		return (
			`Task ${task.taskId}; stored admission operation ${task.opRef}; request hash ${task.requestHash}.\n` +
			"Original linked runtime/job evidence unavailable. Code: linked_record_unavailable.\n" +
			`Opaque corruption fact ${sourceId}.\n` +
			"Identity provenance: validated retained task columns, request hash and original assignment source; stored admission is not GJC truth.\n" +
			"Execution, session ownership, terminal outcome and safety are unknown. Evidence incomplete; no replay, control, release or cleanup authority.\n" +
			(mapped
				? "Destination verified against original activation source and retained delivery."
				: "No verified available original destination: audit only, null delivery intent; no retroactive delivery promise.")
		);
	}

	/**
	 * Negative-only write. No caller body, SDK identity, target or exception text is
	 * accepted. Quarantine remains intact; normal task reads and writes stay strict.
	 */
	workTaskRecordLinkedUnavailableInTransaction(
		taskId: string,
		at: string,
		surfaceAvailable: (origin: OriginRef) => boolean,
	): WorkTaskSource | undefined {
		this.requireTransaction();
		taskAssert(workTime(at));
		const { task, mapped } = this.#workTaskNegativeAnchor(taskId);
		try {
			this.#workTaskRuntime(task, { opRef: task.opRef, sessionId: task.sessionId!, epoch: task.epoch! });
			return undefined;
		} catch {
			/* Only the failure to validate is evidence; never parse for salvage. */
		}
		const runtime = this.#database.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(task.opRef);
		const job = this.#database.query("SELECT * FROM lane_jobs WHERE job_id = ?").get(task.jobId);
		const fingerprint = taskHash("work-task-linked-unavailable", {
			taskId: task.taskId,
			opRef: task.opRef,
			requestHash: task.requestHash,
			runtime,
			job,
		});
		const sourceId = `unavailability-${fingerprint}`;
		const existing = this.workTaskSourceGet(sourceId);
		if (existing) {
			this.#validateWorkTaskUnavailable(task, mapped, existing);
			return existing;
		}
		let deliver = false;
		if (mapped) {
			try {
				deliver = surfaceAvailable(JSON.parse(taskJson(task.thread!)) as OriginRef) === true;
			} catch {
				/* Presentation lookup failure cannot invent a destination or erase the negative fact. */
			}
		}
		const retained = this.#workTaskNegativeAnchor(taskId);
		taskAssert(
			taskJson(retained.task) === taskJson(task) &&
				retained.mapped === mapped &&
				taskJson(this.#database.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(task.opRef)) ===
					taskJson(runtime) &&
				taskJson(this.#database.query("SELECT * FROM lane_jobs WHERE job_id = ?").get(task.jobId)) === taskJson(job),
			"conflict",
		);
		const input: WorkTaskSourceInput = {
			sourceId,
			taskId,
			kind: "observation",
			body: this.#workTaskUnavailableBody(task, sourceId, deliver),
			evidence: {
				principalId: "gateway",
				origin: deliver ? task.thread! : task.request.evidence.origin,
				eventId: sourceId,
				editId: null,
				evidenceAt: at,
				observedAt: at,
			},
			supersedes: null,
			completeness: "incomplete",
			controlId: null,
			reportId: null,
		};
		let payload: ChatMessagePayload | undefined;
		if (deliver) {
			const deliveryId = workTaskSourceDeliveryId(taskId, sourceId, task.thread!);
			const body = buildDeliveryPayload(task.opRef, task.thread!, input.body, deliveryId, undefined, false);
			taskAssert(body, "unavailable");
			payload = { ...body, workTask: { taskId, opRef: task.opRef, sourceId, mappedOnly: true } };
		}
		return this.#workTaskSourceAppendInTransaction(input, payload, task);
	}

	#validateWorkTaskUnavailable(task: WorkTask, mapped: boolean, source: WorkTaskSource): void {
		const delivered = source.deliveryId !== null;
		taskAssert(
			/^unavailability-[0-9a-f]{64}$/.test(source.sourceId) &&
				source.taskId === task.taskId &&
				source.kind === "observation" &&
				source.body === this.#workTaskUnavailableBody(task, source.sourceId, delivered) &&
				source.completeness === "incomplete" &&
				source.supersedes === null &&
				source.controlId === null &&
				source.reportId === null &&
				source.administrative === undefined &&
				source.evidence.principalId === "gateway" &&
				source.evidence.eventId === source.sourceId &&
				source.evidence.editId === null &&
				source.evidence.evidenceAt === source.evidence.observedAt &&
				taskJson(source.evidence.origin) === taskJson(delivered ? task.thread : task.request.evidence.origin) &&
				(!delivered ||
					(mapped && source.deliveryId === workTaskSourceDeliveryId(task.taskId, source.sourceId, task.thread!))),
			"identity",
		);
		if (delivered) {
			const delivery = this.deliveryGet(source.deliveryId!);
			taskAssert(
				delivery && delivery.turn_id === task.opRef && delivery.origin_key === originKey(task.thread!),
				"identity",
			);
			const payload = JSON.parse(delivery.payload_json) as ChatMessagePayload;
			validateWorkTaskMessageMetadata(payload.workTask, {
				origin: task.thread!,
				taskId: task.taskId,
				opRef: task.opRef,
				sourceId: source.sourceId,
			});
			taskAssert(
				payload.deliveryId === source.deliveryId &&
					payload.turnId === task.opRef &&
					taskJson(payload.origin) === taskJson(task.thread) &&
					payload.text === source.body &&
					payload.role === "assistant" &&
					payload.final === false &&
					!payload.reaction &&
					!payload.voiceText,
				"identity",
			);
		}
	}

	/** Read-only scope proof for negative administration; normal SDK readers remain strict. */
	workTaskQualifiedAdmissionScopeInTransaction(taskId: string): WorkTaskQualifiedAdmissionScope {
		this.requireTransaction();
		const { task, mapped } = this.#workTaskNegativeAnchor(taskId);
		taskAssert(mapped, "unavailable");
		return {
			qualification: "retained_admission",
			taskId,
			requestHash: task.requestHash,
			request: {
				kind: task.request.kind,
				cwd: task.request.cwd,
				coordinator: structuredClone(task.request.coordinator),
			},
		};
	}

	/** Negative presentation only; retained mapping is not runtime or execution proof. */
	workTaskQualifiedSurfaceInTransaction(taskId: string, origin: OriginRef): boolean {
		this.requireTransaction();
		const { task, mapped } = this.#workTaskNegativeAnchor(taskId);
		return mapped && taskJson(task.thread) === taskJson(origin);
	}

	/** Standalone administrative snapshot; never an SDK status or execution capability. */
	workTaskDispositionBasis(taskId: string): WorkTaskDispositionBasisResult {
		const select = (): WorkTaskDispositionBasisResult => {
			try {
				const task = this.#workTaskAdmission(taskId);
				if (!task) return { execution: "none", kind: "unavailable", basis: null };
				let unavailable = false;
				try {
					this.workTaskGet(taskId);
				} catch {
					unavailable = true;
				}
				let qualification:
					| Extract<WorkTaskDispositionBasisResult, { kind: "validation_unavailable" }>["qualification"]
					| undefined;
				if (unavailable) {
					const anchor = this.#workTaskNegativeAnchor(taskId);
					// A public administrative target requires an authentic original destination.
					if (!anchor.mapped) return { execution: "none", kind: "unavailable", basis: null };
					const fingerprint = this.#workTaskCorruptionFingerprint(task);
					const source = this.workTaskSourceGet(`unavailability-${fingerprint}`);
					if (!source) return { execution: "none", kind: "unavailable", basis: null };
					this.#validateWorkTaskUnavailable(task, anchor.mapped, source);
					qualification = {
						kind: "validation_unavailable",
						scope: task.request.kind,
						sourceId: source.sourceId,
						corruptionFingerprint: fingerprint,
						firstObservedAt: source.evidence.observedAt,
					};
				}
				const rows = this.#database
					.query<{ control_id: string }, [string]>(
						"SELECT control_id FROM work_controls WHERE task_id = ? AND phase = 'held' ORDER BY sequence LIMIT 21",
					)
					.all(taskId);
				const controls: WorkTaskDispositionBasis["controls"][number][] = [];
				let partial = rows.length > 20;
				for (const row of rows.slice(0, 20)) {
					try {
						const control = this.workControlGet(row.control_id)!;
						if (control.phase !== "held" || control.receipt !== null) continue;
						this.#workTaskDispositionControl(task, control, unavailable);
						controls.push({
							kind: "control",
							controlId: control.controlId,
							eventId: control.request.evidence.eventId,
							clientRef: control.clientRef,
						});
					} catch {
						partial = true;
					}
				}
				const basis: WorkTaskDispositionBasis = {
					taskId,
					jobId: task.jobId,
					expectedOpRef: task.opRef,
					sessionId: task.sessionId,
					epoch: task.epoch,
					cwd: task.request.cwd,
					requestHash: task.requestHash,
					expectedTaskVersion: task.version,
					controls,
					controlsCompleteness: partial ? "partial" : "complete",
					report:
						task.obligationState === "held" || (qualification && task.obligationState === "awaiting_final")
							? { kind: "report", reportId: task.terminalReportId }
							: null,
				};
				return validateWorkTaskDispositionBasisResult(
					qualification
						? { execution: "none", kind: "validation_unavailable", basis, qualification }
						: { execution: "none", kind: "original", basis },
				);
			} catch {
				return { execution: "none", kind: "unavailable", basis: null };
			}
		};
		return this.#inTransaction ? select() : this.withTransaction(select);
	}

	#workTaskCorruptionFingerprint(task: WorkTask): string {
		return taskHash("work-task-linked-unavailable", {
			taskId: task.taskId,
			opRef: task.opRef,
			requestHash: task.requestHash,
			runtime: this.#database.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(task.opRef),
			job: this.#database.query("SELECT * FROM lane_jobs WHERE job_id = ?").get(task.jobId),
		});
	}

	#workTaskDispositionControl(task: WorkTask, control: WorkControl, runtimeUnavailable: boolean): void {
		taskAssert(
			control.request.taskId === task.taskId &&
				control.request.expectedOpRef === task.opRef &&
				control.sessionId === task.sessionId &&
				control.epoch === task.epoch,
			"identity",
		);
		if (!runtimeUnavailable) return;
		// Direct controls without a retained admission hash cannot independently
		// authenticate a client reference when the runtime is unreadable.
		const route = this.workTaskSourceGet(`route-${control.controlId}`);
		taskAssert(
			route &&
				route.taskId === task.taskId &&
				route.kind === "decision" &&
				route.controlId === control.controlId &&
				route.completeness === "complete" &&
				route.reportId === null &&
				route.supersedes === null &&
				taskJson(route.evidence) === taskJson(control.request.evidence),
			"identity",
		);
		const proof = JSON.parse(route.body) as { requestHash: string; routeAdmission: WorkControlRouteAdmission };
		taskAssert(
			proof.requestHash === control.requestHash &&
				proof.routeAdmission.taskId === task.taskId &&
				proof.routeAdmission.opRef === task.opRef &&
				taskJson(proof.routeAdmission.thread) === taskJson(task.thread),
			"identity",
		);
		const source = this.workTaskSourceGet(proof.routeAdmission.sourceId);
		taskAssert(
			source &&
				source.taskId === task.taskId &&
				source.kind === "instruction" &&
				source.completeness === "complete" &&
				source.body === control.request.body &&
				taskJson(source.evidence) === taskJson(control.request.evidence),
			"identity",
		);
	}

	/** Retained negative facts only; no reinterpretation of current SDK JSON. */
	workTaskLinkedUnavailableSourcesInTransaction(taskId: string): {
		sources: readonly WorkTaskSource[];
		manifest: string;
		omitted: number | null;
	} {
		this.requireTransaction();
		const { task, mapped } = this.#workTaskNegativeAnchor(taskId);
		const rows = this.#database
			.query<{ source_id: string }, [string]>(
				"SELECT source_id FROM work_task_sources WHERE task_id = ? AND source_id GLOB 'unavailability-*' AND length(source_id) = 79 AND substr(source_id, 16) NOT GLOB '*[^0-9a-f]*' ORDER BY sequence DESC LIMIT 51",
			)
			.all(taskId);
		const sources: WorkTaskSource[] = [];
		let bytes = 0;
		for (const row of rows.slice(0, 50)) {
			const source = this.workTaskSourceGet(row.source_id)!;
			this.#validateWorkTaskUnavailable(task, mapped, source);
			bytes += Buffer.byteLength(JSON.stringify(source), "utf8");
			if (bytes > 12 * 1024) break;
			sources.push(source);
		}
		const omitted = rows.length === 51 ? null : rows.length - sources.length;
		return {
			sources,
			manifest: taskHash("work-task-negative-manifest", [
				taskId,
				omitted,
				...sources.map((source) => source.contentHash),
			]),
			omitted,
		};
	}

	workTaskByThread(threadOriginKey: string): WorkTask | undefined {
		const row = this.#database
			.query<{ task_id: string }, [string]>("SELECT task_id FROM work_tasks WHERE thread_origin_key = ?")
			.get(threadOriginKey);
		return row ? this.workTaskGet(row.task_id) : undefined;
	}

	/** Routing hint only: includes corrupt records and permanent settled mappings. */
	workTaskDiscordLocator(conversationId: string): { taskId: string } | undefined {
		// Ingress validates the generic OriginRef segment. Membership discovery
		// must also find corrupt nonnumeric mappings, not impose task admission.
		// Canonical Discord thread keys are discord/thread/<id>/parent=<id>.
		// A binary range uses the existing UNIQUE thread_origin_key index, without
		// reading record_json or trusting a caller-supplied parent/boundary.
		const prefix = `discord/thread/${conversationId}`;
		const rows = this.#database
			.query<{ task_id: string }, [string, string]>(
				"SELECT task_id FROM work_tasks WHERE thread_origin_key >= ? AND thread_origin_key < ? LIMIT 2",
			)
			.all(`${prefix}/`, `${prefix}0`);
		taskAssert(rows.length <= 1, "identity");
		return rows[0] ? { taskId: rows[0].task_id } : undefined;
	}

	workTaskByLane(laneName: string): WorkTask | undefined {
		const row = this.#database
			.query<{ task_id: string }, [string]>("SELECT task_id FROM work_tasks WHERE lane_name = ?")
			.get(laneName);
		return row ? this.workTaskGet(row.task_id) : undefined;
	}

	/** Bounded non-consuming keyset read. Quarantined tasks remain visible as debt. */
	workTaskList(limit = 20, afterTaskId = ""): readonly WorkTask[] {
		taskAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 20);
		return this.#database
			.query<{ task_id: string }, [string, number]>(
				"SELECT task_id FROM work_tasks WHERE task_id > ? ORDER BY task_id LIMIT ?",
			)
			.all(afterTaskId, limit)
			.map((row) => this.workTaskGet(row.task_id)!);
	}

	/**
	 * Index locators only, NOT validated task facts. Recovery inspects each key under
	 * its own error boundary; one corrupt record must not hide the rest of the page.
	 * No JSON parsing, mutation, quarantine filtering or consuming inbox operation.
	 */
	workTaskKeys(
		limit = 20,
		afterTaskId = "",
	): {
		readonly keys: readonly { readonly taskId: string; readonly jobId: string }[];
		readonly nextTaskId: string | null;
	} {
		taskAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 20);
		taskAssert(afterTaskId === "" || workString(afterTaskId, 128));
		const rows = this.#database
			.query<{ taskId: string; jobId: string }, [string, number]>(
				"SELECT task_id AS taskId, job_id AS jobId FROM work_tasks WHERE task_id > ? ORDER BY task_id LIMIT ?",
			)
			.all(afterTaskId, limit + 1);
		const keys = rows.slice(0, limit);
		return { keys, nextTaskId: rows.length > limit ? keys[keys.length - 1]!.taskId : null };
	}

	/** Candidate locators, not an owner-action classification or validated record read. */
	workTaskSnapshotKeys(limit = 20): {
		readonly keys: readonly { readonly taskId: string; readonly jobId: string }[];
		readonly total: number;
		readonly omitted: number;
		readonly manifest: string;
	} {
		taskAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 20);
		const select = () => {
			const total = this.#database
				.query<{ total: number }, []>("SELECT COUNT(*) AS total FROM work_tasks")
				.get()!.total;
			const rows = this.#database
				.query<
					{
						taskId: string;
						jobId: string;
						version: number;
						runtimeVersion: number | null;
						controlRevision: number;
						sourceRevision: number;
						priority: number;
					},
					[number]
				>(`
				SELECT t.task_id AS taskId, t.job_id AS jobId, t.version,
					(SELECT version FROM work_attempt_runtime r WHERE r.op_ref = t.op_ref) AS runtimeVersion,
					(SELECT COALESCE(SUM(version + 1), 0) FROM work_controls c WHERE c.task_id = t.task_id) AS controlRevision,
					(SELECT COALESCE(MAX(sequence), 0) FROM work_task_sources s WHERE s.task_id = t.task_id) AS sourceRevision,
					CASE WHEN t.surface_phase = 'held' OR t.obligation_state = 'held'
						OR EXISTS (SELECT 1 FROM work_controls c WHERE c.task_id = t.task_id AND c.phase IN ('pending','sending','held'))
						THEN 0 ELSE 1 END AS priority
				FROM work_tasks t ORDER BY priority, t.task_id LIMIT ?
			`)
				.all(limit);
			return {
				keys: rows.map(({ taskId, jobId }) => ({ taskId, jobId })),
				total,
				omitted: total - rows.length,
				manifest: taskHash("work-task-selection-v1", [limit, total, rows]),
			};
		};
		return this.#inTransaction ? select() : this.withTransaction(select);
	}

	workTaskCreate(input: { taskId: string; opRef: string; request: WorkTaskRequest }): WorkTaskAdmission<WorkTask> {
		return this.withTransaction(() => this.workTaskCreateInTransaction(input));
	}

	/** Admission can join immutable source and delivery writes in the caller's transaction. */
	workTaskCreateInTransaction(input: {
		taskId: string;
		opRef: string;
		request: WorkTaskRequest;
	}): WorkTaskAdmission<WorkTask> {
		this.requireTransaction();
		validateTaskRequest(input.request);
		const hash = taskRequestHash(input.request);
		const existing = this.workTaskGet(input.taskId);
		if (existing) {
			this.#assertNotQuarantined("work", existing.jobId);
			return { disposition: existing.requestHash === hash ? "duplicate" : "conflict", record: existing };
		}
		const now = input.request.evidence.observedAt;
		const task: WorkTask = {
			taskId: input.taskId,
			laneName: `fm-${input.taskId}`,
			jobId: laneJobDatabaseId(`fm-${input.taskId}`),
			opRef: input.opRef,
			request: JSON.parse(taskJson(input.request)),
			requestHash: hash,
			thread: null,
			surfacePhase: "pending",
			surfaceClaimId: null,
			surfaceHoldReason: null,
			dispatchPhase: "pending",
			sessionId: null,
			epoch: null,
			obligationState: "awaiting_final",
			terminalReportId: null,
			reportDisposition: null,
			reportAdmittedAt: null,
			holdReason: null,
			createdAt: now,
			updatedAt: now,
			version: 0,
		};
		validateTask(task);
		this.#assertNotQuarantined("work", task.jobId);
		taskAssert(
			this.laneJobJson(task.jobId) === undefined &&
				this.laneJobJsonByLaneKey(`work-${task.laneName}`) === undefined &&
				!this.#database
					.query("SELECT 1 FROM work_attempt_runtime WHERE op_ref = ? OR job_id = ?")
					.get(task.opRef, task.jobId),
			"conflict",
		);
		taskAssert(!this.#database.query("SELECT 1 FROM work_tasks WHERE op_ref = ?").get(task.opRef), "conflict");
		this.#database
			.query(
				"INSERT INTO work_tasks(task_id, lane_name, job_id, op_ref, thread_origin_key, surface_phase, dispatch_phase, obligation_state, version, record_json) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)",
			)
			.run(
				task.taskId,
				task.laneName,
				task.jobId,
				task.opRef,
				task.surfacePhase,
				task.dispatchPhase,
				task.obligationState,
				task.version,
				JSON.stringify(task),
			);
		this.workTaskSourceAppendInTransaction({
			sourceId: `assignment-${task.taskId}`,
			taskId: task.taskId,
			kind: "assignment",
			body: task.request.text,
			evidence: task.request.evidence,
			supersedes: null,
			completeness: "complete",
			controlId: null,
			reportId: null,
		});
		return { disposition: "created", record: task };
	}

	#workTaskCas(current: WorkTask, next: WorkTask): WorkTask {
		validateTask(next);
		taskAssert(
			next.version === current.version + 1 &&
				next.taskId === current.taskId &&
				next.opRef === current.opRef &&
				next.requestHash === current.requestHash &&
				next.createdAt === current.createdAt,
			"identity",
		);
		const changed = this.#database
			.query(
				"UPDATE work_tasks SET thread_origin_key = ?, surface_phase = ?, dispatch_phase = ?, obligation_state = ?, version = ?, record_json = ? WHERE task_id = ? AND version = ?",
			)
			.run(
				next.thread === null ? null : originKey(next.thread),
				next.surfacePhase,
				next.dispatchPhase,
				next.obligationState,
				next.version,
				JSON.stringify(next),
				current.taskId,
				current.version,
			).changes;
		taskAssert(changed === 1, "conflict");
		return next;
	}

	/** A claim is never replaced; a bound thread is a permanent one-to-one tombstone. */
	workTaskSurfaceInTransaction(
		taskId: string,
		expectedVersion: number,
		change:
			| { phase: "claimed"; claimId: string; at: string }
			| { phase: "bound"; thread: OriginRef; claimId: string | null; at: string }
			| { phase: "held"; reason: string; at: string },
	): WorkTask | undefined {
		this.requireTransaction();
		const task = this.workTaskGet(taskId);
		if (!task || task.version !== expectedVersion) return undefined;
		this.#assertNotQuarantined("work", task.jobId);
		taskAssert(workTime(change.at));
		taskAssert(
			task.dispatchPhase === "pending" && (task.surfacePhase === "pending" || task.surfacePhase === "claimed"),
			"transition",
		);
		if (change.phase === "held") {
			taskAssert(taskText(change.reason, 2048));
			return this.#workTaskCas(task, {
				...task,
				surfacePhase: "held",
				surfaceHoldReason: change.reason,
				updatedAt: change.at,
				version: task.version + 1,
			});
		}
		if (change.phase === "claimed") {
			taskAssert(task.surfacePhase === "pending" && !("thread" in task.request.surface), "transition");
			taskAssert(workString(change.claimId));
			return this.#workTaskCas(task, {
				...task,
				surfacePhase: "claimed",
				surfaceClaimId: change.claimId,
				updatedAt: change.at,
				version: task.version + 1,
			});
		}
		taskAssert(change.phase === "bound" && task.surfaceClaimId === change.claimId, "identity");
		taskAssert("thread" in task.request.surface || task.surfacePhase === "claimed", "transition");
		validateOriginRef(change.thread);
		taskAssert(change.thread.platform === "discord" && change.thread.kind === "thread");
		if ("parent" in task.request.surface)
			taskAssert(
				change.thread.parentId === task.request.surface.parent.conversationId &&
					change.thread.boundaryId === task.request.surface.parent.boundaryId,
				"identity",
			);
		const key = originKey(change.thread);
		taskAssert(!this.workTaskDiscordLocator(change.thread.conversationId), "conflict");
		// Do not capture a persona's in-flight turn or queued owner input.
		taskAssert(
			!this.#database.query("SELECT 1 FROM inbound_messages WHERE origin_key = ? AND state <> 'done' LIMIT 1").get(key),
			"unavailable",
		);
		return this.#workTaskCas(task, {
			...task,
			surfacePhase: "bound",
			thread: change.thread,
			updatedAt: change.at,
			version: task.version + 1,
		});
	}

	#workTaskRuntime(task: WorkTask, identity: WorkTaskAttemptIdentity): WorkAttemptRuntime {
		taskAssert(
			task.dispatchPhase === "prepared" &&
				task.opRef === identity.opRef &&
				task.sessionId === identity.sessionId &&
				task.epoch === identity.epoch,
			"identity",
		);
		const runtime = this.workAttemptGet(task.opRef);
		taskAssert(
			runtime &&
				runtime.jobId === task.jobId &&
				runtime.sessionId === task.sessionId &&
				runtime.epoch === task.epoch &&
				runtime.sessionKey === `work/task/${task.laneName}` &&
				runtime.cwd === task.request.cwd &&
				runtime.parent?.kind === "persona" &&
				runtime.parent.originKey === originKey(task.request.coordinator) &&
				taskJson(runtime.parent.origin) === taskJson(task.request.coordinator),
			"identity",
		);
		return runtime;
	}

	/** No execution state is copied: this only references the existing report obligation. */
	workTaskObligationInTransaction(
		taskId: string,
		expectedVersion: number,
		input: {
			identity: WorkTaskAttemptIdentity;
			state: "held" | "final_admitted";
			reason: string | null;
			at: string;
		},
	): WorkTask | undefined {
		this.requireTransaction();
		const task = this.workTaskGet(taskId);
		if (!task || task.version !== expectedVersion) return undefined;
		this.#assertNotQuarantined("work", task.jobId);
		taskAssert(workTime(input.at) && task.obligationState !== "final_admitted", "transition");
		const runtime = this.#workTaskRuntime(task, input.identity);
		if (input.state === "final_admitted") {
			taskAssert(
				input.reason === null &&
					runtime.terminal?.kind === "broker" &&
					runtime.output.proof !== null &&
					runtime.output.disposition === "available" &&
					runtime.settledAt !== null,
				"transition",
			);
		} else taskAssert(input.state === "held" && taskText(input.reason, 2048));
		return this.#workTaskCas(task, {
			...task,
			obligationState: input.state,
			holdReason: input.reason,
			terminalReportId: runtime.reportId,
			reportDisposition: runtime.decision,
			reportAdmittedAt: input.state === "final_admitted" ? input.at : task.reportAdmittedAt,
			version: task.version + 1,
			updatedAt: input.at,
		});
	}

	/** Adds original late evidence without rewriting the settled attempt or its report. */
	workTaskSupplementInTransaction(
		taskId: string,
		expectedVersion: number,
		input: {
			identity: WorkTaskAttemptIdentity;
			text: string;
			at: string;
			status: PromptStatusBody;
			proof: {
				source: "turn.result";
				fullness: "original";
				sessionId: string;
				repo: string;
				opRef: string;
				clientRef: string;
				commandId?: string;
				turnId?: string;
				terminalAt: number;
				contentVersion: 1;
				byteLength: number;
			};
		},
		delivery: ChatMessagePayload,
	): WorkTask | undefined {
		this.requireTransaction();
		const task = this.workTaskGet(taskId);
		if (!task || task.version !== expectedVersion) return undefined;
		this.#assertNotQuarantined("work", task.jobId);
		const runtime = this.#workTaskRuntime(task, input.identity);
		const terminal = runtime.terminal;
		const status = input.status;
		const proof = input.proof;
		// Broker settlements keep matching their immutable recorded status; a local
		// settlement admits only the exact status re-observed for this attempt.
		if (terminal?.kind === "local")
			taskAssert(
				runtime.settledAt !== null && localSettledTerminalStatus(runtime, status, Date.parse(input.at)),
				"identity",
			);
		else
			taskAssert(
				terminal?.kind === "broker" &&
					runtime.settledAt !== null &&
					terminal.status !== undefined &&
					taskJson(status) === taskJson(terminal.status),
				"identity",
			);
		taskAssert(
			proof.source === "turn.result" &&
				proof.fullness === "original" &&
				proof.contentVersion === 1 &&
				proof.sessionId === runtime.sessionId &&
				proof.repo === runtime.cwd &&
				proof.opRef === runtime.opRef &&
				proof.clientRef === runtime.opRef &&
				Number.isFinite(proof.terminalAt) &&
				proof.terminalAt === status.terminalAt &&
				proof.commandId === status.commandId &&
				proof.turnId === status.turnId &&
				workTime(input.at) &&
				taskText(input.text, 16 * 1024) &&
				!isSilentOutput(input.text) &&
				proof.byteLength === Buffer.byteLength(input.text, "utf8") &&
				delivery.text === input.text &&
				delivery.final === true,
			"identity",
		);
		taskAssert(task.obligationState !== "final_admitted", "transition");
		this.workTaskSourceAppendInTransaction({
			sourceId: `supplement-proof-${taskId}-original`,
			taskId,
			kind: "decision",
			body: taskJson({ identity: input.identity, proof, status }),
			evidence: {
				principalId: "gateway",
				origin: task.thread!,
				eventId: task.opRef,
				editId: null,
				evidenceAt: new Date(proof.terminalAt).toISOString(),
				observedAt: input.at,
			},
			supersedes: null,
			completeness: "complete",
			controlId: null,
			reportId: runtime.reportId,
		});
		this.workTaskSourceAppendInTransaction(
			{
				sourceId: `supplement-${taskId}-original`,
				taskId,
				kind: "observation",
				body: input.text,
				evidence: {
					principalId: "gateway",
					origin: task.thread!,
					eventId: task.opRef,
					editId: null,
					evidenceAt: new Date(proof.terminalAt).toISOString(),
					observedAt: input.at,
				},
				supersedes: null,
				completeness: "complete",
				controlId: null,
				reportId: runtime.reportId,
			},
			delivery,
		);
		return this.#workTaskCas(task, {
			...task,
			obligationState: "final_admitted",
			holdReason: null,
			terminalReportId: runtime.reportId,
			reportDisposition: runtime.decision,
			reportAdmittedAt: input.at,
			version: task.version + 1,
			updatedAt: input.at,
		});
	}

	workControlGet(controlId: string): WorkControl | undefined {
		const row = this.#database
			.query<
				{
					control_id: string;
					task_id: string;
					event_id: string;
					sequence: number;
					phase: string;
					client_ref: string | null;
					notification_id: string;
					version: number;
					record_json: string;
				},
				[string]
			>("SELECT * FROM work_controls WHERE control_id = ?")
			.get(controlId);
		if (!row) return undefined;
		const control = JSON.parse(row.record_json) as WorkControl;
		this.#validateControl(control);
		taskAssert(
			control.controlId === row.control_id &&
				control.request.taskId === row.task_id &&
				workControlEventKey(control.request) === row.event_id &&
				control.sequence === row.sequence &&
				control.phase === row.phase &&
				control.clientRef === row.client_ref &&
				control.notificationId === row.notification_id &&
				control.version === row.version,
			"identity",
		);
		return control;
	}

	workControlList(taskId: string, afterSequence = 0, limit = 50): readonly WorkControl[] {
		taskAssert(
			Number.isSafeInteger(afterSequence) &&
				afterSequence >= 0 &&
				Number.isSafeInteger(limit) &&
				limit >= 1 &&
				limit <= 50,
		);
		return this.#database
			.query<{ control_id: string }, [string, number, number]>(
				"SELECT control_id FROM work_controls WHERE task_id = ? AND sequence > ? ORDER BY sequence LIMIT ?",
			)
			.all(taskId, afterSequence, limit)
			.map((row) => this.workControlGet(row.control_id)!);
	}

	#validateControl(control: WorkControl): void {
		const request = control.request;
		validateTaskEvidence(request.evidence);
		taskAssert(
			["steer", "cancel_request", "reset_notice"].includes(request.kind) &&
				["read_only", "code_mutating"].includes(request.scope) &&
				taskText(request.body, 16 * 1024),
		);
		taskAssert(workString(request.taskId) && workString(request.expectedOpRef));
		taskAssert(
			control.requestHash === controlRequestHash(request) &&
				control.controlId === `control-${workControlEventKey(request)}` &&
				control.clientRef === (request.kind === "steer" ? `fm-steer-${workControlEventKey(request)}` : null) &&
				control.notificationId === `lane-report-${taskHash("work-control-notification", control.controlId)}`,
			"identity",
		);
		taskAssert(
			Number.isSafeInteger(control.sequence) &&
				control.sequence > 0 &&
				Number.isSafeInteger(control.version) &&
				control.version >= 0,
		);
		taskAssert(["pending", "sending", "accepted", "refused", "held"].includes(control.phase));
		taskAssert(
			(control.sessionId === null && control.epoch === null) ||
				(workString(control.sessionId) && Number.isSafeInteger(control.epoch) && control.epoch! >= 0),
		);
		taskAssert(control.reason === null || taskText(control.reason, 2048));
		taskAssert(!["held", "refused"].includes(control.phase) || control.reason !== null);
		taskAssert(workTime(control.createdAt) && workTime(control.updatedAt));
		taskAssert(control.sendingAt === null || workTime(control.sendingAt));
		taskAssert(
			control.phase !== "sending" ||
				(request.kind === "steer" && control.sendingAt !== null && control.sessionId !== null),
		);
		taskAssert(control.phase !== "pending" || (control.sendingAt === null && control.receipt === null));
		if (control.receipt !== null) {
			const receipt = control.receipt;
			taskAssert(
				["turn.steer", "turn.steer_status"].includes(receipt.source) &&
					receipt.opRef === request.expectedOpRef &&
					receipt.sessionId === control.sessionId &&
					receipt.epoch === control.epoch &&
					receipt.clientRef === control.clientRef &&
					receipt.eventId === request.evidence.eventId &&
					receipt.outcome === control.phase &&
					workTime(receipt.observedAt) &&
					taskText(receipt.evidence, 4096) &&
					control.sendingAt !== null,
				"identity",
			);
		}
		taskAssert(request.kind !== "steer" || control.phase !== "accepted" || control.receipt !== null);
		if (control.notificationDisposition !== null)
			taskAssert(
				["inbound", "delivery"].includes(control.notificationDisposition.kind) &&
					workString(control.notificationDisposition.id),
			);
	}

	workControlAdmitInTransaction(
		request: WorkControlRequest,
		scopeAdmission?: WorkControlScopeAdmission,
		routeAdmission?: WorkControlRouteAdmission,
	): WorkTaskAdmission<WorkControl> {
		this.requireTransaction();
		validateTaskEvidence(request.evidence);
		const task = this.workTaskGet(request.taskId);
		taskAssert(task, "unavailable");
		this.#assertNotQuarantined("work", task.jobId);
		const key = workControlEventKey(request);
		const existing = this.workControlGet(`control-${key}`);
		const requestHash = controlRequestHash(request);
		if (existing)
			return { disposition: existing.requestHash === requestHash ? "duplicate" : "conflict", record: existing };
		if (request.evidence.editId !== null)
			taskAssert(
				this.workControlGet(
					`control-${workControlEventKey({
						...request,
						evidence: { ...request.evidence, editId: null },
					})}`,
				) !== undefined,
				"unavailable",
			);
		taskAssert(task.opRef === request.expectedOpRef && task.thread !== null, "identity");
		if (routeAdmission) {
			const source = this.workTaskSourceGet(routeAdmission.sourceId);
			taskAssert(
				task.surfacePhase === "bound" &&
					routeAdmission.taskId === task.taskId &&
					routeAdmission.opRef === task.opRef &&
					taskJson(routeAdmission.thread) === taskJson(task.thread) &&
					source?.taskId === task.taskId &&
					source.kind === "instruction" &&
					source.completeness === "complete" &&
					source.body === request.body &&
					source.evidence.principalId === request.evidence.principalId &&
					source.evidence.eventId === request.evidence.eventId &&
					source.evidence.editId === request.evidence.editId &&
					taskJson(source.evidence.origin) === taskJson(request.evidence.origin),
				"identity",
			);
		} else taskAssert(taskJson(task.thread) === taskJson(request.evidence.origin), "identity");
		const runtime =
			task.dispatchPhase === "prepared"
				? this.#workTaskRuntime(task, { opRef: task.opRef, sessionId: task.sessionId!, epoch: task.epoch! })
				: undefined;
		if (scopeAdmission) {
			const source = this.workTaskSourceGet(scopeAdmission.sourceId);
			taskAssert(
				request.kind === "steer" &&
					request.scope === "code_mutating" &&
					scopeAdmission.kind === request.scope &&
					scopeAdmission.taskId === task.taskId &&
					scopeAdmission.opRef === task.opRef &&
					scopeAdmission.cwd === task.request.cwd &&
					scopeAdmission.eventId === request.evidence.eventId &&
					workTime(scopeAdmission.validatedAt) &&
					task.surfacePhase === "bound" &&
					source?.taskId === task.taskId &&
					source.kind === "instruction" &&
					source.completeness === "complete" &&
					source.body === request.body &&
					source.evidence.principalId === request.evidence.principalId &&
					source.evidence.eventId === request.evidence.eventId &&
					source.evidence.editId === request.evidence.editId &&
					taskJson(source.evidence.origin) === taskJson(request.evidence.origin),
				"identity",
			);
		}
		const reason =
			request.scope === "code_mutating" && task.request.kind === "read_only" && !scopeAdmission
				? "scope_elevation_refused"
				: runtime?.terminal || task.obligationState === "final_admitted"
					? "original_attempt_terminal"
					: request.kind === "cancel_request"
						? "local_operator_action_required"
						: null;
		const phase: WorkControl["phase"] =
			reason === "local_operator_action_required"
				? "held"
				: reason !== null
					? "refused"
					: request.kind === "reset_notice"
						? "accepted"
						: "pending";
		const sequence = this.#database
			.query<{ sequence: number }, [string]>(
				"SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM work_controls WHERE task_id = ?",
			)
			.get(task.taskId)!.sequence;
		const controlId = `control-${key}`;
		const control: WorkControl = {
			controlId,
			request: JSON.parse(taskJson(request)),
			requestHash,
			sequence,
			clientRef: request.kind === "steer" ? `fm-steer-${key}` : null,
			notificationId: `lane-report-${taskHash("work-control-notification", controlId)}`,
			notificationDisposition: null,
			sessionId: task.sessionId,
			epoch: task.epoch,
			phase,
			reason,
			sendingAt: null,
			receipt: null,
			createdAt: request.evidence.observedAt,
			updatedAt: request.evidence.observedAt,
			version: 0,
		};
		this.#validateControl(control);
		this.#database
			.query(
				"INSERT INTO work_controls(control_id, task_id, event_id, sequence, client_ref, notification_id, phase, version, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				control.controlId,
				task.taskId,
				key,
				sequence,
				control.clientRef,
				control.notificationId,
				phase,
				0,
				JSON.stringify(control),
			);
		if (scopeAdmission && phase === "pending")
			this.workTaskSourceAppendInTransaction({
				sourceId: `scope-${controlId}`,
				taskId: task.taskId,
				kind: "decision",
				body: taskJson({ scopeAdmission, requestHash }),
				evidence: request.evidence,
				supersedes: null,
				completeness: "complete",
				controlId,
				reportId: null,
			});
		if (routeAdmission)
			this.workTaskSourceAppendInTransaction({
				sourceId: `route-${controlId}`,
				taskId: task.taskId,
				kind: "decision",
				body: taskJson({ routeAdmission, requestHash }),
				evidence: request.evidence,
				supersedes: null,
				completeness: "complete",
				controlId,
				reportId: null,
			});
		return { disposition: "created", record: control };
	}

	/** pending→sending is the single send claim; held/sending can never return to pending. */
	workControlTransitionInTransaction(
		controlId: string,
		expectedVersion: number,
		input: {
			phase: "sending" | "accepted" | "refused" | "held";
			identity: WorkTaskAttemptIdentity;
			at: string;
			reason?: string;
			receipt?: WorkControlReceipt;
		},
	): WorkControl | undefined {
		this.requireTransaction();
		const control = this.workControlGet(controlId);
		if (!control || control.version !== expectedVersion) return undefined;
		const task = this.workTaskGet(control.request.taskId);
		taskAssert(task, "unavailable");
		this.#assertNotQuarantined("work", task.jobId);
		const runtime = this.#workTaskRuntime(task, input.identity);
		taskAssert(
			control.request.expectedOpRef === input.identity.opRef &&
				(control.sessionId === null ||
					(control.sessionId === input.identity.sessionId && control.epoch === input.identity.epoch)),
			"identity",
		);
		taskAssert(workTime(input.at) && control.request.kind === "steer", "transition");
		if (input.phase === "sending") {
			taskAssert(
				control.phase === "pending" &&
					control.sendingAt === null &&
					input.receipt === undefined &&
					runtime.sendPhase === "accepted" &&
					runtime.terminal === null &&
					runtime.settledAt === null &&
					task.obligationState !== "final_admitted" &&
					task.surfacePhase === "bound",
				"transition",
			);
			const binding = this.getSessionRecord(runtime.sessionKey);
			taskAssert(binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch, "identity");
			// Only the local cancellation notification is exempt. Pending/held steers
			// retain owner order even if never sent; uncertain sends always block.
			taskAssert(
				!this.#database
					.query(
						`SELECT 1 FROM work_controls WHERE task_id = ? AND sequence < ?
					AND phase NOT IN ('accepted','refused')
					AND NOT COALESCE((phase = 'held' AND json_extract(record_json, '$.request.kind') = 'cancel_request'
						AND json_type(record_json, '$.sendingAt') = 'null'
						AND json_type(record_json, '$.receipt') = 'null'
						AND json_extract(record_json, '$.reason') = 'local_operator_action_required'), 0) LIMIT 1`,
					)
					.get(task.taskId, control.sequence),
				"order",
			);
		} else if (input.phase === "accepted" || (input.phase === "refused" && control.sendingAt !== null)) {
			taskAssert(
				(control.phase === "sending" || control.phase === "held") &&
					input.receipt !== undefined &&
					input.receipt.outcome === input.phase,
				"transition",
			);
		} else {
			taskAssert(
				(input.phase === "held" && (control.phase === "pending" || control.phase === "sending")) ||
					(input.phase === "refused" && control.phase === "pending"),
				"transition",
			);
			taskAssert(input.receipt === undefined, "transition");
		}
		taskAssert(!["refused", "held"].includes(input.phase) || taskText(input.reason, 2048));
		const next: WorkControl = {
			...control,
			phase: input.phase,
			sessionId: runtime.sessionId,
			epoch: runtime.epoch,
			sendingAt: input.phase === "sending" ? input.at : control.sendingAt,
			reason: input.reason ?? null,
			receipt: input.receipt ?? null,
			updatedAt: input.at,
			version: control.version + 1,
		};
		this.#workControlCas(control, next);
		return next;
	}

	#workControlCas(current: WorkControl, next: WorkControl): void {
		this.#validateControl(next);
		taskAssert(
			next.controlId === current.controlId &&
				next.requestHash === current.requestHash &&
				next.sequence === current.sequence &&
				next.version === current.version + 1,
			"identity",
		);
		taskAssert(
			this.#database
				.query("UPDATE work_controls SET phase = ?, version = ?, record_json = ? WHERE control_id = ? AND version = ?")
				.run(next.phase, next.version, JSON.stringify(next), current.controlId, current.version).changes === 1,
			"conflict",
		);
	}

	workTaskSourceAppendInTransaction(input: WorkTaskSourceInput, delivery?: ChatMessagePayload): WorkTaskSource {
		taskAssert(input.administrative === undefined && !/^disposition-[0-9a-f]{64}$/.test(input.sourceId), "invalid");
		taskAssert(!/^unavailability-[0-9a-f]{64}$/.test(input.sourceId), "invalid");
		taskAssert(input.review === undefined && !input.sourceId.startsWith("review-"), "invalid");
		return this.#workTaskSourceAppendInTransaction(input, delivery);
	}

	/**
	 * Presentation only for independently anchored retained negative sources.
	 * Undefined selects the ordinary strict path; validation failures never do.
	 * Neither runtime validity nor execution/control/release authority is inferred.
	 */
	workTaskNegativeDeliveryConfirmInTransaction(input: unknown) {
		this.requireTransaction();
		const params = validateDeliveryConfirmParams(input);
		const current = this.deliveryGet(params.deliveryId);
		taskAssert(current, "unavailable");
		const payload = JSON.parse(current.payload_json) as ChatMessagePayload;
		const metadata = validateWorkTaskMessageMetadata(payload.workTask, { origin: payload.origin });
		const source = this.workTaskSourceGet(metadata.sourceId);
		taskAssert(source, "identity");
		if (!/^unavailability-[0-9a-f]{64}$/.test(source.sourceId) && !source.administrative?.request.validationUnavailable)
			return undefined;
		const { task, mapped } = this.#workTaskNegativeAnchor(metadata.taskId);
		taskAssert(mapped && task.thread, "identity");
		if (!source.administrative) this.#validateWorkTaskUnavailable(task, mapped, source);
		// Administrative source reads validate the original negative observation and
		// its admission; both kinds still require this exact retained presentation.
		taskAssert(
			source.taskId === task.taskId &&
				metadata.opRef === task.opRef &&
				source.deliveryId === params.deliveryId &&
				source.deliveryId === workTaskSourceDeliveryId(task.taskId, source.sourceId, task.thread) &&
				current.turn_id === task.opRef &&
				current.origin_key === originKey(task.thread) &&
				payload.deliveryId === current.delivery_id &&
				payload.turnId === task.opRef &&
				taskJson(payload.origin) === taskJson(task.thread) &&
				payload.text === source.body &&
				payload.role === "assistant" &&
				payload.final === false &&
				!payload.reaction &&
				!payload.voiceText,
			"identity",
		);
		const receipt = params.platformReceipt;
		taskAssert(receipt && taskJson(receipt.origin) === taskJson(task.thread), "identity");
		taskAssert(["pending", "inflight", "confirmed"].includes(current.state), "transition");
		const firstId = receipt.messageIds[0]!;
		const evidenceAt = new Date(Number((BigInt(firstId) >> 22n) + 1420070400000n)).toISOString();
		const sourceId = `receipt-${createHash("sha256").update(current.delivery_id).digest("hex")}`;
		const body = JSON.stringify({
			deliveryId: current.delivery_id,
			firstMessageId: firstId,
			messageCount: receipt.messageIds.length,
			messageIdsSha256: createHash("sha256").update(JSON.stringify(receipt.messageIds)).digest("hex"),
		});
		const previous = this.workTaskSourceGet(sourceId);
		taskAssert((!previous || previous.body === body) && (current.state !== "confirmed" || previous), "conflict");
		const outcome = this.deliveryConfirmWithSettleInTransaction(current.delivery_id, "delivered");
		this.#workTaskSourceAppendInTransaction(
			{
				sourceId,
				taskId: task.taskId,
				kind: "observation",
				body,
				evidence: {
					origin: task.thread,
					principalId: "gateway:discord-adapter",
					eventId: firstId,
					editId: null,
					evidenceAt,
					observedAt: previous?.evidence.observedAt ?? new Date().toISOString(),
				},
				supersedes: null,
				completeness: "complete",
				controlId: null,
				reportId: null,
			},
			undefined,
			task,
		);
		return outcome;
	}

	/**
	 * Server supplies evidence only after existing owner authentication/routing.
	 * WorkTaskEvidence is attribution, NOT a caller credential or an authorization API.
	 * No runtime/control/report write is performed. The enclosing transaction must
	 * propagate failures so CAS, source audit and optional mapped intent roll back.
	 */
	workTaskDispositionInTransaction(
		params: WorkTaskDispositionParams,
		trustedEvidence: WorkTaskEvidence,
		delivery?: ChatMessagePayload,
	): WorkTaskDispositionResult {
		this.requireTransaction();
		const request = validateWorkTaskDispositionParams(params);
		taskAssert(request.validationUnavailable === undefined, "identity");
		validateTaskEvidence(trustedEvidence);
		taskAssert(
			trustedEvidence.eventId === request.eventId &&
				trustedEvidence.editId === null &&
				taskText(trustedEvidence.principalId, 1024),
			"identity",
		);
		const task = this.workTaskGet(request.taskId);
		taskAssert(task, "unavailable");
		this.#assertNotQuarantined("work", task.jobId);
		const sourceId = workTaskDispositionId(task.taskId, request.eventId, trustedEvidence.origin);
		if (delivery) taskAssert(delivery.text === workTaskDispositionText(request), "identity");
		const existing = this.workTaskSourceGet(sourceId);
		if (existing) {
			const record = existing.administrative;
			taskAssert(
				record &&
					taskJson(record.request) === taskJson(request) &&
					record.principalId === trustedEvidence.principalId &&
					taskJson(record.origin) === taskJson(trustedEvidence.origin),
				"conflict",
			);
			// Receipt/observation times may differ on a retry; immutable input may not.
			if (delivery) {
				taskAssert(existing.deliveryId !== null, "conflict");
				this.#workTaskSourceAppendInTransaction(
					{
						sourceId: existing.sourceId,
						taskId: existing.taskId,
						kind: existing.kind,
						body: existing.body,
						evidence: existing.evidence,
						supersedes: existing.supersedes,
						completeness: existing.completeness,
						controlId: existing.controlId,
						reportId: existing.reportId,
						administrative: record,
					},
					delivery,
				);
			}
			return validateWorkTaskDispositionResult({
				execution: "none",
				disposition: "duplicate",
				dispositionId: sourceId,
				sourceId,
				deliveryId: existing.deliveryId,
				record,
			});
		}
		taskAssert(
			task.jobId === request.jobId &&
				task.opRef === request.expectedOpRef &&
				task.sessionId === request.sessionId &&
				task.epoch === request.epoch &&
				task.request.cwd === request.cwd &&
				task.requestHash === request.requestHash,
			"identity",
		);
		taskAssert(task.version === request.expectedTaskVersion, "conflict");
		if (request.target.kind === "control") {
			const control = this.workControlGet(request.target.controlId);
			taskAssert(
				control &&
					control.request.taskId === task.taskId &&
					control.request.expectedOpRef === task.opRef &&
					control.request.evidence.eventId === request.target.eventId &&
					control.clientRef === request.target.clientRef &&
					control.sessionId === task.sessionId &&
					control.epoch === task.epoch,
				"identity",
			);
			taskAssert(control.phase === "held" && control.receipt === null, "transition");
		} else {
			taskAssert(task.terminalReportId === request.target.reportId, "identity");
			taskAssert(task.obligationState === "held", "transition");
		}
		const record = validateWorkTaskDispositionRecord({
			request,
			principalId: trustedEvidence.principalId,
			origin: trustedEvidence.origin,
			recordedAt: trustedEvidence.observedAt,
			taskVersion: task.version + 1,
			retained: {
				obligationState: task.obligationState,
				reportId: task.terminalReportId,
				holdReason: task.holdReason,
				controlPhase: request.target.kind === "control" ? "held" : null,
			},
		});
		const source = this.#workTaskSourceAppendInTransaction(
			{
				sourceId,
				taskId: task.taskId,
				kind: "decision",
				body: workTaskDispositionText(request),
				evidence: trustedEvidence,
				supersedes: null,
				completeness: "incomplete",
				controlId: request.target.kind === "control" ? request.target.controlId : null,
				reportId: request.target.kind === "report" ? request.target.reportId : null,
				administrative: record,
			},
			delivery,
		);
		this.#workTaskCas(task, { ...task, version: record.taskVersion, updatedAt: record.recordedAt });
		return validateWorkTaskDispositionResult({
			execution: "none",
			disposition: "recorded",
			dispositionId: sourceId,
			sourceId,
			deliveryId: source.deliveryId,
			record,
		});
	}

	/**
	 * Owner authentication, current scope and captured proof remain server duties.
	 * The only task mutation is its audit CAS version; no runtime field is salvaged.
	 */
	workTaskNegativeDispositionInTransaction(
		params: WorkTaskDispositionParams,
		trustedEvidence: WorkTaskEvidence,
		surfaceAvailable: (origin: OriginRef) => boolean,
	): WorkTaskDispositionResult {
		this.requireTransaction();
		const request = validateWorkTaskDispositionParams(params);
		taskAssert(request.validationUnavailable, "identity");
		validateTaskEvidence(trustedEvidence);
		taskAssert(
			trustedEvidence.eventId === request.eventId &&
				trustedEvidence.editId === null &&
				taskText(trustedEvidence.principalId, 1024),
			"identity",
		);
		const { task, mapped } = this.#workTaskNegativeAnchor(request.taskId);
		taskAssert(mapped, "unavailable");
		const sourceId = workTaskDispositionId(task.taskId, request.eventId, trustedEvidence.origin);
		const existing = this.workTaskSourceGet(sourceId);
		if (existing) {
			taskAssert(
				existing.administrative &&
					taskJson(existing.administrative.request) === taskJson(request) &&
					existing.administrative.principalId === trustedEvidence.principalId &&
					taskJson(existing.administrative.origin) === taskJson(trustedEvidence.origin),
				"conflict",
			);
			return validateWorkTaskDispositionResult({
				execution: "none",
				disposition: "duplicate",
				dispositionId: sourceId,
				sourceId,
				deliveryId: existing.deliveryId,
				record: existing.administrative,
			});
		}
		const selected = this.workTaskDispositionBasis(task.taskId);
		taskAssert(selected.kind === "validation_unavailable", "unavailable");
		if (selected.kind !== "validation_unavailable") throw new WorkTaskStateError("unavailable");
		const basis = selected.basis;
		taskAssert(taskJson(selected.qualification) === taskJson(request.validationUnavailable), "conflict");
		taskAssert(
			basis.jobId === request.jobId &&
				basis.expectedOpRef === request.expectedOpRef &&
				basis.sessionId === request.sessionId &&
				basis.epoch === request.epoch &&
				basis.cwd === request.cwd &&
				basis.requestHash === request.requestHash,
			"identity",
		);
		taskAssert(basis.expectedTaskVersion === request.expectedTaskVersion, "conflict");
		taskAssert(
			request.target.kind === "report"
				? basis.report !== null && taskJson(basis.report) === taskJson(request.target)
				: basis.controls.some((target) => taskJson(target) === taskJson(request.target)),
			"identity",
		);
		const record = validateWorkTaskDispositionRecord({
			request,
			principalId: trustedEvidence.principalId,
			origin: trustedEvidence.origin,
			recordedAt: trustedEvidence.observedAt,
			taskVersion: task.version + 1,
			retained: {
				obligationState: task.obligationState,
				reportId: task.terminalReportId,
				holdReason: task.holdReason,
				controlPhase: request.target.kind === "control" ? "held" : null,
			},
		});
		let delivery: ChatMessagePayload | undefined;
		let deliver = false;
		try {
			deliver = surfaceAvailable(JSON.parse(taskJson(task.thread!))) === true;
		} catch {
			/* Audit only. */
		}
		if (deliver) {
			const deliveryId = workTaskSourceDeliveryId(task.taskId, sourceId, task.thread!);
			const body = buildDeliveryPayload(
				task.opRef,
				task.thread!,
				workTaskDispositionText(request),
				deliveryId,
				undefined,
				false,
			);
			taskAssert(body, "unavailable");
			delivery = { ...body, workTask: { taskId: task.taskId, opRef: task.opRef, sourceId, mappedOnly: true } };
		}
		// External presentation lookup must not change the captured admission or corruption.
		taskAssert(
			taskJson(this.#workTaskNegativeAnchor(task.taskId).task) === taskJson(task) &&
				taskJson(this.workTaskDispositionBasis(task.taskId)) === taskJson(selected),
			"conflict",
		);
		// Preserve all task fields, including updatedAt; only the administrative CAS advances.
		this.#workTaskCas(task, { ...task, version: record.taskVersion });
		const source = this.#workTaskSourceAppendInTransaction(
			{
				sourceId,
				taskId: task.taskId,
				kind: "decision",
				body: workTaskDispositionText(request),
				evidence: trustedEvidence,
				supersedes: null,
				completeness: "incomplete",
				controlId: request.target.kind === "control" ? request.target.controlId : null,
				reportId: request.target.kind === "report" ? request.target.reportId : null,
				administrative: record,
			},
			delivery,
			{ ...task, version: record.taskVersion },
		);
		return validateWorkTaskDispositionResult({
			execution: "none",
			disposition: "recorded",
			dispositionId: sourceId,
			sourceId,
			deliveryId: source.deliveryId,
			record,
		});
	}

	#workTaskSourceAppendInTransaction(
		input: WorkTaskSourceInput,
		delivery?: ChatMessagePayload,
		negativeAdmission?: WorkTask,
	): WorkTaskSource {
		this.requireTransaction();
		const task = negativeAdmission ?? this.workTaskGet(input.taskId);
		taskAssert(task, "unavailable");
		if (!negativeAdmission) this.#assertNotQuarantined("work", task.jobId);
		validateTaskEvidence(input.evidence);
		validateAdministrativeSource(input);
		if (input.review) {
			const review = validateWorkTaskReviewParams(input.review);
			taskAssert(
				input.kind === "decision" &&
					input.taskId === review.taskId &&
					input.sourceId === `review-${review.reviewId}` &&
					input.reportId === review.reportId &&
					input.evidence.principalId === `coordinator:${review.callerSessionId}` &&
					input.evidence.eventId === review.reviewId,
				"identity",
			);
		}
		taskAssert(
			workString(input.sourceId) &&
				taskText(input.body, 64 * 1024) &&
				["assignment", "instruction", "decision", "observation"].includes(input.kind) &&
				["complete", "incomplete"].includes(input.completeness),
		);
		taskAssert(
			input.kind !== "assignment" ||
				(input.sourceId === `assignment-${task.taskId}` &&
					input.body === task.request.text &&
					taskJson(input.evidence) === taskJson(task.request.evidence) &&
					input.supersedes === null),
			"identity",
		);
		if (input.supersedes !== null) {
			const previous = this.workTaskSourceGet(input.supersedes);
			taskAssert(
				previous?.taskId === input.taskId && previous.kind === input.kind && input.kind !== "assignment",
				"identity",
			);
		}
		if (input.controlId !== null)
			taskAssert(this.workControlGet(input.controlId)?.request.taskId === task.taskId, "identity");
		if (input.reportId !== null)
			taskAssert(input.reportId === workAttemptReportId(this.instanceId, task.jobId, task.opRef), "identity");
		const contentHash = taskHash("work-task-source", input);
		if (delivery) {
			taskAssert(task.thread !== null, "identity");
			try {
				validateWorkTaskMessageMetadata(delivery.workTask, {
					origin: delivery.origin,
					taskId: task.taskId,
					opRef: task.opRef,
					sourceId: input.sourceId,
				});
			} catch {
				throw new WorkTaskStateError("identity");
			}
		}
		const deliveryId = delivery ? workTaskSourceDeliveryId(task.taskId, input.sourceId, task.thread!) : null;
		const existing = this.workTaskSourceGet(input.sourceId);
		if (existing) {
			taskAssert(existing.contentHash === contentHash && existing.deliveryId === deliveryId, "conflict");
			if (delivery) this.#workTaskSourceDelivery(task, deliveryId!, delivery);
			return existing;
		}
		const sequence = this.#database
			.query<{ sequence: number }, [string]>(
				"SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM work_task_sources WHERE task_id = ?",
			)
			.get(task.taskId)!.sequence;
		taskAssert(Number.isSafeInteger(sequence) && sequence > 0);
		const source: WorkTaskSource = { ...JSON.parse(taskJson(input)), sequence, contentHash, deliveryId };
		if (delivery) this.#workTaskSourceDelivery(task, deliveryId!, delivery);
		this.#database
			.query(
				"INSERT INTO work_task_sources(source_id, task_id, sequence, content_hash, delivery_id, record_json) VALUES (?, ?, ?, ?, ?, ?)",
			)
			.run(source.sourceId, task.taskId, sequence, contentHash, deliveryId, JSON.stringify(source));
		return source;
	}

	#workTaskSourceDelivery(task: WorkTask, deliveryId: string, payload: ChatMessagePayload): void {
		taskAssert(
			task.thread !== null &&
				task.surfacePhase === "bound" &&
				payload.deliveryId === deliveryId &&
				payload.turnId === task.opRef &&
				taskJson(payload.origin) === taskJson(task.thread) &&
				taskText(payload.text, 1024 * 1024) &&
				payload.role === "assistant" &&
				typeof payload.final === "boolean" &&
				!isSilentOutput(payload.text) &&
				!payload.reaction &&
				!payload.voiceText,
			"identity",
		);
		this.deliveryCreateInTransaction({
			id: deliveryId,
			turnId: task.opRef,
			originKey: originKey(task.thread),
			payloadJson: JSON.stringify(payload),
		});
	}

	/** Current trusted-local persona fence; caller hints are not credentials. */
	workTaskReviewer(sessionId: string, epoch?: number): { origin: OriginRef; originKey: string; epoch: number } {
		const key = this.originForSessionId(sessionId);
		const binding = key ? this.getSessionRecord(key) : undefined;
		const rawOrigin = key ? this.getOriginByKey(key) : undefined;
		taskAssert(key && !key.startsWith("work/") && binding?.sessionId === sessionId && rawOrigin, "identity");
		const origin = validateOriginRef(rawOrigin as unknown as OriginRef);
		taskAssert(
			originKey(origin) === key &&
				(epoch === undefined || binding.epoch === epoch) &&
				!this.inboundHasQuarantinedNonterminalTurn(key) &&
				!(origin.platform === "discord" && this.workTaskDiscordLocator(origin.conversationId)),
			"identity",
		);
		return { origin, originKey: key, epoch: binding.epoch };
	}

	workTaskOriginalSource(taskId: string): WorkTaskSource | undefined {
		const task = this.workTaskGet(taskId);
		if (!task || task.obligationState !== "final_admitted") return;
		const source =
			this.workTaskSourceGet(`final-${taskId}-original`) ?? this.workTaskSourceGet(`supplement-${taskId}-original`);
		if (!source) return;
		taskAssert(
			source.taskId === taskId &&
				source.reportId === workAttemptReportId(this.instanceId, task.jobId, task.opRef) &&
				source.kind === "observation" &&
				source.evidence.eventId === task.opRef &&
				source.completeness === "complete",
			"identity",
		);
		return source;
	}

	#workTaskReviews(taskId: string): WorkTaskSource[] {
		return this.#database
			.query<{ source_id: string }, [string]>(
				"SELECT source_id FROM work_task_sources WHERE task_id = ? AND json_type(record_json, '$.review') = 'object' ORDER BY sequence",
			)
			.all(taskId)
			.map((row) => this.workTaskSourceGet(row.source_id)!);
	}

	workTaskReviewLocator(taskId: string): WorkTaskReviewLocator | undefined {
		const task = this.workTaskGet(taskId);
		const source = this.workTaskOriginalSource(taskId);
		if (!task || !source) return;
		const reviews = this.#workTaskReviews(taskId).filter(
			(entry) => entry.review!.sourceId === source.sourceId && entry.review!.contentHash === source.contentHash,
		);
		const questions = reviews.filter((entry) => entry.review!.disposition === "owner_question");
		return {
			taskId,
			opRef: task.opRef,
			reportId: source.reportId!,
			sourceId: source.sourceId,
			contentHash: source.contentHash,
			totalBytes: Buffer.byteLength(source.body, "utf8"),
			completeness: source.completeness,
			reviewId: reviews.at(-1)?.review?.reviewId ?? null,
			pending: reviews.length === 0,
			ownerQuestions: questions.map((entry) => ({
				reviewId: entry.review!.reviewId,
				question: entry.review!.question!,
				deliveryId: this.#workTaskReviewDeliveryId(entry.sourceId),
			})),
		};
	}

	/** Keyset scanning includes released tasks and advances even across corrupt records. */
	workTaskPendingReviews(coordinatorKey: string, afterTaskId = "", limit = 20): WorkTaskReviewPendingResult {
		taskAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 20);
		const page = this.workTaskKeys(limit, afterTaskId);
		const items: WorkTaskReviewLocator[] = [];
		const unavailableTaskIds: string[] = [];
		for (const key of page.keys) {
			try {
				const task = this.workTaskGet(key.taskId)!;
				if (originKey(task.request.coordinator) !== coordinatorKey) continue;
				const locator = this.workTaskReviewLocator(task.taskId);
				if (locator?.pending) items.push(locator);
			} catch {
				unavailableTaskIds.push(key.taskId);
			}
		}
		return { mode: "pending_reviews", items, nextTaskId: page.nextTaskId, unavailableTaskIds };
	}

	#workTaskReviewDeliveryId(sourceId: string): string {
		return `review-question-${taskHash("work-task-review-delivery", sourceId)}`;
	}

	workTaskReview(params: WorkTaskReviewParams, at = new Date().toISOString()): WorkTaskReviewResult {
		const input = validateWorkTaskReviewParams(params);
		return this.withTransaction((): WorkTaskReviewResult => {
			const reviewer = this.workTaskReviewer(input.callerSessionId, input.callerEpoch);
			const task = this.workTaskGet(input.taskId);
			const original = this.workTaskOriginalSource(input.taskId);
			taskAssert(
				task &&
					original &&
					task.opRef === input.expectedOpRef &&
					originKey(task.request.coordinator) === reviewer.originKey &&
					taskJson(task.request.coordinator) === taskJson(reviewer.origin) &&
					original.sourceId === input.sourceId &&
					original.contentHash === input.contentHash &&
					original.reportId === input.reportId &&
					original.completeness === "complete",
				"identity",
			);
			const sourceId = `review-${input.reviewId}`;
			const deliveryId = input.disposition === "owner_question" ? this.#workTaskReviewDeliveryId(sourceId) : null;
			const existing = this.workTaskSourceGet(sourceId);
			if (existing) {
				taskAssert(taskJson(existing.review) === taskJson(input), "conflict");
				return { execution: "none", disposition: "duplicate", sourceId, reviewId: input.reviewId, deliveryId };
			}
			const prior = this.#workTaskReviews(task.taskId).at(-1);
			taskAssert((prior?.review?.reviewId ?? null) === input.expectedReviewId, "order");
			const source = this.#workTaskSourceAppendInTransaction({
				sourceId,
				taskId: task.taskId,
				kind: "decision",
				body: `${input.disposition}: ${input.rationale}${input.question ? `\nOwner question: ${input.question}` : ""}\nReview of ${input.sourceId} (${input.contentHash}); not task success or an owner answer.`,
				evidence: {
					principalId: `coordinator:${input.callerSessionId}`,
					origin: reviewer.origin,
					eventId: input.reviewId,
					editId: null,
					evidenceAt: at,
					observedAt: at,
				},
				supersedes: prior?.sourceId ?? null,
				completeness: "complete",
				controlId: null,
				reportId: input.reportId,
				review: input,
			});
			if (deliveryId) {
				const thread = task.thread;
				const link =
					thread?.platform === "discord"
						? `https://discord.com/channels/${thread.boundaryId ?? "@me"}/${thread.conversationId}`
						: taskJson(thread);
				const payload = buildDeliveryPayload(
					task.opRef,
					task.request.coordinator,
					`Owner decision requested for task ${task.taskId}\n${input.question}\n${link}\nReview ${source.sourceId}; original ${original.sourceId} (${original.contentHash}).`,
					deliveryId,
				)!;
				this.deliveryCreateInTransaction({
					id: deliveryId,
					turnId: task.opRef,
					originKey: reviewer.originKey,
					payloadJson: JSON.stringify(payload),
				});
			}
			return { execution: "none", disposition: "recorded", sourceId, reviewId: input.reviewId, deliveryId };
		});
	}

	/** Admit only a never-admitted ORIGINAL fallback handoff, without rewriting settlement/history. */
	workTaskRecoverCoordinatorReport(
		taskId: string,
		holdReason?: string,
	): { reportId: string; originKey: string } | undefined {
		return this.withTransaction(() => {
			const task = this.workTaskGet(taskId);
			if (!task || holdReason || !this.workTaskReviewLocator(taskId)?.pending) return;
			const runtime = this.workAttemptGet(task.opRef);
			if (!runtime || runtime.decision !== "fallback" || !runtime.settledAt || runtime.parent?.kind !== "persona")
				return;
			const key = originKey(task.request.coordinator);
			if (
				runtime.parent.originKey !== key ||
				taskJson(runtime.parent.origin) !== taskJson(task.request.coordinator) ||
				(task.request.coordinator.platform === "discord" &&
					this.workTaskDiscordLocator(task.request.coordinator.conversationId)) ||
				!this.getSessionRecord(key)?.sessionId ||
				this.inboundHasQuarantinedNonterminalTurn(key) ||
				this.#database.query("SELECT 1 FROM inbound_messages WHERE message_id = ?").get(runtime.reportId)
			)
				return;
			const fallback = this.deliveryGet(runtime.deliveryId);
			taskAssert(fallback && fallback.origin_key === key && fallback.turn_id === task.opRef, "identity");
			const payload = JSON.parse(fallback.payload_json) as ChatMessagePayload;
			taskAssert(
				payload.deliveryId === runtime.deliveryId &&
					payload.turnId === task.opRef &&
					taskJson(payload.origin) === taskJson(task.request.coordinator) &&
					taskText(payload.text, 2048),
				"identity",
			);
			if (
				!this.inboundEnqueueInTransaction({
					messageId: runtime.reportId,
					originKey: key,
					originRefJson: JSON.stringify(task.request.coordinator),
					body: payload.text,
					receivedAt: runtime.settledAt,
					source: "lane_report",
				})
			)
				return;
			return { reportId: runtime.reportId, originKey: key };
		});
	}

	workTaskSourceGet(sourceId: string): WorkTaskSource | undefined {
		const row = this.#database
			.query<
				{
					source_id: string;
					task_id: string;
					sequence: number;
					content_hash: string;
					delivery_id: string | null;
					record_json: string;
				},
				[string]
			>("SELECT * FROM work_task_sources WHERE source_id = ?")
			.get(sourceId);
		if (!row) return undefined;
		const source = JSON.parse(row.record_json) as WorkTaskSource;
		const { sequence, contentHash, deliveryId, ...input } = source;
		validateTaskEvidence(source.evidence);
		validateAdministrativeSource(source);
		if (source.review) {
			const review = validateWorkTaskReviewParams(source.review);
			taskAssert(
				source.kind === "decision" &&
					source.taskId === review.taskId &&
					source.sourceId === `review-${review.reviewId}` &&
					source.reportId === review.reportId &&
					source.evidence.principalId === `coordinator:${review.callerSessionId}` &&
					source.evidence.eventId === review.reviewId,
				"identity",
			);
		}
		taskAssert(
			workString(source.sourceId) &&
				taskText(source.body, 64 * 1024) &&
				["assignment", "instruction", "decision", "observation"].includes(source.kind) &&
				["complete", "incomplete"].includes(source.completeness) &&
				Number.isSafeInteger(sequence) &&
				sequence > 0,
		);
		taskAssert(
			source.sourceId === row.source_id &&
				source.taskId === row.task_id &&
				sequence === row.sequence &&
				contentHash === row.content_hash &&
				deliveryId === row.delivery_id &&
				contentHash === taskHash("work-task-source", input),
			"identity",
		);
		if (source.administrative) {
			const record = source.administrative;
			const request = record.request;
			const task = request.validationUnavailable
				? this.#workTaskNegativeAnchor(source.taskId).task
				: this.workTaskGet(source.taskId);
			taskAssert(
				task &&
					task.version >= record.taskVersion &&
					task.jobId === request.jobId &&
					task.opRef === request.expectedOpRef &&
					task.requestHash === request.requestHash &&
					task.request.cwd === request.cwd,
				"identity",
			);
			// A known original binding cannot be replaced. A genuinely pre-attempt
			// unknown snapshot does not assert a subsequently acquired identity.
			taskAssert(
				request.sessionId === null || (task.sessionId === request.sessionId && task.epoch === request.epoch),
				"identity",
			);
			if (request.validationUnavailable) {
				const negative = this.workTaskSourceGet(request.validationUnavailable.sourceId);
				taskAssert(negative, "identity");
				this.#validateWorkTaskUnavailable(task, this.#workTaskNegativeAnchor(task.taskId).mapped, negative);
				taskAssert(
					negative.evidence.observedAt === request.validationUnavailable.firstObservedAt &&
						request.validationUnavailable.scope === task.request.kind &&
						negative.sequence < sequence,
					"identity",
				);
				// This authentic historical snapshot does not assert current runtime corruption.
			}
			if (request.target.kind === "control") {
				const control = this.workControlGet(request.target.controlId);
				taskAssert(
					control &&
						control.request.taskId === task.taskId &&
						control.request.expectedOpRef === request.expectedOpRef &&
						control.request.evidence.eventId === request.target.eventId &&
						control.clientRef === request.target.clientRef,
					"identity",
				);
				if (request.validationUnavailable) this.#workTaskDispositionControl(task, control, true);
			} else
				taskAssert(
					request.target.reportId === null ||
						request.target.reportId === workAttemptReportId(this.instanceId, task.jobId, task.opRef),
					"identity",
				);
			taskAssert(
				deliveryId === null ||
					(task.thread !== null && deliveryId === workTaskSourceDeliveryId(task.taskId, source.sourceId, task.thread)),
				"identity",
			);
		}
		return source;
	}

	/** Source/attempt revisions only; a wider projection must also manifest its selected delivery/inbox rows. */
	workTaskSources(
		taskId: string,
		options: { afterSequence?: number; limit?: number; manifest?: string; recent?: boolean } = {},
	):
		| {
				taskId: string;
				taskVersion: number;
				manifest: string;
				newSnapshot: boolean;
				sources: readonly WorkTaskSource[];
				omitted: number;
				overflow: boolean;
				lastSequence: number;
				byteLimit: number;
				recordLimit: number;
		  }
		| undefined {
		return this.withTransaction(() => this.workTaskSourcesInTransaction(taskId, options));
	}

	/** Allows the read model to select delivery and report facts in the same snapshot. */
	workTaskSourcesInTransaction(
		taskId: string,
		options: { afterSequence?: number; limit?: number; manifest?: string; recent?: boolean } = {},
	):
		| {
				taskId: string;
				taskVersion: number;
				manifest: string;
				newSnapshot: boolean;
				sources: readonly WorkTaskSource[];
				omitted: number;
				overflow: boolean;
				lastSequence: number;
				byteLimit: number;
				recordLimit: number;
		  }
		| undefined {
		this.requireTransaction();
		const after = options.afterSequence ?? 0;
		const limit = options.limit ?? 50;
		taskAssert(options.recent === undefined || typeof options.recent === "boolean");
		taskAssert(!options.recent || options.afterSequence === undefined);
		taskAssert(Number.isSafeInteger(after) && after >= 0 && Number.isSafeInteger(limit) && limit >= 1 && limit <= 50);
		const task = this.workTaskGet(taskId);
		if (!task) return undefined;
		const controls = this.#database
			.query<{ count: number; revision: number }, [string]>(
				"SELECT COUNT(*) AS count, COALESCE(SUM(version), 0) AS revision FROM work_controls WHERE task_id = ?",
			)
			.get(taskId)!;
		const count = this.#database
			.query<{ total: number; remaining: number }, [number, string]>(
				"SELECT COUNT(*) AS total, COALESCE(SUM(sequence > ?), 0) AS remaining FROM work_task_sources WHERE task_id = ?",
			)
			.get(after, taskId)!;
		const runtimeVersion = this.workAttemptGet(task.opRef)?.version ?? null;
		const manifest = taskHash("work-task-manifest", [
			taskId,
			task.version,
			runtimeVersion,
			controls,
			count.total,
			...(options.recent ? ["recent", limit] : []),
		]);
		const rows = this.#database
			.query<{ source_id: string; sequence: number }, [string, number, number]>(
				options.recent
					? "SELECT source_id, sequence FROM work_task_sources WHERE task_id = ? AND sequence > ? ORDER BY sequence DESC, source_id DESC LIMIT ?"
					: "SELECT source_id, sequence FROM work_task_sources WHERE task_id = ? AND sequence > ? ORDER BY sequence, source_id LIMIT ?",
			)
			.all(taskId, after, limit);
		const sources: WorkTaskSource[] = [];
		const snapshot = {
			taskId,
			taskVersion: task.version,
			manifest,
			newSnapshot: options.manifest !== undefined && options.manifest !== manifest,
			sources,
			omitted: count.remaining,
			overflow: false,
			lastSequence: rows.at(-1)?.sequence ?? after,
			byteLimit: 16 * 1024,
			recordLimit: limit,
		};
		// Include the envelope and separators. Omitted only decreases; true is shorter than false.
		let bytes = Buffer.byteLength(JSON.stringify(snapshot), "utf8");
		let overflow = false;
		for (const row of rows) {
			const source = this.workTaskSourceGet(row.source_id)!;
			const size = Buffer.byteLength(JSON.stringify(source), "utf8") + (sources.length === 0 ? 0 : 1);
			if (bytes + size > 16 * 1024) {
				overflow = true;
				continue;
			}
			sources.push(source);
			bytes += size;
		}
		return { ...snapshot, omitted: count.remaining - sources.length, overflow };
	}

	workAttemptGet(opRef: string): WorkAttemptRuntime | undefined {
		const row = this.#database
			.query<
				{
					op_ref: string;
					job_id: string;
					lane_key: string;
					session_id: string;
					version: number;
					settled_at: string | null;
					delivery_id: string;
					record_json: string;
				},
				[string]
			>("SELECT * FROM work_attempt_runtime WHERE op_ref = ?")
			.get(opRef);
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

	/** Keyset pagination: callers can recover arbitrarily many lanes in bounded reads. */
	workAttemptOpen(
		limit = 100,
		afterOpRef = "",
		onInvalid?: (error: WorkAttemptStateError) => void,
	): readonly WorkAttemptRuntime[] {
		workAssert(Number.isSafeInteger(limit) && limit >= 1 && limit <= 1000);
		const rows = this.#database
			.query<{ op_ref: string }, [string, number]>(
				"SELECT op_ref FROM work_attempt_runtime WHERE settled_at IS NULL AND op_ref > ? AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id) ORDER BY op_ref LIMIT ?",
			)
			.all(afterOpRef, limit);
		if (!onInvalid) return rows.map((row) => this.workAttemptGet(row.op_ref)!);
		const attempts: WorkAttemptRuntime[] = [];
		for (const row of rows) {
			try {
				const runtime = this.workAttemptGet(row.op_ref);
				if (runtime) attempts.push(runtime);
			} catch (error) {
				if (!(error instanceof WorkAttemptStateError)) throw error;
				onInvalid(error);
			}
		}
		return attempts;
	}

	workAttemptOpenByLane(laneKey: string): WorkAttemptRuntime | undefined {
		const row = this.#database
			.query<{ op_ref: string }, [string]>(
				"SELECT op_ref FROM work_attempt_runtime WHERE lane_key = ? AND settled_at IS NULL AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = work_attempt_runtime.job_id)",
			)
			.get(laneKey);
		return row ? this.workAttemptGet(row.op_ref) : undefined;
	}

	/**
	 * Gets the job ID for a given work attempt opRef (issue #407).
	 */
	workAttemptJobId(opRef: string): string | undefined {
		const row = this.#database
			.query<{ job_id: string }, [string]>("SELECT job_id FROM work_attempt_runtime WHERE op_ref = ?")
			.get(opRef);
		return row?.job_id;
	}

	/**
	 * Quarantines an invalid work attempt to exclude it from future recovery sweeps (issue #407).
	 * Called during recovery when an attempt fails validation.
	 */
	workAttemptQuarantineInvalid(jobId: string): void {
		this.#database
			.query(
				"INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES (?, ?, ?) ON CONFLICT(kind, subject_id) DO NOTHING",
			)
			.run("work", jobId, "recovery-auto-quarantine");
	}

	#laneReportGetInside(reportId: string): LaneReportRow | undefined {
		const row = this.#database
			.query<LaneReportRow, [string]>("SELECT * FROM lane_reports WHERE report_id = ?")
			.get(reportId);
		if (!row) return undefined;
		try {
			validateLaneReportRow(row);
			return row;
		} catch {
			throw new WorkAttemptStateError();
		}
	}

	laneReportGet(reportId: string): LaneReportRow | undefined {
		return this.#laneReportGetInside(reportId);
	}

	laneReportsByParent(parentName: string): readonly LaneReportRow[] {
		return this.#database
			.query<LaneReportRow, [string]>("SELECT * FROM lane_reports WHERE parent_name = ? ORDER BY created_at, rowid")
			.all(parentName)
			.map((row) => {
				try {
					validateLaneReportRow(row);
					return row;
				} catch {
					throw new WorkAttemptStateError();
				}
			});
	}

	laneReportParentNames(): readonly string[] {
		return this.#database
			.query<{ parent_name: string }, []>(
				"SELECT DISTINCT parent_name FROM lane_reports WHERE state IN ('pending','claimed','held') ORDER BY parent_name",
			)
			.all()
			.map((row) => row.parent_name);
	}

	laneReportCounts(parentName: string): { pending: number; claimed: number; held: number; undeliverable: number } {
		const row = this.#database
			.query<{ pending: number; claimed: number; held: number; undeliverable: number }, [string]>(
				"SELECT SUM(state = 'pending') AS pending, SUM(state = 'claimed') AS claimed, SUM(state = 'held') AS held, SUM(state = 'undeliverable') AS undeliverable FROM lane_reports WHERE parent_name = ?",
			)
			.get(parentName);
		return {
			pending: row?.pending ?? 0,
			claimed: row?.claimed ?? 0,
			held: row?.held ?? 0,
			undeliverable: row?.undeliverable ?? 0,
		};
	}

	laneReportClaim(
		reportId: string,
		kind: "steer" | "wake",
		ref: string,
		targetOpRef: string | null,
	): LaneReportRow | undefined {
		assertValidOpRef(ref);
		workAssert(kind === "steer" ? targetOpRef !== null : targetOpRef === null);
		if (targetOpRef !== null) assertValidOpRef(targetOpRef);
		return this.withTransaction(() => {
			const current = this.#laneReportGetInside(reportId);
			if (!current || current.state !== "pending") return undefined;
			const changed = this.#database
				.query(
					"UPDATE lane_reports SET state = 'claimed', claim_kind = ?, claim_ref = ?, claim_target_op_ref = ?, claim_seq = claim_seq + 1, hold_reason = NULL, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
				)
				.run(kind, ref, targetOpRef, new Date().toISOString(), reportId, current.claim_seq).changes;
			return changed === 1 ? this.#laneReportGetInside(reportId) : undefined;
		});
	}

	laneReportConsume(reportId: string, ref: string): boolean {
		return this.withTransaction(() => {
			const current = this.#laneReportGetInside(reportId);
			if (!current) return false;
			if (current.state === "consumed") return current.consumed_op_ref === ref;
			if ((current.state !== "claimed" && current.state !== "held") || current.claim_ref !== ref) return false;
			return (
				this.#database
					.query(
						"UPDATE lane_reports SET state = 'consumed', consumed_op_ref = ?, hold_reason = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_seq = ? AND claim_ref = ?",
					)
					.run(ref, new Date().toISOString(), reportId, current.state, current.claim_seq, ref).changes === 1
			);
		});
	}

	laneReportRequeue(reportId: string, ref: string): boolean {
		return this.withTransaction(() => {
			const current = this.#laneReportGetInside(reportId);
			if (!current || (current.state !== "claimed" && current.state !== "held") || current.claim_ref !== ref)
				return false;
			return (
				this.#database
					.query(
						"UPDATE lane_reports SET state = 'pending', claim_kind = NULL, claim_ref = NULL, claim_target_op_ref = NULL, hold_reason = NULL, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_seq = ? AND claim_ref = ?",
					)
					.run(new Date().toISOString(), reportId, current.state, current.claim_seq, ref).changes === 1
			);
		});
	}

	laneReportHold(reportId: string, reason: string): boolean {
		if (!workString(reason, 128)) throw new WorkAttemptStateError();
		return this.withTransaction(() => {
			const current = this.#laneReportGetInside(reportId);
			if (!current) return false;
			if (current.state === "held") return current.hold_reason === reason;
			if (current.state !== "claimed") return false;
			return (
				this.#database
					.query(
						"UPDATE lane_reports SET state = 'held', hold_reason = ?, updated_at = ? WHERE report_id = ? AND state = 'claimed' AND claim_seq = ?",
					)
					.run(reason, new Date().toISOString(), reportId, current.claim_seq).changes === 1
			);
		});
	}

	laneReportFallback(reportId: string, payload: ChatMessagePayload): boolean {
		return this.withTransaction(() => {
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
			const changed = this.#database
				.query(
					"UPDATE lane_reports SET state = 'fallback', updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
				)
				.run(new Date().toISOString(), reportId, current.claim_seq).changes;
			if (changed !== 1) return false;
			this.deliveryCreateInTransaction({
				id: deliveryId,
				turnId: current.child_op_ref,
				originKey: root.originKey,
				payloadJson: JSON.stringify(payload),
			});
			return true;
		});
	}

	laneReportUndeliverable(reportId: string): boolean {
		return this.withTransaction(() => {
			const current = this.#laneReportGetInside(reportId);
			if (!current || current.state !== "pending") return false;
			return (
				this.#database
					.query(
						"UPDATE lane_reports SET state = 'undeliverable', updated_at = ? WHERE report_id = ? AND state = 'pending' AND claim_seq = ?",
					)
					.run(new Date().toISOString(), reportId, current.claim_seq).changes === 1
			);
		});
	}

	/** Atomically append history, freeze notification intent and refresh activity before send. */
	workAttemptPrepare(runtime: WorkAttemptRuntime, record: LaneJobRecord): void {
		this.withTransaction(() => this.workAttemptPrepareInTransaction(runtime, record));
	}

	/** Joins task/source/delivery writes without a nested transaction or pre-commit publication. */
	workAttemptPrepareInTransaction(runtime: WorkAttemptRuntime, record: LaneJobRecord): void {
		this.requireTransaction();
		this.#assertNotQuarantined("work", runtime.jobId);
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
			workAssert(!previous?.attempts.some((attempt) => attempt.endedAt === undefined));
			workAssert(record.attempts.length === (previous?.attempts.length ?? 0) + 1);
			workAssert(JSON.stringify(record.attempts.slice(0, -1)) === JSON.stringify(previous?.attempts ?? []));
			const binding = this.getSessionRecord(runtime.sessionKey);
			workAssert(binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch);
		}
		this.#workPutHistory(runtime, record);
		this.#database
			.query(
				"INSERT INTO work_attempt_runtime (op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				runtime.opRef,
				runtime.jobId,
				runtime.laneKey,
				runtime.sessionId,
				runtime.version,
				null,
				runtime.deliveryId,
				JSON.stringify(runtime),
			);
		this.#workActivity(runtime, runtime.startedAt);
		const tasks = this.#database
			.query<{ task_id: string }, [string, string, string]>(
				"SELECT task_id FROM work_tasks WHERE job_id = ? OR op_ref = ? OR lane_name = ?",
			)
			.all(runtime.jobId, runtime.opRef, runtime.sessionKey.slice("work/task/".length));
		taskAssert(tasks.length <= 1, "identity");
		if (tasks[0]) {
			const task = this.workTaskGet(tasks[0].task_id)!;
			taskAssert(
				task.dispatchPhase === "pending" &&
					task.surfacePhase === "bound" &&
					task.opRef === runtime.opRef &&
					task.jobId === runtime.jobId &&
					runtime.sessionKey === `work/task/${task.laneName}` &&
					runtime.mode === "start" &&
					runtime.cwd === task.request.cwd &&
					record.attempts.length === 1 &&
					runtime.parent?.kind === "persona" &&
					runtime.parent.originKey === originKey(task.request.coordinator) &&
					taskJson(runtime.parent.origin) === taskJson(task.request.coordinator),
				"identity",
			);
			this.#workTaskCas(task, {
				...task,
				dispatchPhase: "prepared",
				sessionId: runtime.sessionId,
				epoch: runtime.epoch,
				version: task.version + 1,
				updatedAt: runtime.startedAt,
			});
		}
	}

	/** CAS staging, including output-read claims, acceptance proof, and notice receipt. */
	workAttemptUpdate(opRef: string, expectedVersion: number, patch: WorkAttemptPatch): WorkAttemptRuntime | undefined {
		return this.withTransaction(() => this.workAttemptUpdateInTransaction(opRef, expectedVersion, patch));
	}

	workAttemptUpdateInTransaction(
		opRef: string,
		expectedVersion: number,
		patch: WorkAttemptPatch,
	): WorkAttemptRuntime | undefined {
		this.requireTransaction();
		const current = this.workAttemptGet(opRef);
		if (!current || current.version !== expectedVersion || current.settledAt !== null) return undefined;
		this.#assertNotQuarantined("work", current.jobId);
		const next = this.#workPatched(current, patch);
		workAssert(next.decision === "undecided" && next.settledAt === null);
		const acceptanceEstablished = !workAccepted(current) && workAccepted(next);
		if (!this.#workCas(current, next)) return undefined;
		if (next.wakeReportId !== null && workAccepted(next)) this.#consumeWakeReport(next);
		if (acceptanceEstablished && next.noticeHash !== null)
			this.metaSet(`lane-notice:${next.sessionKey}`, JSON.stringify({ epoch: next.epoch, hash: next.noticeHash }));
		return next;
	}

	/**
	 * Streamed turn activity of an open attempt refreshes its lane's
	 * `last_activity_at`, fenced to the attempt's own binding. Never moves it back.
	 */
	workAttemptActivity(opRef: string, at: string): boolean {
		return this.withTransaction(() => {
			const current = this.workAttemptGet(opRef);
			if (!current || current.settledAt !== null) return false;
			this.#assertNotQuarantined("work", current.jobId);
			return (
				this.#database
					.query(
						"UPDATE sessions SET last_activity_at = ? WHERE origin_key = ? AND gjc_session_id = ? AND epoch = ? AND (last_activity_at IS NULL OR last_activity_at < ?)",
					)
					.run(at, current.sessionKey, current.sessionId, current.epoch, at).changes === 1
			);
		});
	}

	/** Atomically settle the attempt and admit its report (or refusal decision). */
	workAttemptSettle(
		opRef: string,
		expectedVersion: number,
		record: LaneJobRecord,
		patch: WorkAttemptSettlement,
		admission?: WorkAttemptAdmission,
	): WorkAttemptSettleResult | undefined {
		return this.withTransaction(() =>
			this.workAttemptSettleInTransaction(opRef, expectedVersion, record, patch, admission),
		);
	}

	workAttemptSettleInTransaction(
		opRef: string,
		expectedVersion: number,
		record: LaneJobRecord,
		patch: WorkAttemptSettlement,
		admission?: WorkAttemptAdmission,
	): WorkAttemptSettleResult | undefined {
		this.requireTransaction();
		const current = this.workAttemptGet(opRef);
		if (!current || current.version !== expectedVersion || current.settledAt !== null) return undefined;
		this.#assertNotQuarantined("work", current.jobId);
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
					const sessionExists = this.#database
						.query("SELECT 1 FROM sessions WHERE origin_key = ?")
						.get(draft.parent.originKey);
					const idExists = this.#database
						.query("SELECT 1 FROM inbound_messages WHERE message_id = ?")
						.get(draft.reportId);
					const refused = Boolean(admission.holdReason) || !sessionExists || Boolean(idExists);
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
					decision = this.#laneParentAvailable(report.parentName) ? "reported" : report.root ? "fallback" : "no_target";
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
		if (!this.#workCas(current, next)) return undefined;
		this.#workPutHistory(next, record);
		this.#workActivity(next, next.settledAt!);

		if (next.wakeReportId !== null && workAccepted(next)) this.#consumeWakeReport(next);
		if (next.decision === "reported" && admission?.kind === "persona") {
			const resolvedOriginRefJson = resolveLaneReportOrigin(next.parent, admission.row.originRefJson);
			workAssert(
				this.inboundEnqueueInTransaction({
					messageId: admission.row.messageId,
					originKey: admission.row.originKey,
					originRefJson: resolvedOriginRefJson,
					body: admission.row.body,
					receivedAt: next.settledAt!,
					source: "lane_report",
				}),
			);
		}
		if (next.decision === "reported" && admission?.kind === "lane") {
			const report = admission.report;
			const createdAt = next.settledAt!;
			this.#database
				.query(
					"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, 0, NULL, NULL, ?, ?)",
				)
				.run(
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
			this.deliveryCreateInTransaction({
				id: next.deliveryId,
				turnId: opRef,
				originKey: expectedOrigin!,
				payloadJson: JSON.stringify(fallbackPayload),
			});
		}
		if (next.decision === "wake_unaccepted") {
			const row = linkedReport!;
			const definitiveRefusal = draft.terminal?.reasonCode === "send_rejected";
			const isHeld = record.state === "awaiting_operator" || record.state === "stalled";
			let state: LaneReportState;
			let holdReason: string | null = null;
			let childDelivery: ChatMessagePayload | undefined;
			let clearClaim = false;
			if (!definitiveRefusal) {
				state = "held";
				holdReason = "wake_acceptance_uncertain";
			} else if (!isHeld) {
				state = "pending";
				clearClaim = true;
				requeuedParent = row.parent_name;
			} else if (row.root_json !== null) {
				state = "fallback";
				const root = laneReportRoot(row)!;
				const deliveryId = workAttemptDeliveryId(this.instanceId, laneJobDatabaseId(row.child_name), row.child_op_ref);
				childDelivery = buildDeliveryPayload(row.child_op_ref, root.origin, row.body, deliveryId)!;
			} else {
				state = "undeliverable";
			}
			const changed = this.#database
				.query(
					"UPDATE lane_reports SET state = ?, claim_kind = ?, claim_ref = ?, claim_target_op_ref = ?, hold_reason = ?, consumed_op_ref = NULL, updated_at = ? WHERE report_id = ? AND state = 'claimed' AND claim_kind = 'wake' AND claim_ref = ? AND claim_seq = ?",
				)
				.run(
					state,
					clearClaim ? null : row.claim_kind,
					clearClaim ? null : row.claim_ref,
					clearClaim ? null : row.claim_target_op_ref,
					holdReason,
					next.settledAt,
					row.report_id,
					opRef,
					row.claim_seq,
				).changes;
			workAssert(changed === 1);
			if (childDelivery) {
				this.deliveryCreateInTransaction({
					id: childDelivery.deliveryId!,
					turnId: childDelivery.turnId,
					originKey: originKey(childDelivery.origin),
					payloadJson: JSON.stringify(childDelivery),
				});
				childFallback = childDelivery;
			}
		}
		return {
			runtime: next,
			...(fallbackPayload ? { fallbackPayload } : {}),
			...(childFallback ? { childFallback } : {}),
			...(requeuedParent ? { requeuedParent } : {}),
		};
	}

	#laneParentAvailable(name: string): boolean {
		const jobId = laneJobDatabaseId(name);
		if (this.isBrokerQuarantined("work", jobId)) return false;
		const json = this.laneJobJsonByLaneKey(`work-${name}`);
		if (json === undefined) return false;
		try {
			return parseLaneJobRecord(json).jobId === jobId;
		} catch {
			return false;
		}
	}

	#consumeWakeReport(runtime: WorkAttemptRuntime): void {
		const reportId = runtime.wakeReportId;
		if (reportId === null) return;
		const row = this.#laneReportGetInside(reportId);
		workAssert(row !== undefined && row.claim_kind === "wake" && row.claim_ref === runtime.opRef);
		if (row.state === "consumed") {
			workAssert(row.consumed_op_ref === runtime.opRef);
			return;
		}
		workAssert(row.state === "claimed" || row.state === "held");
		const changed = this.#database
			.query(
				"UPDATE lane_reports SET state = 'consumed', consumed_op_ref = ?, hold_reason = NULL, updated_at = ? WHERE report_id = ? AND state = ? AND claim_kind = 'wake' AND claim_ref = ? AND claim_seq = ?",
			)
			.run(runtime.opRef, new Date().toISOString(), reportId, row.state, runtime.opRef, row.claim_seq).changes;
		workAssert(changed === 1);
	}

	#workPatched(current: WorkAttemptRuntime, patch: WorkAttemptPatch): WorkAttemptRuntime {
		const allowed = new Set(["sendPhase", "sendEvidence", "terminal", "output"]);
		workAssert(Object.keys(patch).every((key) => allowed.has(key)));
		const next = { ...current, ...patch, version: current.version + 1 };
		validateWorkRuntime(next, this.instanceId);
		workAssert(current.sendPhase !== "accepted" || next.sendPhase === "accepted");
		workAssert(current.sendPhase === "prepared" || next.sendPhase !== "prepared");
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

	#workCas(current: WorkAttemptRuntime, next: WorkAttemptRuntime): boolean {
		return (
			this.#database
				.query(
					"UPDATE work_attempt_runtime SET record_json = ?, version = ?, settled_at = ? WHERE op_ref = ? AND version = ? AND settled_at IS NULL",
				)
				.run(JSON.stringify(next), next.version, next.settledAt, current.opRef, current.version).changes === 1
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

	#workPutHistory(runtime: WorkAttemptRuntime, record: LaneJobRecord): void {
		this.putLaneJob({ ...record, laneKey: runtime.laneKey, json: JSON.stringify(record) });
	}

	#workActivity(runtime: WorkAttemptRuntime, at: string): void {
		this.#database
			.query(
				"UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ? AND gjc_session_id = ? AND epoch = ?",
			)
			.run(
				at,
				JSON.stringify({
					platform: "work",
					kind: "task",
					conversationId: runtime.sessionKey.slice("work/task/".length),
				}),
				runtime.sessionKey,
				runtime.sessionId,
				runtime.epoch,
			);
	}

	/** No transaction ownership and no INSERT OR IGNORE: conflicting obligations fail closed. */
	deliveryCreateInTransaction(row: { id: string; turnId: string; originKey: string; payloadJson: string }): boolean {
		this.requireTransaction();
		const existing = this.#database
			.query<{ turn_id: string; origin_key: string; payload_json: string }, [string]>(
				"SELECT turn_id, origin_key, payload_json FROM deliveries WHERE delivery_id = ?",
			)
			.get(row.id);
		if (existing) {
			workAssert(
				existing.turn_id === row.turnId &&
					existing.origin_key === row.originKey &&
					existing.payload_json === row.payloadJson,
			);
			return false;
		}
		return this.deliveryCreate(row);
	}

	private constructor(database: Database) {
		this.#database = database;
	}

	/**
	 * Peek at the recorded broker authority without full database initialization.
	 * Returns the canonicalAgentDir from broker_authority if it exists, or null if the
	 * database doesn't exist or the table/record is absent. Used during boot to resolve
	 * the agent directory before full database initialization.
	 */
	static peekRecordedAuthority(path: string): string | null {
		try {
			const database = new Database(path, { readonly: true });
			try {
				const stored = database
					.query<{ authority_key: string }, []>("SELECT authority_key FROM broker_authority WHERE singleton = 1")
					.get();
				if (!stored) return null;
				const value: unknown = JSON.parse(stored.authority_key);
				if (!Array.isArray(value) || value.length !== 2) return null;
				const canonicalAgentDir = value[0];
				return typeof canonicalAgentDir === "string" ? canonicalAgentDir : null;
			} finally {
				database.close();
			}
		} catch {
			// Database doesn't exist, table doesn't exist, or read failed; treat as no recorded authority
			return null;
		}
	}

	static async open(path: string, options: { readonly canonicalAgentDir?: string } = {}): Promise<GatewayDatabase> {
		await mkdir(dirname(path), { recursive: true, mode: 0o700 });
		const database = new Database(path);
		try {
			database.exec(
				"PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL; PRAGMA journal_size_limit = 67108864;",
			);
			// Create schema_migrations and broker_authority tables early so they can be used before full migration.
			database.exec(
				"CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); CREATE TABLE IF NOT EXISTS broker_authority (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), authority_key TEXT NOT NULL);",
			);
			const instance = new GatewayDatabase(database);
			// Check broker authority before running other migrations (same transaction for atomicity).
			if (options.canonicalAgentDir !== undefined) {
				const authority = {
					canonicalAgentDir: options.canonicalAgentDir,
					identity: `gjc:${options.canonicalAgentDir}`,
				};
				// This check runs before migrate(), so authority_mismatch leaves schema_migrations unchanged.
				instance.assertBrokerAuthority(authority, { initializeEmpty: true });
			}
			instance.migrate();
			instance.integrityCheck();
			return instance;
		} catch (error) {
			database.close();
			throw error;
		}
	}

	get schemaVersion(): number {
		const row = this.#database
			.query<{ version: number }, []>("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
			.get();
		return row?.version ?? 0;
	}

	get activeSessionCount(): number {
		return this.#database.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM sessions").get()?.count ?? 0;
	}
	/**
	 * Returns the session row even when no gjc session is bound yet (empty
	 * sessionId): the epoch must stay visible after /new bumps it, or dispatch
	 * silently falls back to epoch 0 and the old transcript.
	 */
	getSessionRecord(originKey: string): { sessionId: string; epoch: number } | undefined {
		const row = this.#database
			.query<{ gjc_session_id: string; epoch: number }, [string]>(
				"SELECT gjc_session_id, epoch FROM sessions WHERE origin_key = ?",
			)
			.get(originKey);
		return row ? { sessionId: row.gjc_session_id, epoch: row.epoch } : undefined;
	}

	/**
	 * Get the origin_ref_json for a session by originKey.
	 * Returns the parsed origin or undefined if not found.
	 */
	getOriginByKey(originKey: string): Record<string, unknown> | undefined {
		const row = this.#database
			.query<{ origin_ref_json: string | null }, [string]>("SELECT origin_ref_json FROM sessions WHERE origin_key = ?")
			.get(originKey);
		if (!row || !row.origin_ref_json) return undefined;
		try {
			return JSON.parse(row.origin_ref_json) as Record<string, unknown>;
		} catch {
			return undefined;
		}
	}

	getSessionBootstrap(originKey: string):
		| {
				epoch: number;
				lastBootstrappedEpoch: number;
				agentsMdEpoch: number;
				agentsMdDigest: string | null;
				appliedAt: string | null;
				includedSections: readonly string[];
				byteCount: number;
				truncated: boolean;
				diagnostics: readonly string[];
		  }
		| undefined {
		const row = this.#database
			.query<
				{
					epoch: number;
					last_bootstrapped_epoch: number;
					agents_md_epoch: number;
					agents_md_digest: string | null;
					bootstrap_applied_at: string | null;
					bootstrap_sections_json: string;
					bootstrap_byte_count: number;
					bootstrap_truncated: number;
					bootstrap_diagnostics_json: string;
				},
				[string]
			>(
				"SELECT epoch, last_bootstrapped_epoch, agents_md_epoch, agents_md_digest, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions WHERE origin_key = ?",
			)
			.get(originKey);
		if (!row) return undefined;
		return {
			epoch: row.epoch,
			lastBootstrappedEpoch: row.last_bootstrapped_epoch,
			agentsMdEpoch: row.agents_md_epoch,
			agentsMdDigest: row.agents_md_digest,
			appliedAt: row.bootstrap_applied_at,
			includedSections: parseStringList(row.bootstrap_sections_json),
			byteCount: row.bootstrap_byte_count,
			truncated: row.bootstrap_truncated === 1,
			diagnostics: parseStringList(row.bootstrap_diagnostics_json),
		};
	}

	/** Records the current AGENTS.md digest for a session epoch when it changes. */
	recordSessionAgentsBaseline(originKey: string, epoch: number, digest: string): boolean {
		return (
			this.#database
				.query(
					"UPDATE sessions SET agents_md_epoch = ?, agents_md_digest = ? WHERE origin_key = ? AND epoch = ? AND (agents_md_epoch < ? OR (agents_md_epoch = ? AND agents_md_digest IS NOT ?))",
				)
				.run(epoch, digest, originKey, epoch, epoch, epoch, digest).changes === 1
		);
	}

	markSessionBootstrapped(
		originKey: string,
		epoch: number,
		projection: {
			readonly includedSections: readonly string[];
			readonly byteCount: number;
			readonly truncated: boolean;
			readonly diagnostics: readonly string[];
		},
	): boolean {
		const now = new Date().toISOString();
		return (
			this.#database
				.query(
					"UPDATE sessions SET last_bootstrapped_epoch = ?, bootstrap_applied_at = ?, bootstrap_sections_json = ?, bootstrap_byte_count = ?, bootstrap_truncated = ?, bootstrap_diagnostics_json = ? WHERE origin_key = ? AND epoch = ? AND last_bootstrapped_epoch < ?",
				)
				.run(
					epoch,
					now,
					JSON.stringify(projection.includedSections),
					projection.byteCount,
					projection.truncated ? 1 : 0,
					JSON.stringify(projection.diagnostics),
					originKey,
					epoch,
					epoch,
				).changes === 1
		);
	}

	bumpEpoch(originKey: string, originRefJson: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at) VALUES (?, ?, '', 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, origin_ref_json = excluded.origin_ref_json, last_activity_at = excluded.last_activity_at",
			)
			.run(originKey, originRefJson, now, now);
		return this.#database
			.query<{ epoch: number }, [string]>("SELECT epoch FROM sessions WHERE origin_key = ?")
			.get(originKey)!.epoch;
	}

	/**
	 * Rebind reset for a poisoned gjc session key (#13): the same semantics as
	 * the `/new` reset path — epoch + 1, turn_count back to 0, and the gjc
	 * binding cleared, so the next session.create op derives a fresh idempotency
	 * key and a recovery near the rotation boundary cannot instantly discard its
	 * fresh binding on an inherited count — but the stored origin ref is kept,
	 * because a rebind is a runtime recovery rather than a user command and
	 * carries no origin payload of its own.
	 */
	rebindEpoch(originKey: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, '', 1, 0, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, last_activity_at = excluded.last_activity_at",
			)
			.run(originKey, now, now);
		const row = this.#database
			.query<{ epoch: number }, [string]>("SELECT epoch FROM sessions WHERE origin_key = ?")
			.get(originKey);
		if (!row) throw new Error(`session row for ${originKey} disappeared during rebind`);
		return row.epoch;
	}

	updateActivity(originKey: string, originRefJson: string): void {
		this.#database
			.query("UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ?")
			.run(new Date().toISOString(), originRefJson, originKey);
	}

	/**
	 * Counts completed monitor-authoring turns within the current epoch for
	 * observability and safety diagnostics. Persistent SDK sessions rely on native
	 * compaction; this count never rotates a persona or worker epoch.
	 *
	 * The write is an upsert because monitor authoring origins (issue #68) reach
	 * this path before any chat turn ever bound their session row. When
	 * `originRefJson` is given the row is created with it, so the seeded row is
	 * projectable (admin/cycle) instead of an origin-less stub.
	 */
	incrementTurnCount(originKey: string, originRefJson?: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, ?, '', 0, 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET turn_count = turn_count + 1",
			)
			.run(originKey, originRefJson ?? null, now, now);
		return (
			this.#database
				.query<{ turn_count: number }, [string]>("SELECT turn_count FROM sessions WHERE origin_key = ?")
				.get(originKey)?.turn_count ?? 0
		);
	}

	/**
	 * True when this origin has ever driven a turn as the trigger. `turn_count` is
	 * NOT usable for this: only monitor authoring increments it, so a conversation
	 * that answered ten chat turns still reads zero (verified against a live
	 * database, 2026-09-17). The trigger role on an inbound row is the durable
	 * record of "the persona was asked here and bound a turn to it".
	 */
	originTriggeredTurn(originKey: string): boolean {
		return (
			this.#database
				.query<{ n: number }, [string]>(
					"SELECT 1 AS n FROM inbound_messages WHERE origin_key = ? AND turn_role = 'trigger' LIMIT 1",
				)
				.get(originKey) !== null
		);
	}

	/**
	 * True when one specific message was taken into a turn in that origin, either
	 * as its trigger or as a steer folded into a turn already running. A mention
	 * that lands while the persona is busy becomes a steer, and it is just as
	 * engaged as a trigger.
	 */
	messageJoinedTurn(originKey: string, messageId: string): boolean {
		return (
			this.#database
				.query<{ n: number }, [string, string]>(
					"SELECT 1 AS n FROM inbound_messages WHERE origin_key = ? AND message_id = ? AND turn_role IN ('trigger', 'steer') LIMIT 1",
				)
				.get(originKey, messageId) !== null
		);
	}

	/** Completed turns in the current epoch, without mutating the counter. */
	sessionTurnCount(originKey: string): number {
		return (
			this.#database
				.query<{ turn_count: number }, [string]>("SELECT turn_count FROM sessions WHERE origin_key = ?")
				.get(originKey)?.turn_count ?? 0
		);
	}

	sessionRows(): Array<{
		origin_ref_json: string | null;
		created_at: string;
		last_activity_at: string | null;
		epoch: number;
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#database
			.query(
				"SELECT origin_ref_json, created_at, last_activity_at, epoch, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_ref_json: string | null;
			created_at: string;
			last_activity_at: string | null;
			epoch: number;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
		}>;
	}
	/**
	 * Cycle projection source (ops.cycle): full session identity including the bound
	 * gjc session id. An empty gjc_session_id with a positive epoch is the mid-rebind
	 * state after /new; the projection must report it as stale identity, never healthy.
	 */
	sessionIdentityRows(): Array<{
		origin_key: string;
		origin_ref_json: string | null;
		gjc_session_id: string;
		epoch: number;
		created_at: string;
		last_activity_at: string | null;
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#database
			.query(
				"SELECT origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_key: string;
			origin_ref_json: string | null;
			gjc_session_id: string;
			epoch: number;
			created_at: string;
			last_activity_at: string | null;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
		}>;
	}

	/** Cycle projection source: durable inbound queue state census across all origins. */
	inboundStateCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM inbound_messages GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/**
	 * Cycle projection source: pending inbound per origin key, with the age of
	 * the oldest UNBOUND row and whether a turn is in flight there. Actors keep
	 * an active trigger at `state = 'pending'` and move only `turn_state`, so
	 * "in flight" is `turn_state IN ('bound','accepted')`, never the legacy
	 * `processing` state (normalised away by migration 19).
	 */
	inboundPendingByOrigin(): Array<{
		origin_key: string;
		n: number;
		oldest_unbound_received_at: string | null;
		active: number;
	}> {
		return this.#database
			.query(
				`SELECT origin_key, COUNT(*) AS n,
					MIN(CASE WHEN turn_state IS NULL THEN received_at END) AS oldest_unbound_received_at,
					SUM(CASE WHEN turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') THEN 1 ELSE 0 END) AS active
				FROM inbound_messages WHERE state = 'pending' AND ${REPLAYABLE_INBOUND} GROUP BY origin_key`,
			)
			.all() as Array<{ origin_key: string; n: number; oldest_unbound_received_at: string | null; active: number }>;
	}

	/** Cycle projection source: delivery state census across all origins. */
	deliveryStateCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/** Cycle projection source: unsettled delivery age census per origin key. */
	deliveryUnsettledByOrigin(now = Date.now()): Array<{ origin_key: string; n: number; oldest_ms: number }> {
		return this.#database
			.query(
				// Age in ms from a seconds binding: parenthesize so the seconds delta is
				// multiplied, not the timestamp alone (SQL precedence binds * before -).
				"SELECT origin_key, COUNT(*) AS n, MAX((? - CAST(strftime('%s', created_at) AS INTEGER)) * 1000) AS oldest_ms FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') GROUP BY origin_key",
			)
			.all(Math.floor(now / 1000)) as Array<{ origin_key: string; n: number; oldest_ms: number }>;
	}

	/** Cycle projection source: memory-intent settlement census. */
	memoryIntentCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM memory_intents GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/** Cycle projection source: monitor-event stage census. */
	monitorEventStageCounts(): Array<{ stage: string; n: number }> {
		return this.#database.query("SELECT stage, COUNT(*) AS n FROM monitor_events GROUP BY stage").all() as Array<{
			stage: string;
			n: number;
		}>;
	}

	/**
	 * Cycle projection source: per event type, the run of most recent terminal
	 * events (fired at or after `sinceIso`) that exhausted retries with no stored
	 * authored output. Any other terminal outcome ends the run; in-flight events
	 * are not evidence either way. Types whose latest terminal event is not such a
	 * loss are omitted.
	 */
	monitorAuthoringLossStreaks(
		sinceIso: string,
	): Array<{ eventType: string; consecutive: number; lastFiredAt: string }> {
		const rows = this.#database
			.query<{ event_type: string; fired_at: string; lost: number }, [string]>(
				`SELECT event_type, fired_at, (stage = 'failed_no_retry' AND NOT EXISTS (SELECT 1 FROM authored_outputs a WHERE a.event_id = monitor_events.event_id)) AS lost FROM monitor_events WHERE stage IN ('delivered','authored_no_delivery','failed_no_retry') AND fired_at >= ? AND ${REPLAYABLE_MONITOR} ORDER BY fired_at DESC, rowid DESC`,
			)
			.all(sinceIso);
		const streaks = new Map<string, { consecutive: number; lastFiredAt: string; open: boolean }>();
		for (const row of rows) {
			const streak = streaks.get(row.event_type);
			if (!streak) {
				streaks.set(row.event_type, { consecutive: row.lost ? 1 : 0, lastFiredAt: row.fired_at, open: !!row.lost });
			} else if (streak.open) {
				if (row.lost) streak.consecutive++;
				else streak.open = false;
			}
		}
		return [...streaks]
			.filter(([, streak]) => streak.consecutive > 0)
			.map(([eventType, { consecutive, lastFiredAt }]) => ({ eventType, consecutive, lastFiredAt }))
			.sort((a, b) => a.eventType.localeCompare(b.eventType));
	}

	/**
	 * Cycle projection source: how many of the most recently settled monitor
	 * events, newest first, ended `failed_no_retry` before the first success.
	 * Dispatch outcome, not process liveness, is what a monitor outage looks like.
	 */
	monitorConsecutiveTerminalFailures(limit = 100): number {
		const rows = this.#database
			.query<{ stage: string }, [number]>(
				`SELECT stage FROM monitor_events WHERE stage IN ('delivered','authored_no_delivery','failed_no_retry') AND ${REPLAYABLE_MONITOR} ORDER BY updated_at DESC, rowid DESC LIMIT ?`,
			)
			.all(limit);
		const streak = rows.findIndex((row) => row.stage !== "failed_no_retry");
		return streak < 0 ? rows.length : streak;
	}

	addRecall(originKey: string, originRefJson: string, text: string): void {
		this.#database
			.query("INSERT INTO recall_snippets (origin_key, origin_ref_json, text, at) VALUES (?, ?, ?, ?)")
			.run(originKey, originRefJson, text.slice(0, 1000), new Date().toISOString());
		this.#database
			.query(
				"DELETE FROM recall_snippets WHERE origin_key = ? AND id NOT IN (SELECT id FROM recall_snippets WHERE origin_key = ? ORDER BY id DESC LIMIT 20)",
			)
			.run(originKey, originKey);
	}

	recallRows(): Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }> {
		return this.#database
			.query("SELECT origin_key, origin_ref_json, text, at FROM recall_snippets ORDER BY id DESC")
			.all() as Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }>;
	}

	memoryIntentCreate(row: { id: string; kind: string; payloadJson: string }): void {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO memory_intents (id, kind, payload_json, state, created_at, updated_at) VALUES (?, ?, ?, 'queued', ?, ?)",
			)
			.run(row.id, row.kind, row.payloadJson, now, now);
	}

	/** Insertion-order view (rowid) — preserves admission order within the same millisecond. */
	memoryIntentRowsByRowid(): Array<{
		id: string;
		kind: string;
		payload_json: string;
		state: string;
		attempts: number;
		quarantine_reason: string | null;
	}> {
		return this.#database
			.query("SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents ORDER BY rowid")
			.all() as Array<{
			id: string;
			kind: string;
			payload_json: string;
			state: string;
			attempts: number;
			quarantine_reason: string | null;
		}>;
	}

	memoryIntentBeginAttempt(id: string): void {
		this.#database
			.query("UPDATE memory_intents SET attempts = attempts + 1, updated_at = ? WHERE id = ?")
			.run(new Date().toISOString(), id);
	}

	memoryIntentQuarantine(id: string, reason: string): void {
		if (!reason.trim()) throw new Error("memory intent quarantine reason is required");
		this.#database
			.query(
				"UPDATE memory_intents SET state = 'quarantined', quarantine_reason = COALESCE(quarantine_reason, ?), updated_at = ? WHERE id = ?",
			)
			.run(reason, new Date().toISOString(), id);
	}

	memoryIntentUpdate(id: string, state: "queued" | "written" | "committed" | "receipted" | "quarantined"): void {
		this.#database
			.query("UPDATE memory_intents SET state = ?, updated_at = ? WHERE id = ?")
			.run(state, new Date().toISOString(), id);
	}

	memoryIntentRows(): Array<{
		id: string;
		kind: string;
		payload_json: string;
		state: "queued" | "written" | "committed" | "receipted" | "quarantined";
		attempts: number;
		quarantine_reason: string | null;
	}> {
		return this.#database
			.query(
				"SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents ORDER BY created_at, id",
			)
			.all() as Array<{
			id: string;
			kind: string;
			payload_json: string;
			state: "queued" | "written" | "committed" | "receipted" | "quarantined";
			attempts: number;
			quarantine_reason: string | null;
		}>;
	}

	/** Every row not yet receipted, in admission order; boot recovery never needs the receipted history. */
	memoryIntentOpenRows(): MemoryIntentDbRow[] {
		return this.#database
			.query<MemoryIntentDbRow, []>(
				"SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents WHERE state <> 'receipted' ORDER BY created_at, id",
			)
			.all();
	}

	memoryIntentGet(id: string): MemoryIntentDbRow | undefined {
		return (
			this.#database
				.query<MemoryIntentDbRow, [string]>(
					"SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents WHERE id = ?",
				)
				.get(id) ?? undefined
		);
	}

	/**
	 * Whether a monitor-event memory intent already covers this event. The
	 * deterministic id is a primary-key probe; the payload scan only runs for
	 * legacy intents written before ids were deterministic.
	 */
	memoryIntentCoversEvent(eventId: string): boolean {
		if (this.memoryIntentGet(`monitor-event-intent:${eventId}`)) return true;
		return (
			this.#database
				.query<{ one: number }, [string]>(
					"SELECT 1 AS one FROM memory_intents WHERE kind = 'monitor-event' AND instr(payload_json, ?) > 0 LIMIT 1",
				)
				.get(eventId) !== null
		);
	}

	/** The insert is the acceptance boundary: dispatch may only start once the row is durable. */
	inboundEnqueue(row: {
		messageId: string;
		originKey: string;
		originRefJson: string;
		body: string;
		engagementJson?: string | null;
		receivedAt?: string;
		source?: "platform" | "lane_report";
	}): boolean {
		const enqueue = () => this.inboundEnqueueInTransaction(row);
		return this.#inTransaction ? enqueue() : this.withTransaction(enqueue);
	}

	inboundEnqueueInTransaction(row: {
		messageId: string;
		originKey: string;
		originRefJson: string;
		body: string;
		engagementJson?: string | null;
		receivedAt?: string;
		source?: "platform" | "lane_report";
	}): boolean {
		this.requireTransaction();
		const receivedAt = row.receivedAt ?? new Date().toISOString();
		if (!Number.isFinite(Date.parse(receivedAt))) throw new Error("inbound receivedAt must be an ISO timestamp");
		const source = row.source ?? "platform";
		if (source === "lane_report" && row.engagementJson !== undefined && row.engagementJson !== null)
			throw new Error("lane report inbound rows cannot carry engagement metadata");
		const changes = this.#database
			.query(
				"INSERT INTO inbound_messages (message_id, source, origin_key, origin_ref_json, body, engagement_json, state, received_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(row.messageId, source, row.originKey, row.originRefJson, row.body, row.engagementJson ?? null, receivedAt);
		return changes.changes > 0;
	}

	inboundColumns(): string {
		return "message_id, source, origin_key, origin_ref_json, body, engagement_json, state, received_at, turn_role, turn_epoch, turn_state, turn_op_ref, bound_session_id, dispatched_at, terminal_delivery_id";
	}

	/**
	 * Oldest pending row not yet part of any turn: the next trigger or steer.
	 * Ties on `received_at` (a burst inside one millisecond) break on insertion
	 * order, never on the platform message id, so fragments are steered in the
	 * order they arrived.
	 */
	inboundPendingOldest(originKey: string): InboundMessageRow | undefined {
		return (
			this.#database
				.query<InboundMessageRow, [string]>(
					`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND turn_state IS NULL AND ${REPLAYABLE_INBOUND} ORDER BY received_at, rowid LIMIT 1`,
				)
				.get(originKey) ?? undefined
		);
	}

	/**
	 * Binds the oldest pending row as the trigger of a new turn: session chosen,
	 * op-ref fixed, `dispatched_at` stamped - all BEFORE the send, so the stamp
	 * is provably ahead of any answer. The partial unique index rejects a second
	 * nonterminal trigger in the same epoch.
	 */
	inboundBindTurn(input: {
		messageId: string;
		originKey: string;
		epoch: number;
		opRef: string;
		sessionId: string;
		dispatchedAt?: string;
	}): InboundMessageRow {
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("turn epoch must be a non-negative integer");
		if (!input.sessionId) throw new Error("turn session id must not be empty");
		return this.withTransaction(() => {
			this.#assertNotQuarantined("inbound", input.messageId);
			this.#assertReplayableTurn(input.opRef);
			const existing = this.#database
				.query<{ n: number }, [string, number]>(
					"SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
				)
				.get(input.originKey, input.epoch)?.n;
			if ((existing ?? 0) > 0) throw new InboundTurnConflictError(input.originKey, input.epoch);
			const changes = this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = 'trigger', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ?, bound_session_id = ?, dispatched_at = ?, terminal_delivery_id = NULL WHERE message_id = ? AND origin_key = ? AND state = 'pending' AND turn_state IS NULL",
				)
				.run(
					input.epoch,
					input.opRef,
					input.sessionId,
					input.dispatchedAt ?? new Date().toISOString(),
					input.messageId,
					input.originKey,
				).changes;
			if (changes !== 1) throw new Error(`inbound ${input.messageId} is not a pending unbound row`);
			return this.inboundTurnRow(input.opRef) as InboundMessageRow;
		});
	}

	/**
	 * Records the accepted receipt boundary: the runtime now holds the
	 * operation. A member still riding with the trigger (v18 settled+bound
	 * batch) was in that prompt, so it is done input from here on.
	 */
	inboundTurnAccept(opRef: string): boolean {
		return this.withTransaction(() => {
			const accepted =
				this.#database
					.query(
						"UPDATE inbound_messages SET turn_state = 'accepted' WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state = 'bound'",
					)
					.run(opRef).changes > 0;
			if (accepted)
				this.#database
					.query(
						"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound'",
					)
					.run(opRef);
			return accepted;
		});
	}

	/** Durably blocks an explicitly refused prompt without pretending it was accepted. */
	inboundTurnBlock(opRef: string, reason: "model_not_selected"): boolean {
		return this.withTransaction(() => {
			const trigger = this.inboundTurnRow(opRef);
			if (!trigger || trigger.state !== "pending" || trigger.turn_state !== "bound") return false;
			this.#assertNotQuarantined("inbound", trigger.message_id);
			this.metaSet(inboundTurnBlockMetaKey(opRef), reason);
			return true;
		});
	}

	/** Whether an op-ref has a durable explicit-refusal block. */
	inboundTurnIsBlocked(opRef: string): boolean {
		return this.metaGet(inboundTurnBlockMetaKey(opRef)) !== undefined;
	}

	/** The turn's pre-send dispatch stamp. */
	inboundTurnDispatchedAt(opRef: string): string | undefined {
		return (
			this.#database
				.query<{ dispatched_at: string | null }, [string]>(
					"SELECT dispatched_at FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'",
				)
				.get(opRef)?.dispatched_at ?? undefined
		);
	}

	/**
	 * Claims one terminal reply slot (`part`) of a turn for `deliveryId`. The
	 * trigger row stores the claims as a JSON map of part -> delivery id.
	 * Returns the id that owns the slot after the call: the claimant's when the
	 * slot was free, otherwise the earlier winner's. Durable, so a rebuilt
	 * lifecycle after a restart sees the claim.
	 */
	inboundTurnClaimTerminal(opRef: string, part: number, deliveryId: string): string {
		return this.withTransaction(() => {
			const row = this.#database
				.query<{ terminal_delivery_id: string | null }, [string]>(
					"SELECT terminal_delivery_id FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'",
				)
				.get(opRef);
			let claims: Record<string, string> = {};
			if (row?.terminal_delivery_id) {
				try {
					const parsed: unknown = JSON.parse(row.terminal_delivery_id);
					if (typeof parsed === "object" && parsed !== null) claims = parsed as Record<string, string>;
				} catch {
					claims = {};
				}
			}
			const key = String(part);
			const owner = claims[key];
			if (owner) return owner;
			delete claims.none;
			claims[key] = deliveryId;
			this.#database
				.query("UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger'")
				.run(JSON.stringify(claims), opRef);
			return deliveryId;
		});
	}

	/**
	 * Records that the turn closed without a delivery, and why. Never overwrites
	 * a delivery claim or an earlier reason.
	 */
	inboundTurnMarkUnlinked(opRef: string, reason: TerminalUnlinkedReason): void {
		this.#database
			.query(
				"UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger' AND terminal_delivery_id IS NULL",
			)
			.run(JSON.stringify({ none: reason }), opRef);
	}

	/**
	 * Done triggers dispatched at or after `sinceIso`, and how many of them
	 * carry neither a delivery claim nor a no-delivery reason.
	 */
	inboundTerminalLinkAudit(sinceIso: string): { done: number; unlinked: number } {
		const row = this.#database
			.query<{ done: number; unlinked: number | null }, [string]>(
				"SELECT COUNT(*) AS done, SUM(terminal_delivery_id IS NULL) AS unlinked FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done' AND dispatched_at >= ?",
			)
			.get(sinceIso);
		return { done: row?.done ?? 0, unlinked: row?.unlinked ?? 0 };
	}

	/**
	 * Terminal reconciliation: legacy state and turn state become done in one
	 * statement for the trigger. A trigger no delivery claimed is stamped with
	 * `unlinkedReason` in the same statement, so `done` never leaves the link NULL. A steer still `bound` at this point was issued
	 * into the turn but never answered (torn transport): whether the model saw
	 * it is unknowable now that the turn is over, so it is NOT closed and NOT
	 * dispatched - it stays attributed to this op-ref as an operator-visible
	 * hold (`inboundSteersHeld`). Returns how many TRIGGER rows closed, so a
	 * repeat call is a no-op.
	 */
	inboundTurnComplete(opRef: string, unlinkedReason: TerminalUnlinkedReason = "no_delivery"): number {
		return this.#database
			.query(
				"UPDATE inbound_messages SET state = 'done', turn_state = 'done', terminal_delivery_id = COALESCE(terminal_delivery_id, ?) WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state IN ('bound', 'accepted')",
			)
			.run(JSON.stringify({ none: unlinkedReason }), opRef).changes;
	}

	/**
	 * Releases a turn whose send provably never landed, or whose op failed, for
	 * one fresh turn: the trigger goes back to plain pending. The retry ordinal
	 * is durable so a crash cannot recreate the same op-ref.
	 */
	inboundTurnRequeue(opRef: string): number {
		return this.withTransaction(() => {
			const trigger = this.inboundTurnRow(opRef);
			if (!trigger || trigger.turn_epoch === null) throw new Error(`turn ${opRef} has no retryable trigger`);
			this.#assertNotQuarantined("inbound", trigger.message_id);
			if (trigger.state !== "pending" || !["bound", "accepted"].includes(trigger.turn_state ?? ""))
				throw new Error(`turn ${opRef} cannot be requeued from its current lifecycle state`);
			const key = freshTurnMetaKey(trigger.origin_key, trigger.turn_epoch, trigger.message_id);
			const prior = Number.parseInt(this.metaGet(key) ?? "0", 10);
			const attempt = Number.isSafeInteger(prior) && prior >= 0 ? prior + 1 : 1;
			this.metaSet(key, String(attempt));
			// The trigger and any member still riding with it (a v18 settled+bound
			// batch whose send was proven absent) go back to plain pending together.
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL, bound_session_id = NULL, dispatched_at = NULL, terminal_delivery_id = NULL WHERE turn_op_ref = ? AND state = 'pending' AND (turn_role = 'trigger' OR (turn_role = 'steer' AND turn_state = 'bound'))",
				)
				.run(opRef);
			this.metaDelete(inboundTurnBlockMetaKey(opRef));
			return attempt;
		});
	}

	/** Complete only this failed trigger and reset the next binding, atomically. Never replay input. */
	inboundFailedTurnReset(input: {
		originKey: string;
		epoch: number;
		sessionId: string;
		opRef: string;
		triggerMessageId: string;
	}): number | undefined {
		return this.withTransaction(() => {
			const trigger = this.inboundTurnRow(input.opRef);
			const session = this.getSessionRecord(input.originKey);
			const dedupeKey = `failed-turn-reset:${createHash("sha256")
				.update(JSON.stringify([input.originKey, input.triggerMessageId]))
				.digest("hex")}`;
			const capKey = failedTurnResetCapKey(input.originKey);
			const cap = this.metaGet(capKey);
			if (
				!trigger ||
				trigger.origin_key !== input.originKey ||
				trigger.message_id !== input.triggerMessageId ||
				trigger.turn_epoch !== input.epoch ||
				trigger.bound_session_id !== input.sessionId ||
				trigger.state !== "pending" ||
				trigger.turn_state !== "accepted" ||
				session?.epoch !== input.epoch ||
				session.sessionId !== input.sessionId ||
				this.metaGet(dedupeKey) !== undefined ||
				(cap !== undefined && cap !== "0")
			)
				return undefined;
			if (this.inboundTurnComplete(input.opRef, "turn_failed") !== 1)
				throw new Error("failed turn completion lost its fence");
			this.metaSet(dedupeKey, "1");
			this.metaSet(capKey, "1");
			// Do not present the already-failed trigger as new unread work when the
			// next message bootstraps its session. Keep unrelated pending context intact.
			this.#database
				.query("UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND message_id = ?")
				.run(new Date().toISOString(), input.originKey, input.triggerMessageId);
			// Steers remain attributed to their original op, including uncertain
			// held rows. Unrelated pending messages and context floors are untouched.
			return this.rebindEpoch(input.originKey);
		});
	}

	/** Caller owns the healthy-completion or explicit-/new transaction. */
	clearFailedTurnResetCap(originKey: string): void {
		this.requireTransaction();
		this.metaSet(failedTurnResetCapKey(originKey), "0");
	}

	failedTurnResetCapped(originKey: string): boolean {
		const cap = this.metaGet(failedTurnResetCapKey(originKey));
		return cap !== undefined && cap !== "0";
	}

	freshTurnAttempt(originKey: string, epoch: number, triggerMessageId: string): number {
		const value = Number.parseInt(this.metaGet(freshTurnMetaKey(originKey, epoch, triggerMessageId)) ?? "0", 10);
		return Number.isSafeInteger(value) && value >= 0 ? value : 0;
	}

	/**
	 * Marks a pending row as a steer ISSUED into the running turn, before the
	 * transport answers. It is attributed to the turn from this moment: no
	 * dispatch path can take it as a trigger, and a restart finds it here.
	 */
	inboundSteerIssued(input: { messageId: string; epoch: number; opRef: string }): boolean {
		this.#assertNotQuarantined("inbound", input.messageId);
		this.#assertReplayableTurn(input.opRef);
		return (
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = 'steer', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
				)
				.run(input.epoch, input.opRef, input.messageId, input.opRef).changes === 1
		);
	}

	/**
	 * The runtime recorded the steer: the row is done input of the turn, and
	 * - in the SAME transaction - the platform message it carries is consumed
	 * from the unread context window. A crash between the two would otherwise
	 * leave a done steer whose message still reads as unread for the next
	 * turn, with no pending/held state left to drive a retry.
	 */
	inboundSteerAccepted(input: { messageId: string; epoch: number; opRef: string; contextMessageId?: string }): boolean {
		return this.withTransaction(() => {
			const accepted =
				this.#database
					.query(
						"UPDATE inbound_messages SET state = 'done', turn_role = 'steer', turn_epoch = ?, turn_state = 'done', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
					)
					.run(input.epoch, input.opRef, input.messageId, input.opRef).changes === 1;
			if (accepted && input.contextMessageId)
				this.#database
					.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ? AND consumed_at IS NULL")
					.run(new Date().toISOString(), input.contextMessageId);
			return accepted;
		});
	}

	/** The runtime definitively refused the steer: the row is an ordinary pending message again. */
	inboundSteerRefused(messageId: string, opRef: string): boolean {
		return (
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL WHERE message_id = ? AND state = 'pending' AND turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?",
				)
				.run(messageId, opRef).changes === 1
		);
	}

	/**
	 * A held steer whose turn is over and whose session is gone: whether the model
	 * saw it is unknowable and nothing will ever answer the clientRef replay. It is
	 * closed as done input of that turn - never re-dispatched, which could deliver
	 * a message the model already answered a second time - and, in the SAME
	 * transaction, its platform message leaves the unread context window, exactly
	 * like an accepted steer, so the next turn does not present it as unread.
	 */
	inboundSteerAbandoned(messageId: string, opRef: string, contextMessageId?: string): boolean {
		return this.withTransaction(() => {
			const closed =
				this.#database
					.query(
						"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE message_id = ? AND state = 'pending' AND turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?",
					)
					.run(messageId, opRef).changes === 1;
			if (closed && contextMessageId)
				this.#database
					.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ? AND consumed_at IS NULL")
					.run(new Date().toISOString(), contextMessageId);
			return closed;
		});
	}

	/** Held steers of this origin whose turn is already terminal: unresolvable by the turn, only by a clientRef replay. */
	inboundSteersHeldAfterTerminal(originKey: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE origin_key = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND ${REPLAYABLE_INBOUND} AND turn_op_ref IN (SELECT turn_op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done') ORDER BY received_at, rowid`,
			)
			.all(originKey);
	}

	/** Steers issued into `opRef` whose outcome is still unknown, oldest first. */
	inboundSteersHeld(opRef: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND ${REPLAYABLE_INBOUND} ORDER BY received_at, rowid`,
			)
			.all(opRef);
	}

	/** Historical inspection, including quarantined triggers; not an execution authorization. */
	inboundTurnRow(opRef: string): InboundMessageRow | undefined {
		return (
			this.#database
				.query<InboundMessageRow, [string]>(
					`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'`,
				)
				.get(opRef) ?? undefined
		);
	}

	/** Historical inspection, including quarantined steers; not an execution authorization. */
	inboundTurnRows(opRef: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? ORDER BY received_at, rowid`,
			)
			.all(opRef);
	}

	/** Current and retired nonterminal trigger rows are the recovery subjects. */
	inboundNonterminalTurns(originKey: string, epoch?: number): readonly InboundTurn[] {
		type Row = {
			origin_key: string;
			turn_epoch: number;
			turn_state: Extract<InboundTurnState, "bound" | "accepted">;
			turn_op_ref: string;
			bound_session_id: string | null;
			message_id: string;
		};
		const select =
			"SELECT origin_key, turn_epoch, turn_state, turn_op_ref, bound_session_id, message_id FROM inbound_messages";
		const rows =
			epoch === undefined
				? this.#database
						.query<Row, [string]>(
							`${select} WHERE origin_key = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY turn_epoch, received_at, message_id`,
						)
						.all(originKey)
				: this.#database
						.query<Row, [string, number]>(
							`${select} WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY received_at, message_id`,
						)
						.all(originKey, epoch);
		return rows.map((row) => ({
			originKey: row.origin_key,
			epoch: row.turn_epoch,
			state: row.turn_state,
			opRef: row.turn_op_ref,
			sessionId: row.bound_session_id,
			triggerMessageId: row.message_id,
		}));
	}

	/** Origins with pending rows not yet in a turn: after a restart these have no actor, so recovery must admit them. */
	inboundPendingOrigins(): readonly string[] {
		return this.#database
			.query<{ origin_key: string }, []>(
				`SELECT DISTINCT origin_key FROM inbound_messages WHERE state = 'pending' AND turn_state IS NULL AND ${REPLAYABLE_INBOUND}`,
			)
			.all()
			.map((row) => row.origin_key);
	}
	/** A nonterminal trigger hidden from execution because its inbound row was quarantined. */
	inboundHasQuarantinedNonterminalTurn(originKey: string): boolean {
		return (
			this.#database
				.query<{ n: number }, [string]>(
					"SELECT 1 AS n FROM inbound_messages i JOIN broker_quarantine q ON q.kind = 'inbound' AND q.subject_id = i.message_id WHERE i.origin_key = ? AND i.turn_role = 'trigger' AND i.turn_state IN ('bound','accepted') LIMIT 1",
				)
				.get(originKey) !== null
		);
	}

	/** Origins with bound/accepted turns that need actor reconstruction after boot. */
	inboundNonterminalOrigins(): readonly string[] {
		return this.#database
			.query<{ origin_key: string }, []>(
				`SELECT DISTINCT origin_key FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY origin_key`,
			)
			.all()
			.map((row) => row.origin_key);
	}

	/** Origins with held steers whose terminal trigger no longer reconstructs an actor. */
	inboundHeldSteerOrigins(): readonly string[] {
		return this.#database
			.query<{ origin_key: string }, []>(
				"SELECT DISTINCT origin_key FROM inbound_messages WHERE turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND turn_op_ref IN (SELECT turn_op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done') ORDER BY origin_key",
			)
			.all()
			.map((row) => row.origin_key);
	}

	inboundNonterminalTurnCount(): number {
		return (
			this.#database
				.query<{ n: number }, []>(
					`SELECT COUNT(*) AS n FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND}`,
				)
				.get()?.n ?? 0
		);
	}

	/**
	 * `/new` discards the pending, not-yet-in-a-turn rows received at or before
	 * the floor. This is the ONLY path that completes an inbound row without a
	 * turn, and it is user-initiated; nothing expires a queued message on age.
	 */
	inboundDiscardBefore(originKey: string, floorAt: string): string[] {
		const discard = () => {
			const ids = this.#database
				.query<{ message_id: string }, [string, string]>(
					`SELECT message_id FROM inbound_messages WHERE origin_key = ? AND source = 'platform' AND state = 'pending' AND turn_state IS NULL AND received_at <= ? AND ${REPLAYABLE_INBOUND}`,
				)
				.all(originKey, floorAt)
				.map((row) => row.message_id);
			this.#database
				.query(
					`UPDATE inbound_messages SET state = 'done' WHERE origin_key = ? AND source = 'platform' AND state = 'pending' AND turn_state IS NULL AND received_at <= ? AND ${REPLAYABLE_INBOUND}`,
				)
				.run(originKey, floorAt);
			return ids;
		};
		// resetConversationSession already owns a single atomic transaction; opening
		// another one would break its epoch/floor invariant. Standalone callers get
		// the same atomic select-and-discard boundary here.
		return this.#inTransaction ? discard() : this.withTransaction(discard);
	}

	/**
	 * Conversation context ledger: every inbound platform message (engaged or not)
	 * is recorded here; a turn consumes the unread diff since the last reply.
	 */
	contextRecord(row: {
		messageId: string;
		originKey: string;
		authorId?: string;
		authorName?: string;
		body: string;
		receivedAt?: string;
	}): void {
		this.#database
			.query(
				"INSERT INTO conversation_context (message_id, origin_key, author_id, author_name, body, received_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(
				row.messageId,
				row.originKey,
				row.authorId ?? null,
				row.authorName ?? null,
				row.body,
				row.receivedAt ?? new Date().toISOString(),
			);
	}

	/**
	 * Active retention boundary. Unread rows older than the six-hour relevance
	 * window are expired with aggregate-only evidence; consumed bodies are kept
	 * for a short operational interval, then deleted. Recent unread rows remain
	 * untouched so a failed turn can retry them.
	 */
	contextMaintain(
		now = new Date(),
		retentionMs = CONVERSATION_CONTEXT_RETENTION_MS,
	): { expired: number; deleted: number } {
		if (!Number.isSafeInteger(retentionMs) || retentionMs < CONVERSATION_DIFF_MAX_AGE_MS)
			throw new Error("context retention must cover the unread relevance window");
		return this.withTransaction(() => {
			const at = now.toISOString();
			const relevanceCutoff = new Date(now.getTime() - CONVERSATION_DIFF_MAX_AGE_MS).toISOString();
			const groups = this.#database
				.query<{ origin_key: string; count: number; oldest: string | null; newest: string | null }, [string]>(
					"SELECT origin_key, COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE consumed_at IS NULL AND received_at < ? GROUP BY origin_key",
				)
				.all(relevanceCutoff);
			let expired = 0;
			for (const group of groups) {
				this.#database
					.query(
						"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND received_at < ?",
					)
					.run(at, group.origin_key, relevanceCutoff);
				this.#recordContextOmissions(
					group.origin_key,
					{ count: group.count, oldest: group.oldest, newest: group.newest },
					{ count: 0, oldest: null, newest: null },
					at,
				);
				expired += group.count;
			}
			const deleteCutoff = new Date(now.getTime() - retentionMs).toISOString();
			let deleted = 0;
			while (true) {
				const batch = this.#database
					.query(
						"DELETE FROM conversation_context WHERE rowid IN (SELECT rowid FROM conversation_context WHERE consumed_at IS NOT NULL AND received_at < ? LIMIT ?)",
					)
					.run(deleteCutoff, RETENTION_BATCH_ROWS).changes;
				deleted += batch;
				if (batch < RETENTION_BATCH_ROWS) break;
			}
			return { expired, deleted };
		});
	}

	contextUnread(originKey: string, limit = 100): ConversationContextRow[] {
		return this.#database
			.query<
				{ message_id: string; author_id: string | null; author_name: string | null; body: string; received_at: string },
				[string, number]
			>(
				"SELECT message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL ORDER BY received_at, message_id, rowid LIMIT ?",
			)
			.all(originKey, limit);
	}

	/** When the current session row for this origin was created, if any. */
	contextSessionCreatedAt(originKey: string): string | undefined {
		return this.#database
			.query<{ created_at: string }, [string]>("SELECT created_at FROM sessions WHERE origin_key = ?")
			.get(originKey)?.created_at;
	}

	contextFloorAt(originKey: string): string | undefined {
		return (
			this.#database
				.query<{ floor_at: string | null }, [string]>(
					"SELECT floor_at FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey)?.floor_at ?? undefined
		);
	}

	/**
	 * Atomically prepares the newest bounded unread diff and expires everything
	 * outside it. Selected rows remain unread until the caller proves a terminal
	 * turn outcome; omitted rows are consumed immediately so they cannot replay in
	 * later chunks. Bodies never enter the aggregate diagnostics table.
	 */
	/**
	 * Recent conversation for a fresh session: the last `limit` platform messages
	 * (consumed or not) plus the persona's own confirmed replies, oldest first.
	 * Gives a new epoch the thread it is joining instead of only the unread diff.
	 */
	recentConversation(
		originKey: string,
		conversationId: string,
		limit: number,
		sinceIso: string,
	): Array<{ id?: string; at: string; author: string; body: string }> {
		// /new sets a floor: nothing from before the reset is ever shown again.
		const state = this.#database
			.query<{ floor_at: string | null; floor_row_id: number | null }, [string]>(
				"SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?",
			)
			.get(originKey);
		// Everyone in the window: humans, other bots, and the persona itself.
		const floorAt = [state?.floor_at ?? "", sinceIso].sort().at(-1) ?? sinceIso;
		const floorRowId = state?.floor_row_id ?? 0;
		const inbound = this.#database
			.query<
				{ message_id: string; received_at: string; author_name: string | null; author_id: string | null; body: string },
				[string, string, number, number]
			>(
				"SELECT message_id, received_at, author_name, author_id, body FROM conversation_context WHERE origin_key = ? AND body NOT LIKE '[reaction]%' AND received_at >= ? AND rowid > ? ORDER BY received_at DESC LIMIT ?",
			)
			.all(originKey, floorAt, floorRowId, limit)
			.map((row) => ({
				id: row.message_id,
				at: row.received_at,
				author: row.author_name ?? row.author_id ?? "unknown",
				body: row.body,
			}));
		const replies = this.#database
			.query<{ created_at: string; payload_json: string }, [string, string, number]>(
				"SELECT created_at, payload_json FROM deliveries WHERE state = 'confirmed' AND json_extract(payload_json, '$.origin.conversationId') = ? AND json_extract(payload_json, '$.reaction') IS NULL AND created_at >= ? ORDER BY created_at DESC LIMIT ?",
			)
			.all(conversationId, floorAt, limit)
			.map((row) => {
				const text = (JSON.parse(row.payload_json) as { text?: unknown }).text;
				return { at: row.created_at, author: "you", body: typeof text === "string" ? text : "" };
			})
			.filter((row) => row.body.length > 0);
		return [...inbound, ...replies].sort((a, b) => a.at.localeCompare(b.at)).slice(-limit);
	}

	/**
	 * Recent INBOUND messages only, oldest first, for judging one message against
	 * what people said around it. Deliberately excludes the persona's own replies:
	 * they are long, and measured on 14 real owner messages including them at 200
	 * chars dropped the judge's help score from 0.730 to 0.521 median and turned 5
	 * of 14 into false skips. Read-only - it consumes nothing and sets no floor.
	 */
	recentInbound(
		originKey: string,
		limit: number,
		sinceIso: string,
	): Array<{ id: string; at: string; author: string; body: string }> {
		const state = this.#database
			.query<{ floor_at: string | null; floor_row_id: number | null }, [string]>(
				"SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?",
			)
			.get(originKey);
		const floorAt = [state?.floor_at ?? "", sinceIso].sort().at(-1) ?? sinceIso;
		return this.#database
			.query<
				{
					message_id: string;
					author_name: string | null;
					author_id: string | null;
					body: string;
					received_at: string;
				},
				[string, string, number, number]
			>(
				"SELECT message_id, author_name, author_id, body, received_at FROM conversation_context WHERE origin_key = ? AND body NOT LIKE '[reaction]%' AND received_at >= ? AND rowid > ? ORDER BY received_at DESC LIMIT ?",
			)
			.all(originKey, floorAt, state?.floor_row_id ?? 0, limit)
			.map((row) => ({
				id: row.message_id,
				at: row.received_at,
				author: row.author_name ?? row.author_id ?? "unknown",
				body: row.body,
			}))
			.reverse();
	}

	contextWindow(
		originKey: string,
		triggerMessageId: string,
		now = new Date(),
		limit = CONVERSATION_DIFF_MAX_ROWS,
		maxAgeMs = CONVERSATION_DIFF_MAX_AGE_MS,
	): ConversationContextWindow {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("context window limit must be positive");
		if (!Number.isSafeInteger(maxAgeMs) || maxAgeMs < 0) throw new Error("context max age must be non-negative");
		return this.withTransaction(() => {
			const state = this.#database
				.query<{ floor_at: string | null; floor_row_id: number }, [string]>(
					"SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey);
			const floorRowId = state?.floor_row_id ?? 0;
			const ageFloor = new Date(now.getTime() - maxAgeMs).toISOString();
			// `/new` owns the durable context floor. A session row is written only after
			// inbound acceptance, so treating its creation timestamp as a floor would
			// erase the very first settled batch before its persistent session can read it.
			const effectiveFloor = [ageFloor, state?.floor_at]
				.filter((value): value is string => value !== undefined && value !== null)
				.sort()
				.at(-1) as string;
			// One eligibility rule for selection, truncation and expiry: unread, not
			// the current trigger, inside the age bound, and either a carried
			// survivor at or below the true reset floor (#413) or at/after the
			// effective floor. A row stamped before the floor but recorded after
			// reset carries a post-reset rowid, so it is not carry: it expires.
			const eligible =
				"origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND (rowid <= ? OR received_at >= ?)";
			const eligibleParams: (string | number)[] = [originKey, triggerMessageId, ageFloor, floorRowId, effectiveFloor];
			const expireWhere =
				"origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND (received_at < ? OR (rowid > ? AND received_at < ?))";
			const expireParams: (string | number)[] = [originKey, triggerMessageId, ageFloor, floorRowId, effectiveFloor];
			const expired = this.#contextAggregate(expireWhere, expireParams);
			if (expired.count > 0) {
				this.#database
					.query(`UPDATE conversation_context SET consumed_at = ? WHERE ${expireWhere}`)
					.run(now.toISOString(), ...expireParams);
			}

			const newest = this.#database
				.query<ConversationContextRow & { row_id: number }, (string | number)[]>(
					`SELECT rowid AS row_id, message_id, author_id, author_name, body, received_at FROM conversation_context WHERE ${eligible} ORDER BY received_at DESC, message_id DESC, rowid DESC LIMIT ?`,
				)
				.all(...eligibleParams, limit);
			const boundary = newest[newest.length - 1];
			const olderThanBoundary =
				"AND (received_at < ? OR (received_at = ? AND message_id < ?) OR (received_at = ? AND message_id = ? AND rowid < ?))";
			const boundaryParams: (string | number)[] = boundary
				? [
						boundary.received_at,
						boundary.received_at,
						boundary.message_id,
						boundary.received_at,
						boundary.message_id,
						boundary.row_id,
					]
				: [];
			const truncated = boundary
				? this.#contextAggregate(`${eligible} ${olderThanBoundary}`, [...eligibleParams, ...boundaryParams])
				: { count: 0, oldest: null, newest: null };
			if (boundary && truncated.count > 0) {
				this.#database
					.query(`UPDATE conversation_context SET consumed_at = ? WHERE ${eligible} ${olderThanBoundary}`)
					.run(now.toISOString(), ...eligibleParams, ...boundaryParams);
			}
			this.#recordContextOmissions(originKey, expired, truncated, now.toISOString());
			const pending = this.#pendingContextEvidence(originKey);
			const rows = newest
				.slice()
				.reverse()
				.map(({ row_id: _rowId, ...row }) => row);
			return {
				rows,
				selectedMessageIds: rows.map((row) => row.message_id),
				effectiveFloor,
				expiredCount: pending.expired,
				truncatedCount: pending.truncated,
				omittedOldestAt: pending.oldest,
				omittedNewestAt: pending.newest,
				omissionRevision: pending.revision,
				diagnostics: this.contextDiagnostics(originKey),
			};
		});
	}

	/** Records that a turn ended in failure, so a later `/new` can carry its unanswered messages (#409). */
	markTurnFailed(originKey: string, opRef: string, at = new Date().toISOString()): void {
		this.metaSet(`${failedTurnKeyPrefix(originKey)}${opRef}`, at);
	}

	/**
	 * Establishes a durable reset floor and expires every pre-floor unread row,
	 * except the unanswered tail: the messages of turns that failed after the
	 * origin's last answered turn (each trigger plus the messages steered into
	 * it), within the unread relevance window. A `/new` issued because turns kept
	 * failing must not erase the asks those turns never answered (#409). Carried
	 * rows are made unread again (a `[turn failed]` notice is not an answer), but
	 * the floor stays true: `floor_at` is the reset timestamp and `floor_row_id`
	 * the newest row present at reset. Only contextWindow treats a surviving
	 * unread row at or below that rowid as carry; ordinary history readers keep
	 * the strict boundary (#413). Failure markers for the origin are consumed
	 * here.
	 */
	contextSetFloor(originKey: string, floorAt = new Date().toISOString()): void {
		const maxRowId =
			this.#database
				.query<{ row_id: number }, [string]>(
					"SELECT COALESCE(MAX(rowid), 0) AS row_id FROM conversation_context WHERE origin_key = ?",
				)
				.get(originKey)?.row_id ?? 0;
		const carriedIds = this.#unansweredTailRowIds(originKey, floorAt);
		const keep = carriedIds.length > 0 ? ` AND rowid NOT IN (${carriedIds.map(() => "?").join(",")})` : "";
		const expireWhere = `origin_key = ? AND consumed_at IS NULL AND (received_at < ? OR rowid <= ?)${keep}`;
		const expireParams = [originKey, floorAt, maxRowId, ...carriedIds];
		const expired = this.#contextAggregate(expireWhere, expireParams);
		if (expired.count > 0)
			this.#database
				.query(`UPDATE conversation_context SET consumed_at = ? WHERE ${expireWhere}`)
				.run(floorAt, ...expireParams);
		if (carriedIds.length > 0)
			this.#database
				.query(
					`UPDATE conversation_context SET consumed_at = NULL WHERE rowid IN (${carriedIds.map(() => "?").join(",")})`,
				)
				.run(...carriedIds);
		const prefix = failedTurnKeyPrefix(originKey);
		this.#database.query("DELETE FROM meta WHERE key >= ? AND key < ?").run(prefix, `${prefix}\uffff`);
		this.#database
			.query(
				"INSERT INTO conversation_context_state (origin_key, floor_at, floor_row_id) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET floor_at = excluded.floor_at, floor_row_id = excluded.floor_row_id",
			)
			.run(originKey, floorAt, maxRowId);
		this.#recordContextOmissions(originKey, expired, { count: 0, oldest: null, newest: null }, floorAt);
	}

	/** Context rowids (ascending) of failed turns after the last answered turn, inside the relevance window. */
	#unansweredTailRowIds(originKey: string, floorAt: string): number[] {
		const prefix = failedTurnKeyPrefix(originKey);
		const failedOps = new Set(
			this.#database
				.query<{ key: string }, [string, string]>("SELECT key FROM meta WHERE key >= ? AND key < ?")
				.all(prefix, `${prefix}\uffff`)
				.map((row) => row.key.slice(prefix.length)),
		);
		if (failedOps.size === 0) return [];
		const relevanceCutoff = new Date(Date.parse(floorAt) - CONVERSATION_DIFF_MAX_AGE_MS).toISOString();
		const triggers = this.#database
			.query<{ turn_op_ref: string }, [string, string, string]>(
				"SELECT turn_op_ref FROM inbound_messages WHERE origin_key = ? AND turn_role = 'trigger' AND state = 'done' AND turn_op_ref IS NOT NULL AND received_at >= ? AND received_at < ? ORDER BY received_at, rowid",
			)
			.all(originKey, relevanceCutoff, floorAt);
		const carryOps: string[] = [];
		for (const { turn_op_ref: opRef } of triggers) {
			if (failedOps.has(opRef)) carryOps.push(opRef);
			else carryOps.length = 0;
		}
		if (carryOps.length === 0) return [];
		return this.#database
			.query<{ row_id: number }, (string | number)[]>(
				`SELECT c.rowid AS row_id FROM conversation_context c WHERE c.origin_key = ? AND c.received_at >= ? AND c.received_at < ? AND c.message_id IN (SELECT message_id FROM inbound_messages WHERE origin_key = ? AND turn_op_ref IN (${carryOps.map(() => "?").join(",")})) ORDER BY c.rowid`,
			)
			.all(originKey, relevanceCutoff, floorAt, originKey, ...carryOps)
			.map((row) => row.row_id);
	}

	/** Commits a successful/duplicate-safe turn cursor and acknowledges its omission notice atomically. */
	contextCommitWindow(originKey: string, messageIds: readonly string[], omissionRevision: number): void {
		const now = new Date().toISOString();
		this.withTransaction(() => {
			for (const id of messageIds)
				this.#database.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(now, id);
			this.#database
				.query(
					"UPDATE conversation_context_state SET pending_expired_count = 0, pending_truncated_count = 0, pending_omitted_oldest_at = NULL, pending_omitted_newest_at = NULL WHERE origin_key = ? AND omission_revision = ?",
				)
				.run(originKey, omissionRevision);
		});
	}

	contextDiagnostics(originKey?: string): ConversationContextDiagnostics {
		const unread = originKey
			? (this.#database
					.query<{ n: number }, [string]>(
						"SELECT COUNT(*) AS n FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL",
					)
					.get(originKey)?.n ?? 0)
			: (this.#database
					.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM conversation_context WHERE consumed_at IS NULL")
					.get()?.n ?? 0);
		const state = originKey
			? this.#database
					.query<
						{
							expired: number;
							truncated: number;
							oldest: string | null;
							newest: string | null;
							floor: string | null;
						},
						[string]
					>(
						"SELECT expired_count AS expired, truncated_count AS truncated, omitted_oldest_at AS oldest, omitted_newest_at AS newest, floor_at AS floor FROM conversation_context_state WHERE origin_key = ?",
					)
					.get(originKey)
			: this.#database
					.query<{ expired: number; truncated: number; oldest: string | null; newest: string | null; floor: null }, []>(
						"SELECT COALESCE(SUM(expired_count), 0) AS expired, COALESCE(SUM(truncated_count), 0) AS truncated, MIN(omitted_oldest_at) AS oldest, MAX(omitted_newest_at) AS newest, NULL AS floor FROM conversation_context_state",
					)
					.get();
		return {
			unread,
			expired: state?.expired ?? 0,
			truncated: state?.truncated ?? 0,
			omittedOldestAt: state?.oldest ?? null,
			omittedNewestAt: state?.newest ?? null,
			floorAt: state?.floor ?? null,
		};
	}

	contextDiagnosticsByOrigin(): Map<string, ConversationContextDiagnostics> {
		const origins = this.#database
			.query<{ origin_key: string }, []>(
				"SELECT origin_key FROM conversation_context UNION SELECT origin_key FROM conversation_context_state",
			)
			.all();
		return new Map(origins.map((row) => [row.origin_key, this.contextDiagnostics(row.origin_key)]));
	}

	#contextAggregate(
		where: string,
		params: readonly (string | number)[],
	): {
		count: number;
		oldest: string | null;
		newest: string | null;
	} {
		const row = this.#database
			.query<{ count: number; oldest: string | null; newest: string | null }, (string | number)[]>(
				`SELECT COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE ${where}`,
			)
			.get(...params);
		return row ?? { count: 0, oldest: null, newest: null };
	}

	#recordContextOmissions(
		originKey: string,
		expired: { count: number; oldest: string | null; newest: string | null },
		truncated: { count: number; oldest: string | null; newest: string | null },
		at: string,
	): void {
		if (expired.count === 0 && truncated.count === 0) return;
		const oldest =
			[expired.oldest, truncated.oldest].filter((value): value is string => value !== null).sort()[0] ?? null;
		const newest =
			[expired.newest, truncated.newest]
				.filter((value): value is string => value !== null)
				.sort()
				.at(-1) ?? null;
		this.#database.query("INSERT OR IGNORE INTO conversation_context_state (origin_key) VALUES (?)").run(originKey);
		this.#database
			.query(
				"UPDATE conversation_context_state SET expired_count = expired_count + ?, truncated_count = truncated_count + ?, pending_expired_count = pending_expired_count + ?, pending_truncated_count = pending_truncated_count + ?, omission_revision = omission_revision + 1, omitted_oldest_at = CASE WHEN omitted_oldest_at IS NULL OR ? < omitted_oldest_at THEN ? ELSE omitted_oldest_at END, omitted_newest_at = CASE WHEN omitted_newest_at IS NULL OR ? > omitted_newest_at THEN ? ELSE omitted_newest_at END, pending_omitted_oldest_at = CASE WHEN pending_omitted_oldest_at IS NULL OR ? < pending_omitted_oldest_at THEN ? ELSE pending_omitted_oldest_at END, pending_omitted_newest_at = CASE WHEN pending_omitted_newest_at IS NULL OR ? > pending_omitted_newest_at THEN ? ELSE pending_omitted_newest_at END, last_omitted_at = ? WHERE origin_key = ?",
			)
			.run(
				expired.count,
				truncated.count,
				expired.count,
				truncated.count,
				oldest,
				oldest,
				newest,
				newest,
				oldest,
				oldest,
				newest,
				newest,
				at,
				originKey,
			);
	}

	#pendingContextEvidence(originKey: string): {
		expired: number;
		truncated: number;
		oldest: string | null;
		newest: string | null;
		revision: number;
	} {
		return (
			this.#database
				.query<
					{ expired: number; truncated: number; oldest: string | null; newest: string | null; revision: number },
					[string]
				>(
					"SELECT pending_expired_count AS expired, pending_truncated_count AS truncated, pending_omitted_oldest_at AS oldest, pending_omitted_newest_at AS newest, omission_revision AS revision FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey) ?? { expired: 0, truncated: 0, oldest: null, newest: null, revision: 0 }
		);
	}

	contextConsume(messageIds: readonly string[]): void {
		if (messageIds.length === 0) return;
		const now = new Date().toISOString();
		this.withTransaction(() => {
			for (const id of messageIds)
				this.#database.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(now, id);
		});
	}

	/** Whether this origin ever ingested `messageId` (as a turn trigger/steer, or as conversation context). */
	inboundKnownMessage(originKey: string, messageId: string): boolean {
		return (
			(this.#database
				.query<{ n: number }, [string, string, string, string]>(
					"SELECT (SELECT COUNT(*) FROM inbound_messages WHERE origin_key = ? AND message_id = ?) + (SELECT COUNT(*) FROM conversation_context WHERE origin_key = ? AND message_id = ?) AS n",
				)
				.get(originKey, messageId, originKey, messageId)?.n ?? 0) > 0
		);
	}

	/**
	 * Rewrites the recorded body of an already-ingested platform message after
	 * the user edited it, so a later unread diff shows what the message says
	 * now. The edit itself is streamed as its own inbound row.
	 */
	contextUpdateBody(originKey: string, messageId: string, body: string): boolean {
		return (
			this.#database
				.query("UPDATE conversation_context SET body = ? WHERE origin_key = ? AND message_id = ?")
				.run(body, originKey, messageId).changes > 0
		);
	}

	inboundPendingCount(originKey: string): number {
		return (
			this.#database
				.query<{ n: number }, [string]>(
					`SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND ${REPLAYABLE_INBOUND}`,
				)
				.get(originKey)?.n ?? 0
		);
	}

	getSession(originKey: string): string | undefined {
		return this.#database
			.query<{ gjc_session_id: string }, [string]>("SELECT gjc_session_id FROM sessions WHERE origin_key = ?")
			.get(originKey)?.gjc_session_id;
	}

	/** Currently referenced sessions, restricted to gateway ownership under the active authority. */
	referencedSessionIds(): ReadonlySet<string> {
		const rows = this.#database
			.query<{ id: string }, []>(
				`SELECT b.session_id AS id FROM broker_owned_bindings b
				JOIN broker_authority a ON a.authority_key = b.authority_key
				WHERE a.singleton = 1 AND b.session_id IN (
					SELECT gjc_session_id FROM sessions WHERE gjc_session_id <> ''
					UNION SELECT bound_session_id FROM inbound_messages
					WHERE bound_session_id IS NOT NULL AND state = 'pending'
					AND message_id NOT IN (SELECT subject_id FROM broker_quarantine WHERE kind = 'inbound')
					UNION SELECT session_id FROM work_attempt_runtime WHERE settled_at IS NULL
					AND job_id NOT IN (SELECT subject_id FROM broker_quarantine WHERE kind = 'work')
				)`,
			)
			.all();
		return new Set(rows.map((row) => row.id));
	}

	/** Bound `work.run` lanes: the sessions the lane governor counts against the admission cap. */
	workLaneRows(): Array<{ origin_key: string; gjc_session_id: string; last_activity_at: string | null }> {
		return this.#database
			.query<{ origin_key: string; gjc_session_id: string; last_activity_at: string | null }, []>(
				"SELECT origin_key, gjc_session_id, last_activity_at FROM sessions WHERE origin_key LIKE 'work/task/%' AND gjc_session_id <> '' ORDER BY last_activity_at",
			)
			.all();
	}

	/** Check if this gateway is in broker mode (has an active broker authority). */
	isBrokerMode(): boolean {
		return this.#brokerAuthority() !== null;
	}

	/** Get the repo for a work lane by session ID from the owned binding, or undefined if not found. */
	workLaneRepoBySessionId(sessionId: string): string | undefined {
		const authority = this.#brokerAuthority();
		if (!authority) return undefined;
		const row = this.#database
			.query<{ repo: string }, [string, string]>(
				"SELECT repo FROM broker_owned_bindings WHERE authority_key = ? AND session_id = ?",
			)
			.get(brokerAuthorityKey(authority), sessionId);
		return row?.repo;
	}

	putSession(originKey: string, sessionId: string): void {
		if (this.#database.query("SELECT 1 FROM broker_authority").get()) throw new BrokerAuthorityError("unowned_session");
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id",
			)
			.run(originKey, sessionId, new Date().toISOString());
	}

	/**
	 * Fenced persistent-session binding. A create response may race `/new`; the
	 * binding must never overwrite a newer durable epoch with an old session id.
	 */
	putSessionAtEpoch(originKey: string, sessionId: string, epoch: number): boolean {
		if (this.#database.query("SELECT 1 FROM broker_authority").get()) throw new BrokerAuthorityError("unowned_session");
		if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error("session epoch must be a non-negative integer");
		return (
			this.#database
				.query(
					"INSERT INTO sessions (origin_key, gjc_session_id, epoch, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id WHERE sessions.epoch = excluded.epoch",
				)
				.run(originKey, sessionId, epoch, new Date().toISOString()).changes === 1
		);
	}

	deliveryCreate(row: { id: string; turnId: string; originKey: string; payloadJson: string }): boolean {
		const now = new Date().toISOString();
		return (
			this.#database
				.query(
					"INSERT OR IGNORE INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
				)
				.run(row.id, row.turnId, row.originKey, row.payloadJson, now, now).changes === 1
		);
	}

	deliveryUpdate(id: string, state: string, attempts?: number, lastError?: string): void {
		this.#database
			.query(
				"UPDATE deliveries SET state = ?, attempts = COALESCE(?, attempts), last_error = COALESCE(?, last_error), updated_at = ? WHERE delivery_id = ?",
			)
			.run(state, attempts ?? null, lastError ?? null, new Date().toISOString(), id);
	}

	deliveryExpireBefore(
		before: string,
		expiredAt: string,
	): Array<{
		delivery_id: string;
		origin_key: string;
		attempts: number;
		updated_at: string;
		last_error: string | null;
	}> {
		const rows = this.#database
			.query<{ delivery_id: string; origin_key: string; attempts: number; last_error: string | null }, [string]>(
				"SELECT delivery_id, origin_key, attempts, last_error FROM deliveries WHERE state NOT IN ('confirmed', 'expired') AND created_at < ?",
			)
			.all(before);
		if (rows.length === 0) return [];
		this.#database
			.query(
				"UPDATE deliveries SET state = 'expired', updated_at = ? WHERE state NOT IN ('confirmed', 'expired') AND created_at < ?",
			)
			.run(expiredAt, before);
		return rows.map((row) => ({ ...row, updated_at: expiredAt }));
	}

	deliveryRequeueById(id: string): string[] {
		const changed = this.#database
			.query(
				"UPDATE deliveries SET state = 'pending', attempts = 0, updated_at = ? WHERE delivery_id = ? AND state IN ('expired', 'failed_ambiguous', 'pending')",
			)
			.run(new Date().toISOString(), id).changes;
		return changed === 1 ? [id] : [];
	}

	deliveryRequeueSince(since: string): string[] {
		const rows = this.#database
			.query<{ delivery_id: string }, [string]>(
				"SELECT delivery_id FROM deliveries WHERE state IN ('expired', 'failed_ambiguous', 'pending') AND updated_at >= ? ORDER BY updated_at, delivery_id",
			)
			.all(since);
		if (rows.length === 0) return [];
		this.#database
			.query(
				"UPDATE deliveries SET state = 'pending', attempts = 0, updated_at = ? WHERE state IN ('expired', 'failed_ambiguous', 'pending') AND updated_at >= ?",
			)
			.run(new Date().toISOString(), since);
		return rows.map((row) => row.delivery_id);
	}

	deliveryRows(): Array<{
		delivery_id: string;
		turn_id: string;
		origin_key: string;
		payload_json: string;
		state: string;
		attempts: number;
		created_at: string;
		updated_at: string;
		last_error: string | null;
	}> {
		return this.#database
			.query(
				"SELECT delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at, last_error FROM deliveries ORDER BY created_at",
			)
			.all() as Array<{
			delivery_id: string;
			turn_id: string;
			origin_key: string;
			payload_json: string;
			state: string;
			attempts: number;
			created_at: string;
			updated_at: string;
			last_error: string | null;
		}>;
	}

	deliveryGet(deliveryId: string): DeliveryDbRow | undefined {
		return (
			this.#database
				.query<DeliveryDbRow, [string]>(`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE delivery_id = ?`)
				.get(deliveryId) ?? undefined
		);
	}

	deliveryGetMany(deliveryIds: readonly string[]): DeliveryDbRow[] {
		const rows: DeliveryDbRow[] = [];
		for (const id of new Set(deliveryIds)) {
			const row = this.deliveryGet(id);
			if (row) rows.push(row);
		}
		return rows;
	}

	/** Rows still owed to an adapter. Bounded by open work, never by delivery history. */
	deliveryUnsettledRows(): DeliveryDbRow[] {
		return this.#database
			.query<DeliveryDbRow, []>(
				`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') ORDER BY created_at`,
			)
			.all();
	}

	/** Ledger status without materializing history: unsettled count/age, the five oldest unsettled rows, expired count and the five newest. */
	deliveryLedgerCounts(): {
		pending: number;
		oldestCreatedAt: string | null;
		expired: number;
		recentExpired: Array<{
			delivery_id: string;
			origin_key: string;
			attempts: number;
			updated_at: string;
			last_error: string | null;
		}>;
		recentPending: DeliveryDbRow[];
	} {
		const open = this.#database
			.query<{ n: number; oldest: string | null }, []>(
				"SELECT COUNT(*) AS n, MIN(created_at) AS oldest FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous')",
			)
			.get();
		const expired = this.#database
			.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM deliveries WHERE state = 'expired'")
			.get();
		const recentExpired = this.#database
			.query<
				{ delivery_id: string; origin_key: string; attempts: number; updated_at: string; last_error: string | null },
				[]
			>(
				"SELECT delivery_id, origin_key, attempts, updated_at, last_error FROM deliveries WHERE state = 'expired' ORDER BY updated_at DESC, delivery_id LIMIT 5",
			)
			.all();
		const recentPending = this.#database
			.query<DeliveryDbRow, []>(
				`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') ORDER BY created_at LIMIT 5`,
			)
			.all();
		return {
			pending: open?.n ?? 0,
			oldestCreatedAt: open?.oldest ?? null,
			expired: expired?.n ?? 0,
			recentExpired,
			recentPending,
		};
	}

	monitorCreate(row: {
		id: string;
		name: string;
		triggerJson: string;
		eventTypesJson: string;
		burstPolicy: string;
		overlap?: string;
		channelTargetJson: string | null;
		enabled: boolean;
		instruction: string | null;
		modelJson: string | null;
		serviceTier: string | null;
		procedureFilesJson?: string | null;
	}): void {
		this.#database
			.query(
				"INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, overlap, channel_target_json, enabled, created_at, instruction, model_json, service_tier, procedure_files_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				row.id,
				row.name,
				row.triggerJson,
				row.eventTypesJson,
				row.burstPolicy,
				row.overlap ?? "queue",
				row.channelTargetJson,
				row.enabled ? 1 : 0,
				new Date().toISOString(),
				row.instruction,
				row.modelJson,
				row.serviceTier,
				row.procedureFilesJson ?? null,
			);
	}
	monitorRows(): Array<{
		monitor_id: string;
		name: string;
		trigger_json: string;
		event_types_json: string;
		burst_policy: string;
		overlap: string;
		channel_target_json: string | null;
		enabled: number;
		created_at: string;
		instruction: string | null;
		model_json: string | null;
		service_tier: string | null;
		procedure_files_json: string | null;
	}> {
		return this.#database
			.query(
				"SELECT monitor_id, name, trigger_json, event_types_json, burst_policy, overlap, channel_target_json, enabled, created_at, instruction, model_json, service_tier, procedure_files_json FROM monitors ORDER BY created_at",
			)
			.all() as Array<{
			monitor_id: string;
			name: string;
			trigger_json: string;
			event_types_json: string;
			burst_policy: string;
			overlap: string;
			channel_target_json: string | null;
			enabled: number;
			created_at: string;
			instruction: string | null;
			model_json: string | null;
			service_tier: string | null;
			procedure_files_json: string | null;
		}>;
	}
	monitorUpdate(row: {
		id: string;
		name: string;
		triggerJson: string;
		eventTypesJson: string;
		burstPolicy: string;
		channelTargetJson: string | null;
		enabled: boolean;
		instruction: string | null;
		modelJson: string | null;
		serviceTier: string | null;
		procedureFilesJson: string | null;
	}): boolean {
		return (
			this.#database
				.query(
					"UPDATE monitors SET name = ?, trigger_json = ?, event_types_json = ?, burst_policy = ?, channel_target_json = ?, enabled = ?, instruction = ?, model_json = ?, service_tier = ?, procedure_files_json = ? WHERE monitor_id = ?",
				)
				.run(
					row.name,
					row.triggerJson,
					row.eventTypesJson,
					row.burstPolicy,
					row.channelTargetJson,
					row.enabled ? 1 : 0,
					row.instruction,
					row.modelJson,
					row.serviceTier,
					row.procedureFilesJson,
					row.id,
				).changes > 0
		);
	}
	monitorDelete(id: string): boolean {
		const deleted = this.#database.query("DELETE FROM monitors WHERE monitor_id = ?").run(id).changes > 0;
		this.metaDelete(monitorCronMetaKey(id));
		return deleted;
	}
	/**
	 * Admits one monitor event. Under the `skip` overlap policy the predecessor
	 * check and the insert share one statement, so two concurrent fires can never
	 * both see "nothing in flight": the event lands as terminal `skipped` naming
	 * the oldest same-monitor event still awaiting authoring (issue #83).
	 * Returns the predecessor's id when skipped, undefined when admitted.
	 */
	monitorEventCreate(row: {
		eventId: string;
		monitorId: string;
		eventType: string;
		payloadJson: string;
		firedAt: string;
		overlap?: "queue" | "skip";
	}): string | undefined {
		const predecessor =
			row.overlap === "skip"
				? `(SELECT event_id FROM monitor_events WHERE monitor_id = ? AND stage IN (${IN_FLIGHT_MONITOR_STAGES}) AND ${REPLAYABLE_MONITOR} ORDER BY fired_at, rowid LIMIT 1)`
				: "NULL";
		const inserted = this.#database
			.query<{ skipped_by: string | null }, string[]>(
				`INSERT INTO monitor_events (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, skipped_by, updated_at) SELECT ?, ?, ?, ?, ?, CASE WHEN p.id IS NULL THEN 'admitted' ELSE 'skipped' END, NULL, p.id, ? FROM (SELECT ${predecessor} AS id) AS p RETURNING skipped_by`,
			)
			.get(
				row.eventId,
				row.monitorId,
				row.eventType,
				row.payloadJson,
				row.firedAt,
				new Date().toISOString(),
				...(row.overlap === "skip" ? [row.monitorId] : []),
			);
		return inserted?.skipped_by ?? undefined;
	}
	/**
	 * Durable dispatch lease (red-team blocker 1): a process claims an event
	 * before sending its authoring turn. The claim stores an owner token and an
	 * expiry; a claim is valid only while `expires_at` is in the future. This
	 * closes the restart race where the external gjc authoring turn outlives a
	 * dead gateway process — a new process must not re-author the same event
	 * concurrently, and only an expired claim may be stolen.
	 *
	 * Semantics:
	 * - claim succeeds iff no live (unexpired) lease exists for the event;
	 * - stealing replaces the expired lease with a fresh lease_id + owner;
	 * - `monitorEventReleaseLease` is lease-guarded: a stale attempt whose lease
	 *   expired and was stolen can never release the newer owner's claim.
	 */
	monitorEventAcquireLease(eventId: string, owner: string, leaseId: string, ttlMs: number, now = Date.now()): boolean {
		this.#assertNotQuarantined("monitor", eventId);
		const nowIso = new Date(now).toISOString();
		const expiresIso = new Date(now + ttlMs).toISOString();
		const claim = this.#database
			.query(
				`INSERT INTO dispatch_leases (event_id, owner, lease_id, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET owner = excluded.owner, lease_id = excluded.lease_id, acquired_at = excluded.acquired_at, expires_at = excluded.expires_at
WHERE excluded.acquired_at IS NOT NULL AND (SELECT expires_at FROM dispatch_leases WHERE event_id = excluded.event_id) <= excluded.acquired_at`,
			)
			.run(eventId, owner, leaseId, nowIso, expiresIso).changes;
		return claim > 0;
	}
	/** Releases a lease only when the caller still owns it (stale attempts are no-ops). */
	monitorEventReleaseLease(eventId: string, leaseId: string): void {
		this.#database.query("DELETE FROM dispatch_leases WHERE event_id = ? AND lease_id = ?").run(eventId, leaseId);
	}
	/** Total live (unexpired) leases — test/ops hygiene seam for leak detection. */
	monitorLeaseLiveCount(now = Date.now()): number {
		return (
			this.#database
				.query<{ n: number }, [string]>("SELECT COUNT(*) AS n FROM dispatch_leases WHERE expires_at > ?")
				.get(new Date(now).toISOString())?.n ?? 0
		);
	}
	/** Returns the lease id of the live (unexpired) claim, if any. */
	monitorEventLiveLeaseOwner(eventId: string, now = Date.now()): string | undefined {
		const row = this.#database
			.query<{ lease_id: string; expires_at: string }, [string]>(
				"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
			)
			.get(eventId);
		if (row && Date.parse(row.expires_at) > now) return row.lease_id;
		return undefined;
	}
	/** Extends a live lease only when the caller still owns it (stale attempts are no-ops). */
	monitorEventRenewLease(eventId: string, leaseId: string, ttlMs: number, now = Date.now()): boolean {
		this.#assertNotQuarantined("monitor", eventId);
		const row = this.#database
			.query<{ lease_id: string }, [string, string, string]>(
				"SELECT lease_id FROM dispatch_leases WHERE event_id = ? AND lease_id = ? AND expires_at > ?",
			)
			.get(eventId, leaseId, new Date(now).toISOString());
		if (!row) return false;
		this.#database
			.query("UPDATE dispatch_leases SET expires_at = ? WHERE event_id = ? AND lease_id = ?")
			.run(new Date(now + ttlMs).toISOString(), eventId, leaseId);
		return true;
	}
	/**
	 * Fenced stage transition (round-3 blocker 1): the lease ownership check and
	 * the stage write happen atomically in ONE UPDATE — the WHERE clause includes
	 * a live-lease subquery, so a lease stolen between the caller's check and the
	 * write cannot be exploited (no TOCTOU). Returns true when this attempt's
	 * write actually landed.
	 */
	monitorEventFencedUpdate(
		eventId: string,
		leaseId: string,
		stage: MonitorEventStage,
		batchId: string | null = null,
		now = Date.now(),
	): boolean {
		if (!MONITOR_EVENT_STAGES.includes(stage)) throw new Error(`unknown monitor event stage: ${stage}`);
		const changes = this.#database
			.query(
				`UPDATE monitor_events SET stage = ?, batch_id = COALESCE(?, batch_id), updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
			)
			.run(stage, batchId, new Date(now).toISOString(), eventId, leaseId, new Date(now).toISOString()).changes;
		return changes > 0;
	}
	/**
	 * Fenced output write: authored_outputs upsert + authored stage transition in
	 * one transaction, both gated on the live lease. A stale attempt cannot write
	 * its note over the newer attempt's.
	 */
	monitorEventFencedAuthor(
		eventId: string,
		leaseId: string,
		note: string,
		noDelivery = false,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(
					noDelivery ? "authored_no_delivery" : "authored",
					new Date(now).toISOString(),
					eventId,
					leaseId,
					new Date(now).toISOString(),
				).changes;
			if (changes === 0) return false;
			this.authoredOutputCreate(eventId, note);
			return true;
		});
	}
	/**
	 * Fenced delivery admission (round-3 blocker 1): the pending delivery row is
	 * inserted only if EVERY given event still holds leaseId live, checked in the
	 * same transaction as the insert. Returns false (nothing written) when any
	 * event's lease was lost — a stale attempt can never emit.
	 */
	monitorDeliveryPrepareFenced(
		deliveryId: string,
		batchId: string,
		originKey: string,
		payloadJson: string,
		eventIds: readonly string[],
		leaseId: string,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			for (const eventId of eventIds) {
				this.#assertNotQuarantined("monitor", eventId);
				const lease = this.#database
					.query<{ lease_id: string; expires_at: string }, [string]>(
						"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
					)
					.get(eventId);
				if (!lease || lease.lease_id !== leaseId || Date.parse(lease.expires_at) <= now) return false;
			}
			const nowIso = new Date(now).toISOString();
			this.#database
				.query(
					"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
				)
				.run(deliveryId, batchId, originKey, payloadJson, nowIso, nowIso);
			return true;
		});
	}
	/**
	 * Atomic LEASE-FENCED authored output + memory-intent admission (round-3/4
	 * blockers): the authored_outputs upsert and the queued monitor-event intent
	 * commit in ONE transaction, and the stage UPDATE carries the live-lease
	 * predicate — a crash or interleaving between writes can never produce zero
	 * or duplicate intents for the same event, and a stale attempt cannot write.
	 */
	monitorEventFencedAuthorWithIntent(
		eventId: string,
		leaseId: string,
		note: string,
		noDelivery: boolean,
		eventType: string,
		intentId: string,
		originRefJson: string,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			// leaseId === "" means UNFENCED (reconcile re-author path: single-flight,
			// event already proven non-terminal). Dispatch attempts always pass a
			// real lease id and get the EXISTS predicate.
			const leasePredicate =
				leaseId === ""
					? "1=1"
					: `EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`;
			const leaseArgs = leaseId === "" ? [] : [leaseId, new Date(now).toISOString()];
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, updated_at = ?
WHERE event_id = ? AND ${leasePredicate}`,
				)
				.run(
					noDelivery ? "authored_no_delivery" : "authored",
					new Date(now).toISOString(),
					eventId,
					...leaseArgs,
				).changes;
			if (changes === 0) return false;
			this.authoredOutputCreate(eventId, note);
			// Idempotent intent: the INSERT is keyed on the deterministic intent id;
			// a second admission for the same event is a no-op.
			const existing = this.#database
				.query<{ id: string }, [string]>("SELECT id FROM memory_intents WHERE id = ?")
				.get(intentId);
			if (existing) return true;
			this.memoryIntentCreate({
				id: intentId,
				kind: "monitor-event",
				payloadJson: JSON.stringify({
					kind: "monitor-event",
					identity: `monitor-event:${eventId}`,
					originRefJson,
					userText: `Monitor event ${eventId}: ${eventType}`,
					replyText: note,
				}),
			});
			return true;
		});
	}
	/**
	 * Fenced failure write: failed stage + public-safe evidence row in one
	 * transaction, both gated on the live lease (round-3 blocker 1).
	 */
	monitorEventFencedFail(
		eventId: string,
		leaseId: string,
		batchId: string,
		code: string,
		detail: string,
		now = Date.now(),
		terminal = false,
		options?: MonitorFailureOptions,
	): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, batch_id = ?, updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(
					terminal ? "failed_no_retry" : "failed",
					batchId,
					new Date(now).toISOString(),
					eventId,
					leaseId,
					new Date(now).toISOString(),
				).changes;
			if (changes === 0) return false;
			this.monitorFailureRecord(eventId, code, detail, options);
			return true;
		});
	}
	/** Terminalizes an event that cannot safely enter dispatch, with bounded public-safe evidence. */
	monitorEventTerminalFail(eventId: string, code: string, detail: string): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					"UPDATE monitor_events SET stage = 'failed_no_retry', updated_at = ? WHERE event_id = ? AND stage NOT IN ('delivered', 'authored_no_delivery', 'failed_no_retry')",
				)
				.run(new Date().toISOString(), eventId).changes;
			if (changes === 0) return false;
			this.monitorFailureRecord(eventId, code, detail);
			return true;
		});
	}
	/** True while the given lease is the live claim for the event. */
	monitorEventLeaseHeld(eventId: string, leaseId: string, now = Date.now()): boolean {
		const row = this.#database
			.query<{ lease_id: string; expires_at: string }, [string]>(
				"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
			)
			.get(eventId);
		return row?.lease_id === leaseId && Date.parse(row.expires_at) > now;
	}
	/**
	 * Fail-closed stage transition: an unknown stage name throws instead of
	 * writing a state no recovery path understands.
	 */
	monitorEventUpdate(eventId: string, stage: MonitorEventStage, batchId: string | null = null): void {
		if (!MONITOR_EVENT_STAGES.includes(stage)) throw new Error(`unknown monitor event stage: ${stage}`);
		this.#database
			.query("UPDATE monitor_events SET stage = ?, batch_id = COALESCE(?, batch_id), updated_at = ? WHERE event_id = ?")
			.run(stage, batchId, new Date().toISOString(), eventId);
	}
	/**
	 * Red-team blocker 4: settlement must be MONOTONIC. Once an event is
	 * terminally settled (`delivered`), a late/out-of-order delivery.fail can
	 * never regress it; a duplicate confirm is a no-op. Non-terminal stages
	 * (`authored`) still receive fail-path demotions so failed deliveries stay
	 * operator-visible.
	 */
	/**
	 * Atomic confirm + settlement (terminal-critic blocker 2): the ledger row
	 * transition to `confirmed` and the batch monitor-event settlement commit in
	 * ONE transaction. `outcome` distinguishes unknown / transitioned /
	 * already_terminal so the server can ack idempotently. A crash can no longer
	 * leave confirmed-ledger/authored-event pairs.
	 */
	deliveryConfirmWithSettle(
		deliveryId: string,
		monitorEventStage: "delivered" | "authored",
	): "unknown" | "transitioned" | "already_terminal" {
		return this.withTransaction(() => this.deliveryConfirmWithSettleInTransaction(deliveryId, monitorEventStage));
	}
	/** Joins mapped receipt and activation evidence under the caller's transaction owner. */
	deliveryConfirmWithSettleInTransaction(
		deliveryId: string,
		monitorEventStage: "delivered" | "authored",
	): "unknown" | "transitioned" | "already_terminal" {
		this.requireTransaction();
		const delivery = this.#database
			.query<{ delivery_id: string; turn_id: string; state: string }, [string]>(
				"SELECT delivery_id, turn_id, state FROM deliveries WHERE delivery_id = ?",
			)
			.get(deliveryId);
		if (!delivery) return "unknown";
		if (delivery.state === "confirmed" || delivery.state === "expired") {
			// Idempotent no-op: already terminal. Repairs a legacy split state
			// (confirmed ledger, authored events) if one exists.
			if (delivery.state === "confirmed") {
				this.settleMonitorEventsForTurn(delivery.turn_id, monitorEventStage);
			}
			return "already_terminal";
		}
		this.#database
			.query("UPDATE deliveries SET state = 'confirmed', updated_at = ? WHERE delivery_id = ?")
			.run(new Date().toISOString(), deliveryId);
		this.settleMonitorEventsForTurn(delivery.turn_id, monitorEventStage);
		return "transitioned";
	}
	/** Settles the monitor batch of a turn id (used by the atomic confirm path). */
	settleMonitorEventsForTurn(turnId: string, stage: "delivered" | "authored"): void {
		const events = this.#database
			.query<{ event_id: string }, [string]>("SELECT event_id FROM monitor_events WHERE batch_id = ?")
			.all(turnId);
		for (const event of events) this.monitorEventSettle(event.event_id, stage);
	}
	monitorEventSettle(eventId: string, stage: "delivered" | "authored"): boolean {
		// Delivery acknowledgements still commit; old monitor history is not rewritten.
		if (this.isBrokerQuarantined("monitor", eventId)) return false;
		const row = this.#database
			.query<{ stage: string }, [string]>("SELECT stage FROM monitor_events WHERE event_id = ?")
			.get(eventId);
		if (!row) return false;
		// delivered may ONLY be reached from authored: an omitted event still in
		// dispatched/batched can never be promoted by a batch-wide settlement
		// (terminal-critic blocker 3).
		if (
			stage === "delivered" &&
			(row.stage === "authored" || (row.stage === "failed_no_retry" && this.authoredOutput(eventId) !== undefined))
		) {
			this.monitorEventUpdate(eventId, "delivered");
			return true;
		}
		// Fail path: only demote events still in `authored`; never touch delivered
		// (or any terminal stage).
		if (stage === "authored" && row.stage === "authored") return false;
		if (
			stage === "authored" &&
			row.stage !== "delivered" &&
			row.stage !== "authored_no_delivery" &&
			row.stage !== "failed_no_retry"
		) {
			this.monitorEventUpdate(eventId, "authored");
			return true;
		}
		return false;
	}
	/**
	 * Events left `authored` behind an expired delivery can never reach `delivered` (#94).
	 * Move the batch's still-authored events to `failed_no_retry` with evidence.
	 * Runs inside the caller's transaction and returns the failed event ids.
	 */
	monitorEventsFailExpiredDelivery(deliveryId: string, reason: string): string[] {
		const delivery = this.#database
			.query<{ turn_id: string; attempts: number; state: string }, [string]>(
				"SELECT turn_id, attempts, state FROM deliveries WHERE delivery_id = ?",
			)
			.get(deliveryId);
		if (delivery?.state !== "expired") return [];
		const events = this.#database
			.query<{ event_id: string }, [string]>(
				`SELECT event_id FROM monitor_events WHERE batch_id = ? AND stage = 'authored' AND ${REPLAYABLE_MONITOR}`,
			)
			.all(delivery.turn_id);
		for (const { event_id } of events) {
			this.monitorEventUpdate(event_id, "failed_no_retry");
			this.monitorFailureRecord(
				event_id,
				"delivery_expired",
				`delivery ${deliveryId} expired after ${delivery.attempts} attempts: ${reason}`,
			);
		}
		return events.map((row) => row.event_id);
	}
	/**
	 * Bumps the reclaim counter; returns the new count. Reconcile stops
	 * reclaiming an event once it exceeds MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS.
	 */
	monitorEventIncrementAttempts(eventId: string): number {
		this.#database
			.query("UPDATE monitor_events SET dispatch_attempts = dispatch_attempts + 1, updated_at = ? WHERE event_id = ?")
			.run(new Date().toISOString(), eventId);
		return (
			this.#database
				.query<{ n: number }, [string]>("SELECT dispatch_attempts AS n FROM monitor_events WHERE event_id = ?")
				.get(eventId)?.n ?? 0
		);
	}
	monitorEventDispatchAttempts(eventId: string): number {
		return (
			this.#database
				.query<{ dispatch_attempts: number }, [string]>(
					"SELECT dispatch_attempts FROM monitor_events WHERE event_id = ?",
				)
				.get(eventId)?.dispatch_attempts ?? 0
		);
	}
	/**
	 * `order` is explicit because listings want newest-first while recovery must replay in the
	 * order events fired; `rowid` breaks same-millisecond ties so both orders are deterministic.
	 */
	monitorEventRows(
		monitorId?: string,
		order: "newest" | "oldest" = "newest",
		includeQuarantined = false,
		limit?: number,
	): MonitorEventDbRow[] {
		const direction = order === "oldest" ? "ASC" : "DESC";
		const terms = [
			monitorId === undefined ? undefined : "monitor_id = ?",
			includeQuarantined ? undefined : REPLAYABLE_MONITOR,
		];
		const where = terms.filter((term) => term !== undefined).join(" AND ") || "1=1";
		const params: Array<string | number> = monitorId === undefined ? [] : [monitorId];
		if (limit !== undefined) params.push(limit);
		return this.#database
			.query<MonitorEventDbRow, Array<string | number>>(
				`SELECT * FROM monitor_events WHERE ${where} ORDER BY fired_at ${direction}, rowid ${direction}${limit === undefined ? "" : " LIMIT ?"}`,
			)
			.all(...params);
	}

	monitorEventGet(eventId: string, includeQuarantined = false): MonitorEventDbRow | undefined {
		return (
			this.#database
				.query<MonitorEventDbRow, [string]>(
					`SELECT * FROM monitor_events WHERE event_id = ?${includeQuarantined ? "" : ` AND ${REPLAYABLE_MONITOR}`}`,
				)
				.get(eventId) ?? undefined
		);
	}

	/** The named replayable events, newest first (the order `monitorEventRows()` yields). */
	monitorEventsByIds(eventIds: readonly string[]): MonitorEventDbRow[] {
		if (eventIds.length === 0) return [];
		return this.#database
			.query<MonitorEventDbRow, [string]>(
				`SELECT * FROM monitor_events WHERE event_id IN (SELECT value FROM json_each(?)) AND ${REPLAYABLE_MONITOR} ORDER BY fired_at DESC, rowid DESC`,
			)
			.all(JSON.stringify(eventIds));
	}

	/** One delivery's replayable events, newest first. */
	monitorEventsByBatch(batchId: string): MonitorEventDbRow[] {
		return this.#database
			.query<MonitorEventDbRow, [string]>(
				`SELECT * FROM monitor_events WHERE batch_id = ? AND ${REPLAYABLE_MONITOR} ORDER BY fired_at DESC, rowid DESC`,
			)
			.all(batchId);
	}

	/** Replayable events that have not reached a terminal stage, oldest first: the only rows reconcile can act on. */
	monitorEventsOpen(): MonitorEventDbRow[] {
		return this.#database
			.query<MonitorEventDbRow, []>(
				`SELECT * FROM monitor_events WHERE ${OPEN_MONITOR_EVENT} AND ${REPLAYABLE_MONITOR} ORDER BY fired_at, rowid`,
			)
			.all();
	}

	/**
	 * Deliveries that settled (confirmed or expired) while one of their events is
	 * still `authored`: the split state reconcile repairs. Driven from the
	 * `authored` rows, so cost follows the stranded set, not delivery history.
	 */
	monitorDeliveriesAwaitingSettlement(): Array<{ delivery_id: string; turn_id: string; state: string }> {
		return this.#database
			.query<{ delivery_id: string; turn_id: string; state: string }, []>(
				// CROSS JOIN pins the order: start from the (tiny) stranded `authored` batches, never from delivery history.
				`SELECT d.delivery_id, d.turn_id, d.state FROM (SELECT DISTINCT batch_id FROM monitor_events WHERE stage = 'authored' AND ${REPLAYABLE_MONITOR}) b CROSS JOIN deliveries d ON d.turn_id = b.batch_id WHERE d.state IN ('confirmed','expired')`,
			)
			.all();
	}

	/** Newest authored notes for one monitor, newest first. */
	monitorRecentAuthoredNotes(
		monitorId: string,
		limit: number,
	): Array<{ event_type: string; fired_at: string; note: string }> {
		return this.#database
			.query<{ event_type: string; fired_at: string; note: string }, [string, number]>(
				`SELECT monitor_events.event_type AS event_type, monitor_events.fired_at AS fired_at, authored_outputs.output_text AS note FROM monitor_events JOIN authored_outputs ON authored_outputs.event_id = monitor_events.event_id WHERE monitor_events.monitor_id = ? AND ${REPLAYABLE_MONITOR} ORDER BY monitor_events.fired_at DESC, monitor_events.rowid DESC LIMIT ?`,
			)
			.all(monitorId, limit);
	}
	authoredOutputCreate(eventId: string, outputText: string): void {
		this.#assertNotQuarantined("monitor", eventId);
		this.#database
			.query(
				"INSERT INTO authored_outputs (event_id, output_text, authored_at) VALUES (?, ?, ?) ON CONFLICT(event_id) DO UPDATE SET output_text = excluded.output_text, authored_at = excluded.authored_at",
			)
			.run(eventId, outputText, new Date().toISOString());
	}
	/**
	 * Records the procedure versions one event's authoring turn is given (issue
	 * #82). Lease-fenced like every dispatch write: a stale attempt cannot
	 * overwrite the version a newer attempt authored with.
	 */
	monitorEventFencedSetProcedure(eventId: string, leaseId: string, procedureJson: string, now = Date.now()): boolean {
		this.#assertNotQuarantined("monitor", eventId);
		return (
			this.#database
				.query(
					`UPDATE monitor_events SET procedure_json = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(procedureJson, eventId, leaseId, new Date(now).toISOString()).changes > 0
		);
	}
	authoredOutput(eventId: string): string | undefined {
		return this.#database
			.query<{ output_text: string }, [string]>("SELECT output_text FROM authored_outputs WHERE event_id = ?")
			.get(eventId)?.output_text;
	}
	/** Latest public-safe failure evidence for one event, if any. */
	monitorFailure(eventId: string): MonitorFailureRow | undefined {
		return (
			this.#database
				.query<MonitorFailureRow, [string]>(
					"SELECT event_id, code, detail, failed_at, protocol_reason, response_byte_length, response_entry_count FROM monitor_failures WHERE event_id = ? ORDER BY rowid DESC LIMIT 1",
				)
				.get(eventId) ?? undefined
		);
	}
	/**
	 * Persists bounded, public-safe dispatch evidence: a stable machine code plus
	 * a one-line detail that must never contain secrets or raw error bodies.
	 * Keeps the newest 20 rows per event and deletes stale rows so the table
	 * cannot grow without bound across retries.
	 */
	monitorFailureRecord(eventId: string, code: string, detail: string, options?: MonitorFailureOptions): void {
		// Runs inside the caller's transaction when one is open (dispatch failure
		// bookkeeping must be atomic with the stage transition); standalone otherwise.
		const protocolFailure = options?.protocolFailure;
		if (protocolFailure) {
			if (!(PROTOCOL_FAILURE_REASONS as readonly string[]).includes(protocolFailure.reason))
				throw new Error("invalid monitor protocol failure reason");
			if (!Number.isSafeInteger(protocolFailure.responseByteLength) || protocolFailure.responseByteLength < 0)
				throw new Error("invalid monitor protocol response byte length");
			if (
				protocolFailure.responseEntryCount !== null &&
				(!Number.isSafeInteger(protocolFailure.responseEntryCount) ||
					protocolFailure.responseEntryCount < 0 ||
					protocolFailure.responseEntryCount > protocolFailure.responseByteLength)
			)
				throw new Error("invalid monitor protocol response entry count");
		}
		const failedAt = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO monitor_failures (event_id, code, detail, failed_at, protocol_reason, response_byte_length, response_entry_count) VALUES (?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				eventId,
				code,
				detail.slice(0, 500),
				failedAt,
				protocolFailure?.reason ?? null,
				protocolFailure?.responseByteLength ?? null,
				protocolFailure?.responseEntryCount ?? null,
			);
		this.#database
			.query(
				"DELETE FROM monitor_failures WHERE event_id = ? AND rowid NOT IN (SELECT rowid FROM monitor_failures WHERE event_id = ? ORDER BY rowid DESC LIMIT 20)",
			)
			.run(eventId, eventId);
	}
	/** Public-safe protocol failure history and delivery recovery timing for an event. */
	monitorEventRecovery(eventId: string): MonitorEventRecovery | undefined {
		const event = this.#database
			.query<{ stage: string; updated_at: string; dispatch_attempts: number }, [string]>(
				"SELECT stage, updated_at, dispatch_attempts FROM monitor_events WHERE event_id = ?",
			)
			.get(eventId);
		if (!event) return undefined;
		const rows = this.#database
			.query<
				{
					protocol_reason: string | null;
					failed_at: string;
					response_byte_length: number | null;
					response_entry_count: number | null;
				},
				[string]
			>(
				"SELECT protocol_reason, failed_at, response_byte_length, response_entry_count FROM monitor_failures WHERE event_id = ? AND protocol_reason IS NOT NULL ORDER BY rowid ASC",
			)
			.all(eventId);
		const protocolFailures: MonitorProtocolFailureRecord[] = [];
		for (const row of rows) {
			if (
				!(PROTOCOL_FAILURE_REASONS as readonly string[]).includes(row.protocol_reason ?? "") ||
				!Number.isSafeInteger(row.response_byte_length) ||
				(row.response_byte_length ?? -1) < 0 ||
				(row.response_entry_count !== null &&
					(!Number.isSafeInteger(row.response_entry_count) ||
						row.response_entry_count < 0 ||
						row.response_entry_count > (row.response_byte_length ?? -1)))
			)
				continue;
			protocolFailures.push({
				reason: row.protocol_reason as ProtocolFailureReason,
				failedAt: row.failed_at,
				responseByteLength: row.response_byte_length as number,
				responseEntryCount: row.response_entry_count,
			});
		}
		const first = protocolFailures[0];
		if (!first) return undefined;
		const deliveredAt = event.stage === "delivered" ? event.updated_at : null;
		const failedAtMs = Date.parse(first.failedAt);
		const deliveredAtMs = deliveredAt === null ? Number.NaN : Date.parse(deliveredAt);
		const latency = deliveredAtMs - failedAtMs;
		return {
			protocolFailures,
			firstFailedAt: first.failedAt,
			deliveredAt,
			recoveryLatencyMs:
				Number.isFinite(failedAtMs) && Number.isFinite(deliveredAtMs) && Number.isSafeInteger(latency) && latency >= 0
					? latency
					: null,
			dispatchAttempts: event.dispatch_attempts + 1,
		};
	}
	monitorFailures(eventIds: readonly string[]): Map<string, MonitorFailureRow> {
		const rows = new Map<string, MonitorFailureRow>();
		for (const eventId of eventIds) {
			const row = this.monitorFailure(eventId);
			if (row) rows.set(eventId, row);
		}
		return rows;
	}
	/**
	 * Red-team blocker 2: the slot claim and the event admission commit in ONE
	 * transaction. A claim row whose event_id is NULL means the gateway fired
	 * the slot but died before submitting the event — reconcile admits it. A
	 * crash can therefore strand neither a claimed-but-unadmitted slot (this
	 * row shape makes it visible) nor a duplicate event (unique slot key).
	 * A slot skipped by the overlap policy is still claimed: it fired, and its
	 * `skipped` event row is the record of what happened to it.
	 */
	monitorSlotClaimWithEvent(row: {
		monitorId: string;
		slotAt: string;
		eventId: string;
		eventType: string;
		payloadJson: string;
		overlap?: "queue" | "skip";
	}): { admitted: false } | { admitted: true; skippedBy: string | undefined } {
		const now = new Date().toISOString();
		return this.withTransaction(() => {
			const claim = this.#database
				.query(
					"INSERT INTO monitor_slots (monitor_id, slot_at, event_id, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(monitor_id, slot_at) DO NOTHING",
				)
				.run(row.monitorId, row.slotAt, row.eventId, now).changes;
			if (claim === 0) return { admitted: false as const };
			const skippedBy = this.monitorEventCreate({
				eventId: row.eventId,
				monitorId: row.monitorId,
				eventType: row.eventType,
				payloadJson: row.payloadJson,
				firedAt: row.slotAt,
				...(row.overlap ? { overlap: row.overlap } : {}),
			});
			return { admitted: true as const, skippedBy };
		});
	}
	/** Test/recovery seam: move a monitor's creation instant (clamps catch-up). */
	monitorSetCreatedAt(monitorId: string, createdAt: string): void {
		this.#database.query("UPDATE monitors SET created_at = ? WHERE monitor_id = ?").run(createdAt, monitorId);
	}
	monitorSlotExists(monitorId: string, slotAt: string): boolean {
		const row = this.#database
			.query<{ n: number }, [string, string]>(
				"SELECT COUNT(*) AS n FROM monitor_slots WHERE monitor_id = ? AND slot_at = ?",
			)
			.get(monitorId, slotAt);
		return (row?.n ?? 0) > 0;
	}
	/** Durable boundary from the newest claimed slot or newest policy skip. */
	monitorCronCursor(monitorId: string): string | undefined {
		const claimed = this.#database
			.query<{ slot_at: string | null }, [string]>(
				"SELECT MAX(slot_at) AS slot_at FROM monitor_slots WHERE monitor_id = ?",
			)
			.get(monitorId)?.slot_at;
		const skipped = this.monitorCronState(monitorId)?.cursor;
		if (!claimed) return skipped;
		if (!skipped) return claimed;
		return Date.parse(skipped) > Date.parse(claimed) ? skipped : claimed;
	}
	/** Catch-up diagnostics; absent until this monitor has skipped at least one slot. */
	monitorCronState(monitorId: string): MonitorCronState | undefined {
		const raw = this.metaGet(monitorCronMetaKey(monitorId));
		return raw === undefined ? undefined : (JSON.parse(raw) as MonitorCronState);
	}
	/** Records policy refusals durably and advances the cursor so they are counted once. */
	monitorCronRecordSkip(
		monitorId: string,
		skip: { count: number; oldest: string; newest: string },
		recordedAt: string,
	): MonitorCronState {
		return this.withTransaction(() => {
			const previous = this.monitorCronState(monitorId);
			const state: MonitorCronState = {
				cursor: skip.newest,
				skippedTotal: (previous?.skippedTotal ?? 0) + skip.count,
				lastSkip: { ...skip, recordedAt },
			};
			this.metaSet(monitorCronMetaKey(monitorId), JSON.stringify(state));
			return state;
		});
	}
	/**
	 * One bounded batch of history retention. Only terminal rows are touched;
	 * quarantined subjects are skipped (their triggers abort deletes), pending
	 * steers keep their trigger row, and each monitor keeps its newest slot
	 * because `monitorCronCursor` derives the cron schedule boundary from it.
	 */
	retentionSweep(now = new Date(), batch = RETENTION_BATCH_ROWS): RetentionSweepResult {
		const historyCutoff = new Date(now.getTime() - HISTORY_RETENTION_MS).toISOString();
		const deliveryCutoff = new Date(now.getTime() - CONFIRMED_DELIVERY_RETENTION_MS).toISOString();
		return this.withTransaction(() => {
			const deleted: Record<string, number> = {};
			let more = false;
			// Derive retained report IDs from durable task keys, not runtime JSON.
			// Corrupt or missing runtime evidence cannot drop debt or block unrelated
			// history. json_each avoids one SQL parameter per retained task.
			const taskReportIds = this.#database
				.query<{ job_id: string; op_ref: string }, []>("SELECT job_id, op_ref FROM work_tasks")
				.all()
				.map((task) => workAttemptReportId(this.instanceId, task.job_id, task.op_ref));
			const note = (table: string, count: number) => {
				deleted[table] = count;
				if (count >= batch) more = true;
			};
			note(
				"inbound_messages",
				this.#database
					.query(
						`DELETE FROM inbound_messages WHERE rowid IN (SELECT rowid FROM inbound_messages WHERE state = 'done' AND received_at < ? AND ${REPLAYABLE_INBOUND} AND ${UNRETAINED_TASK_INBOUND} AND NOT EXISTS (SELECT 1 FROM inbound_messages s WHERE s.turn_op_ref = inbound_messages.turn_op_ref AND s.state <> 'done') LIMIT ?)`,
					)
					.run(historyCutoff, JSON.stringify(taskReportIds), batch).changes,
			);
			const events = this.#database
				.query<{ event_id: string }, [string, number]>(
					`SELECT event_id FROM monitor_events WHERE ${TERMINAL_MONITOR_EVENT} AND updated_at < ? AND ${REPLAYABLE_MONITOR} ORDER BY updated_at LIMIT ?`,
				)
				.all(historyCutoff, batch);
			const dropOutput = this.#database.query("DELETE FROM authored_outputs WHERE event_id = ?");
			const dropFailures = this.#database.query("DELETE FROM monitor_failures WHERE event_id = ?");
			const dropLease = this.#database.query("DELETE FROM dispatch_leases WHERE event_id = ?");
			const dropEvent = this.#database.query("DELETE FROM monitor_events WHERE event_id = ?");
			for (const { event_id } of events) {
				dropOutput.run(event_id);
				dropFailures.run(event_id);
				dropLease.run(event_id);
				dropEvent.run(event_id);
			}
			note("monitor_events", events.length);
			note(
				"deliveries",
				this.#database
					.query(
						`DELETE FROM deliveries WHERE rowid IN (SELECT rowid FROM deliveries WHERE ((state = 'confirmed' AND updated_at < ?) OR (state = 'expired' AND updated_at < ?)) AND ${UNRETAINED_TASK_DELIVERY} LIMIT ?)`,
					)
					.run(deliveryCutoff, historyCutoff, batch).changes,
			);
			note(
				"lane_reports",
				this.#database
					.query(
						"DELETE FROM lane_reports WHERE rowid IN (SELECT rowid FROM lane_reports WHERE state IN ('consumed','fallback','undeliverable') AND updated_at < ? AND NOT EXISTS (SELECT 1 FROM work_tasks t WHERE t.op_ref = lane_reports.child_op_ref) LIMIT ?)",
					)
					.run(historyCutoff, batch).changes,
			);
			note(
				"monitor_slots",
				this.#database
					.query(
						"DELETE FROM monitor_slots WHERE rowid IN (SELECT t.rowid FROM monitor_slots t WHERE t.slot_at < ? AND t.slot_at < (SELECT MAX(m.slot_at) FROM monitor_slots m WHERE m.monitor_id = t.monitor_id) LIMIT ?)",
					)
					.run(historyCutoff, batch).changes,
			);
			return { deleted, more };
		}) as RetentionSweepResult;
	}

	/** Post-sweep upkeep: fold the WAL back (PASSIVE never blocks writers) and refresh planner stats. */
	maintain(): void {
		this.#database.exec("PRAGMA wal_checkpoint(PASSIVE); PRAGMA optimize;");
	}

	deliveryPrune(before: string): number {
		return this.#database
			.query(`DELETE FROM deliveries WHERE state = 'confirmed' AND updated_at < ? AND ${UNRETAINED_TASK_DELIVERY}`)
			.run(before).changes;
	}

	/**
	 * All writes pass through this single-writer boundary. Callbacks must be
	 * synchronous and must never perform external I/O; nested transactions fail.
	 */
	withTransaction<T>(work: () => T): T {
		if (this.#inTransaction) throw new Error("nested database transactions are forbidden");
		this.#inTransaction = true;
		try {
			this.#database.exec("BEGIN IMMEDIATE");
			const result = work();
			this.#database.exec("COMMIT");
			return result;
		} catch (error) {
			this.#database.exec("ROLLBACK");
			throw error;
		} finally {
			this.#inTransaction = false;
		}
	}

	backupInto(path: string): void {
		this.#database.query("VACUUM INTO ?").run(path);
	}

	integrityCheckDetail(): string {
		return (
			this.#database.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check ??
			"unknown"
		);
	}

	close(): void {
		this.#database.close();
	}

	private migrate(): void {
		// schema_migrations and broker_authority tables are created in open() before this method is called.
		const current = this.schemaVersion;
		if (current > LATEST_SCHEMA_VERSION) {
			throw new DatabaseStartupError(
				"newer_schema",
				`database schema ${current} is newer than supported schema ${LATEST_SCHEMA_VERSION}`,
			);
		}
		if (current < 1) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE sessions (origin_key TEXT PRIMARY KEY, gjc_session_id TEXT NOT NULL, created_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(1, new Date().toISOString());
			});
		}
		if (current < 2) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL, origin_key TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','inflight','confirmed','failed_ambiguous','expired')), attempts INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(2, new Date().toISOString());
			});
		}

		if (current < 3) {
			this.withTransaction(() => {
				this.#database.exec(
					"ALTER TABLE sessions ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0; ALTER TABLE sessions ADD COLUMN last_activity_at TEXT; ALTER TABLE sessions ADD COLUMN origin_ref_json TEXT; CREATE TABLE recall_snippets (id INTEGER PRIMARY KEY AUTOINCREMENT, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, text TEXT NOT NULL, at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(3, new Date().toISOString());
			});
		}

		if (current < 4) {
			this.withTransaction(() => {
				this.#database.exec("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
				this.#database.query("INSERT INTO meta (key, value) VALUES ('instance_id', ?)").run(crypto.randomUUID());
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(4, new Date().toISOString());
			});
		}
		if (current < 5) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE memory_intents (id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','written','committed','receipted','quarantined')), created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(5, new Date().toISOString());
			});
		}
		if (current < 6) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE monitors (monitor_id TEXT PRIMARY KEY, name TEXT NOT NULL, trigger_json TEXT NOT NULL, event_types_json TEXT NOT NULL, burst_policy TEXT NOT NULL, channel_target_json TEXT, enabled INTEGER NOT NULL, created_at TEXT NOT NULL); CREATE TABLE monitor_events (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','failed')), batch_id TEXT, updated_at TEXT NOT NULL); CREATE TABLE authored_outputs (event_id TEXT PRIMARY KEY, output_text TEXT NOT NULL, authored_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(6, new Date().toISOString());
			});
		}
		if (current < 7) {
			this.withTransaction(() => {
				// Inbound messages must be durable before dispatch: a message arriving while a turn is
				// in flight was previously processed transiently and lost outright.
				this.#database.exec(
					"CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL); CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(7, new Date().toISOString());
			});
		}
		if (current < 8) {
			this.withTransaction(() => {
				// Declined messages are still context, never commands: every inbound platform
				// message lands here so an engaged turn can read the unread diff since the
				// persona's last reply in that conversation.
				this.#database.exec(
					"CREATE TABLE conversation_context (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, author_id TEXT, author_name TEXT, body TEXT NOT NULL, received_at TEXT NOT NULL, consumed_at TEXT); CREATE INDEX conversation_context_unread ON conversation_context (origin_key, consumed_at, received_at)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(8, new Date().toISOString());
			});
		}
		if (current < 9) {
			this.withTransaction(() => {
				// v9 retained an observational monitor turn count. Persistent SDK sessions
				// now rely on native compaction; no gateway turn ceiling consumes this column.
				this.#database.exec("ALTER TABLE sessions ADD COLUMN turn_count INTEGER NOT NULL DEFAULT 0");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(9, new Date().toISOString());
			});
		}
		if (current < 10) {
			this.withTransaction(() => {
				// Issue #10: durable lane jobs. One row per job; the record JSON is the
				// fail-closed authority (schema-validated on read by @gajae-gateway/subsession),
				// while the status column stays a plain indexed projection for operators.
				this.#database.exec(
					"CREATE TABLE lane_jobs (job_id TEXT PRIMARY KEY, lane_key TEXT NOT NULL UNIQUE, branch TEXT NOT NULL, worktree_path TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','stalled','done','aborted')), record_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(10, new Date().toISOString());
			});
		}
		if (current < 11) {
			this.withTransaction(() => {
				// Monitor recovery (issue #29): extend the monitor event state contract,
				// preserve every legacy row, and add bounded operator evidence/slot tables.
				// Existing schema-10 lane_jobs deployments reach this step without loss.
				this.#database.exec(
					`CREATE TABLE monitor_events_new (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
INSERT INTO monitor_events_new (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, 0, updated_at FROM monitor_events;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_new RENAME TO monitor_events;
CREATE TABLE monitor_failures (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL, failed_at TEXT NOT NULL);
CREATE INDEX monitor_failures_event ON monitor_failures (event_id, failed_at);
CREATE TABLE monitor_slots (monitor_id TEXT NOT NULL, slot_at TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (monitor_id, slot_at));`,
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(11, new Date().toISOString());
			});
		}
		if (current < 12) {
			this.withTransaction(() => {
				// Durable dispatch ownership and atomic scheduled-slot admission.
				this.#database.exec(
					`CREATE TABLE dispatch_leases (event_id TEXT PRIMARY KEY, owner TEXT NOT NULL, lease_id TEXT NOT NULL, acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX dispatch_leases_expiry ON dispatch_leases (expires_at);
ALTER TABLE monitor_slots ADD COLUMN event_id TEXT;`,
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(12, new Date().toISOString());
			});
		}
		if (current < 13) {
			this.withTransaction(() => {
				// Issue #35: durable reset floors plus aggregate-only omission evidence.
				// Existing context rows remain intact and unread until the bounded policy
				// classifies them; lane jobs, monitor tables, and meta counters are untouched.
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS conversation_context_state (origin_key TEXT PRIMARY KEY, floor_at TEXT, floor_row_id INTEGER NOT NULL DEFAULT 0, expired_count INTEGER NOT NULL DEFAULT 0, truncated_count INTEGER NOT NULL DEFAULT 0, omitted_oldest_at TEXT, omitted_newest_at TEXT, last_omitted_at TEXT, pending_expired_count INTEGER NOT NULL DEFAULT 0, pending_truncated_count INTEGER NOT NULL DEFAULT 0, pending_omitted_oldest_at TEXT, pending_omitted_newest_at TEXT, omission_revision INTEGER NOT NULL DEFAULT 0)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(13, new Date().toISOString());
			});
		}
		if (current < 14) {
			this.withTransaction(() => {
				// Issue #37: durable once-per-origin-epoch bootstrap projection. Pending
				// state is the monotonic inequality epoch > last_bootstrapped_epoch, so
				// every existing and newly bumped epoch starts pending without a second
				// state machine that can drift from sessions. Bodies are never persisted.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(sessions)")
						.all()
						.map((row) => row.name),
				);
				const additions = [
					["last_bootstrapped_epoch", "INTEGER NOT NULL DEFAULT -1"],
					["bootstrap_applied_at", "TEXT"],
					["bootstrap_sections_json", "TEXT NOT NULL DEFAULT '[]'"],
					["bootstrap_byte_count", "INTEGER NOT NULL DEFAULT 0"],
					["bootstrap_truncated", "INTEGER NOT NULL DEFAULT 0 CHECK(bootstrap_truncated IN (0,1))"],
					["bootstrap_diagnostics_json", "TEXT NOT NULL DEFAULT '[]'"],
				] as const;
				for (const [name, declaration] of additions)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE sessions ADD COLUMN ${name} ${declaration}`);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(14, new Date().toISOString());
			});
		}
		if (current < 15) {
			this.withTransaction(() => {
				// Per-monitor authoring instruction. Nullable additive column: every
				// existing monitor keeps firing with no instruction and the authoring
				// prompt falls back to the built-in maintenance guidance.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(monitors)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("instruction")) this.#database.exec("ALTER TABLE monitors ADD COLUMN instruction TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(15, new Date().toISOString());
			});
		}
		if (current < 16) {
			this.withTransaction(() => {
				// Issue #20: per-conversation model override. This lives beside session
				// state rather than in config.json, because config is the owner's file
				// (a slash command must not race their editor) and `model` is not a
				// reloadable field, so a config write would not be read until restart.
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS conversation_model (origin_key TEXT PRIMARY KEY, selection_json TEXT NOT NULL, set_by TEXT, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(16, new Date().toISOString());
			});
		}
		if (current < 17) {
			this.withTransaction(() => {
				// Issue #92: lifecycle state and its durable session/cursor binding are
				// additive. Do not rewrite the legacy state column or its v16 CHECK.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
						.all()
						.map((row) => row.name),
				);
				const additions = [
					["batch_key", "TEXT"],
					["batch_role", "TEXT CHECK(batch_role IS NULL OR batch_role IN ('trigger', 'member', 'steer'))"],
					["batch_epoch", "INTEGER"],
					["batch_state", "TEXT CHECK(batch_state IS NULL OR batch_state IN ('settled', 'accepted', 'done'))"],
					["attributed_op_ref", "TEXT"],
					["accepted_at", "TEXT"],
					["bound_session_id", "TEXT"],
				] as const;
				for (const [name, declaration] of additions)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
				this.#database.exec(
					"CREATE UNIQUE INDEX IF NOT EXISTS inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, batch_epoch) WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted')",
				);
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS session_tail_cursors (session_id TEXT PRIMARY KEY, cursor TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database.exec(
					"UPDATE inbound_messages SET bound_session_id = (SELECT gjc_session_id FROM sessions WHERE sessions.origin_key = inbound_messages.origin_key AND sessions.epoch = inbound_messages.batch_epoch AND sessions.gjc_session_id <> '') WHERE bound_session_id IS NULL AND batch_state IN ('settled', 'accepted')",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(17, new Date().toISOString());
			});
		}
		if (current < 18) {
			this.withTransaction(() => {
				// dispatched_at: wall-clock stamped at bind time, BEFORE the send, so a
				// tail-less reconcile has a transcript floor that can never postdate
				// the answer (accepted_at is stamped after the CLI returns and can).
				// terminal_delivery_id: which ledger delivery satisfied this batch's
				// one terminal reply slot; a later terminal for the same batch is a
				// no-op regardless of text.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
						.all()
						.map((row) => row.name),
				);
				for (const [name, declaration] of [
					["dispatched_at", "TEXT"],
					["terminal_delivery_id", "TEXT"],
				] as const)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(18, new Date().toISOString());
			});
		}
		if (current < 19) {
			this.withTransaction(() => {
				// The batch model is gone: no settle window, no coalesced members, no
				// expiry. One trigger row = one turn; steers attach to the running
				// turn's op-ref. Rebuilt table because SQLite cannot drop CHECKed
				// columns in place. Preserved per row: bound_session_id (reattach
				// after restart), dispatched_at (the attribution floor) and
				// terminal_delivery_id (restart-safe per-part terminal claim).
				// A v18 nonterminal batch maps to a nonterminal turn on its trigger.
				// Its `member` rows were part of that batch's ONE prompt: members of
				// an accepted batch (the runtime holds the op, they were sent) become
				// done input attributed to the turn, like steers. Members of a
				// settled+bound batch sit in the send/ack crash window - the prompt
				// may or may not have reached the runtime - so they ride with the
				// trigger as pending `steer`/`bound` rows and are decided WITH it by
				// recovery (accepted -> done input; send proven absent -> released
				// to plain pending together). Members of a settled+unbound batch
				// were never sent and go back to plain pending. Nothing is deleted.
				// Legacy `processing` rows (the pre-actor claim path, whose startup
				// normalisation is gone) return to pending.
				this.#database.exec(
					"CREATE TABLE inbound_messages_v19 (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL, turn_role TEXT CHECK(turn_role IS NULL OR turn_role IN ('trigger', 'steer')), turn_epoch INTEGER, turn_state TEXT CHECK(turn_state IS NULL OR turn_state IN ('bound', 'accepted', 'done')), turn_op_ref TEXT, bound_session_id TEXT, dispatched_at TEXT, terminal_delivery_id TEXT)",
				);
				this.#database.exec(
					"INSERT INTO inbound_messages_v19 (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at) SELECT message_id, origin_key, origin_ref_json, body, engagement_json, CASE state WHEN 'processing' THEN 'pending' ELSE state END, received_at FROM inbound_messages",
				);
				// Triggers that were bound (session chosen) keep their turn; an unbound
				// settled trigger never had an operation and returns to plain pending.
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'trigger', turn_epoch = src.batch_epoch, turn_state = CASE src.batch_state WHEN 'settled' THEN 'bound' ELSE src.batch_state END, turn_op_ref = src.attributed_op_ref, bound_session_id = src.bound_session_id, dispatched_at = src.dispatched_at, terminal_delivery_id = src.terminal_delivery_id FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'trigger' AND src.attributed_op_ref IS NOT NULL AND (src.batch_state <> 'settled' OR src.bound_session_id IS NOT NULL)",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'steer'",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET state = 'done', turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state IN ('accepted', 'done')",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'bound', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state = 'settled' AND src.bound_session_id IS NOT NULL AND src.state = 'pending'",
				);
				this.#database.exec("DROP TABLE inbound_messages");
				this.#database.exec("ALTER TABLE inbound_messages_v19 RENAME TO inbound_messages");
				this.#database.exec("CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)");
				this.#database.exec(
					"CREATE UNIQUE INDEX inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, turn_epoch) WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
				);
				this.#database.exec(
					"CREATE UNIQUE INDEX inbound_messages_turn_trigger ON inbound_messages (turn_op_ref) WHERE turn_role = 'trigger'",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(19, new Date().toISOString());
			});
		}
		if (current < 20) {
			this.withTransaction(() => {
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(monitors)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("model_json")) this.#database.exec("ALTER TABLE monitors ADD COLUMN model_json TEXT");
				if (!columns.has("service_tier")) this.#database.exec("ALTER TABLE monitors ADD COLUMN service_tier TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(20, new Date().toISOString());
			});
		}
		if (current < 21) {
			this.withTransaction(() => {
				// Existing lane history is preserved verbatim. Recovery explicitly adopts
				// historical open attempts without inventing a target or dispatch proof.
				this.#database.exec(`CREATE TABLE work_attempt_runtime (
					op_ref TEXT PRIMARY KEY, job_id TEXT NOT NULL, lane_key TEXT NOT NULL,
					session_id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version >= 0),
					settled_at TEXT, delivery_id TEXT NOT NULL UNIQUE,
					record_json TEXT NOT NULL CHECK(length(record_json) <= 16384));
					CREATE UNIQUE INDEX work_attempt_open_lane ON work_attempt_runtime(lane_key) WHERE settled_at IS NULL;`);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(21, new Date().toISOString());
			});
		}
		if (current < 22) {
			this.withTransaction(() => {
				this.#database.exec(`CREATE TABLE IF NOT EXISTS broker_authority (
					singleton INTEGER PRIMARY KEY CHECK(singleton = 1), authority_key TEXT NOT NULL);
					CREATE TABLE broker_owned_bindings (
						authority_key TEXT NOT NULL, session_id TEXT NOT NULL, origin_key TEXT NOT NULL,
						epoch INTEGER NOT NULL CHECK(epoch >= 0), repo TEXT NOT NULL, created_at TEXT NOT NULL,
						PRIMARY KEY(authority_key, session_id), UNIQUE(authority_key, origin_key, epoch));
					CREATE TABLE broker_tail_cursors (
						authority_key TEXT NOT NULL, session_id TEXT NOT NULL, cursor TEXT NOT NULL, updated_at TEXT NOT NULL,
						PRIMARY KEY(authority_key, session_id));
					CREATE TABLE broker_cutovers (
						id TEXT PRIMARY KEY, old_authority TEXT, target_authority TEXT NOT NULL UNIQUE,
						evidence TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('quiescent','quarantine')),
						snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
					CREATE TABLE broker_quarantine (
						kind TEXT NOT NULL CHECK(kind IN ('inbound','work','monitor')), subject_id TEXT NOT NULL,
						cutover_id TEXT NOT NULL, PRIMARY KEY(kind, subject_id));
					CREATE TABLE broker_retired_sessions (session_id TEXT PRIMARY KEY, cutover_id TEXT NOT NULL);`);
				for (const table of [
					"broker_owned_bindings",
					"broker_cutovers",
					"broker_quarantine",
					"broker_retired_sessions",
				]) {
					for (const action of ["UPDATE", "DELETE"])
						this.#database.exec(
							`CREATE TRIGGER ${table}_immutable_${action.toLowerCase()} BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'immutable broker provenance'); END`,
						);
				}
				for (const [kind, table, column] of [
					["inbound", "inbound_messages", "message_id"],
					["work", "lane_jobs", "job_id"],
					["work", "work_attempt_runtime", "job_id"],
					["monitor", "monitor_events", "event_id"],
					["monitor", "authored_outputs", "event_id"],
				] as const) {
					for (const action of ["UPDATE", "DELETE"])
						this.#database.exec(`CREATE TRIGGER ${table}_quarantine_${action.toLowerCase()} BEFORE ${action} ON ${table}
						WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = '${kind}' AND subject_id = OLD.${column})
						BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END`);
				}
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(22, new Date().toISOString());
			});
		}
		if (current < 23) {
			this.withTransaction(() => {
				this.#database.exec(
					"ALTER TABLE memory_intents ADD COLUMN quarantine_reason TEXT; ALTER TABLE memory_intents ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0 AND typeof(attempts) = 'integer')",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(23, new Date().toISOString());
			});
		}
		if (current < 24) {
			this.withTransaction(() => {
				this.#database.exec(`
					ALTER TABLE inbound_messages ADD COLUMN source TEXT NOT NULL DEFAULT 'platform' CHECK(source IN ('platform','lane_report'));
					CREATE TABLE lane_reports (
						report_id TEXT PRIMARY KEY,
						parent_name TEXT NOT NULL,
						child_name TEXT NOT NULL,
						child_op_ref TEXT NOT NULL UNIQUE,
						body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) <= 2048),
						root_json TEXT,
						state TEXT NOT NULL CHECK(state IN ('pending','claimed','consumed','fallback','held','undeliverable')),
						claim_kind TEXT CHECK(claim_kind IS NULL OR claim_kind IN ('steer','wake')),
						claim_ref TEXT,
						claim_target_op_ref TEXT,
						claim_seq INTEGER NOT NULL DEFAULT 0 CHECK(claim_seq >= 0),
						hold_reason TEXT,
						consumed_op_ref TEXT,
						created_at TEXT NOT NULL,
						updated_at TEXT NOT NULL,
						CHECK(state NOT IN ('claimed','held') OR (claim_kind IS NOT NULL AND claim_ref IS NOT NULL)),
						CHECK(state <> 'pending' OR (claim_kind IS NULL AND claim_ref IS NULL AND claim_target_op_ref IS NULL)),
						CHECK(claim_kind IS NULL OR claim_seq > 0),
						CHECK(claim_kind IS NULL OR claim_kind <> 'steer' OR claim_target_op_ref IS NOT NULL),
						CHECK(claim_kind IS NULL OR claim_kind <> 'wake' OR claim_target_op_ref IS NULL),
						CHECK((state = 'consumed' AND consumed_op_ref IS NOT NULL) OR (state <> 'consumed' AND consumed_op_ref IS NULL))
					);
					CREATE INDEX lane_reports_parent_state ON lane_reports(parent_name, state, created_at);
					DROP TRIGGER IF EXISTS work_attempt_runtime_quarantine_update;
					DROP TRIGGER IF EXISTS work_attempt_runtime_quarantine_delete;
				`);
				for (const row of this.#database
					.query<{ op_ref: string; job_id: string; record_json: string }, []>(
						"SELECT op_ref, job_id, record_json FROM work_attempt_runtime",
					)
					.all()) {
					try {
						const legacy = JSON.parse(row.record_json) as Record<string, unknown>;
						workAssert(legacy && typeof legacy === "object" && !Array.isArray(legacy));
						const target = legacy.target;
						let parent: WorkParent | null = null;
						if (target !== null) {
							validateOriginRef(target as OriginRef);
							const origin = structuredClone(target as OriginRef);
							parent = { kind: "persona", originKey: originKey(origin), origin };
						}
						delete legacy.target;
						legacy.parent = parent;
						legacy.reportId = workAttemptReportId(this.instanceId, row.job_id, row.op_ref);
						legacy.wakeReportId = null;
						legacy.noticeHash = null;
						if (legacy.decision === "enqueued") legacy.decision = "fallback";
						const runtime = legacy as unknown as WorkAttemptRuntime;
						validateWorkRuntime(runtime, this.instanceId);
						workAssert(runtime.opRef === row.op_ref && runtime.jobId === row.job_id);
						this.#database
							.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
							.run(JSON.stringify(runtime), row.op_ref);
					} catch {
						throw new DatabaseStartupError(
							"migration_corrupt",
							`schema v24 could not migrate work attempt ${row.op_ref}`,
						);
					}
				}
				this.#database.exec(`
					CREATE TRIGGER work_attempt_runtime_quarantine_update BEFORE UPDATE ON work_attempt_runtime
					WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
					BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
					CREATE TRIGGER work_attempt_runtime_quarantine_delete BEFORE DELETE ON work_attempt_runtime
					WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
					BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
				`);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(24, new Date().toISOString());
			});
		}
		if (current < 25) {
			this.withTransaction(() => {
				// Track each epoch's AGENTS.md baseline. Only the digest is persisted;
				// prompt content remains in the workspace and is re-read on each turn.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(sessions)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("agents_md_epoch"))
					this.#database.exec("ALTER TABLE sessions ADD COLUMN agents_md_epoch INTEGER NOT NULL DEFAULT -1");
				if (!columns.has("agents_md_digest"))
					this.#database.exec("ALTER TABLE sessions ADD COLUMN agents_md_digest TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(25, new Date().toISOString());
			});
		}
		if (current < 26) {
			this.withTransaction(() => {
				// Issue #171: the allowlisted classification of the latest failed attempt.
				const columns = this.#database
					.query<{ name: string }, []>("PRAGMA table_info(deliveries)")
					.all()
					.map((row) => row.name);
				if (!columns.includes("last_error")) this.#database.exec("ALTER TABLE deliveries ADD COLUMN last_error TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(26, new Date().toISOString());
			});
		}
		if (current < 27) {
			this.withTransaction(() => {
				// Issue #82: declared procedure files are re-read per firing and each
				// authoring event records the procedure version it was given.
				const columns = (table: string) =>
					new Set(
						this.#database
							.query<{ name: string }, []>(`PRAGMA table_info(${table})`)
							.all()
							.map((row) => row.name),
					);
				if (!columns("monitors").has("procedure_files_json"))
					this.#database.exec("ALTER TABLE monitors ADD COLUMN procedure_files_json TEXT");
				if (!columns("monitor_events").has("procedure_json"))
					this.#database.exec("ALTER TABLE monitor_events ADD COLUMN procedure_json TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(27, new Date().toISOString());
			});
		}
		if (current < 28) {
			this.withTransaction(() => {
				// Issue #83: per-monitor overlap policy and the terminal `skipped` stage.
				// Rebuild monitor_events because its stage CHECK constraint must widen.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(monitors)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("overlap"))
					this.#database.exec(
						"ALTER TABLE monitors ADD COLUMN overlap TEXT NOT NULL DEFAULT 'queue' CHECK(overlap IN ('queue','skip'))",
					);
				this.#database.exec(`CREATE TABLE monitor_events_v28 (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry','skipped')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, skipped_by TEXT, updated_at TEXT NOT NULL, procedure_json TEXT);
INSERT INTO monitor_events_v28 (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at, procedure_json) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at, procedure_json FROM monitor_events ORDER BY rowid;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_v28 RENAME TO monitor_events;
CREATE INDEX monitor_events_monitor_stage ON monitor_events (monitor_id, stage);`);
				for (const action of ["UPDATE", "DELETE"])
					this.#database.exec(`CREATE TRIGGER monitor_events_quarantine_${action.toLowerCase()} BEFORE ${action} ON monitor_events
					WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'monitor' AND subject_id = OLD.event_id)
					BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END`);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(28, new Date().toISOString());
			});
		}
		if (current < 29) {
			this.withTransaction(() => {
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(monitor_failures)")
						.all()
						.map((row) => row.name),
				);
				const additions = [
					[
						"protocol_reason",
						"TEXT CHECK(protocol_reason IS NULL OR protocol_reason IN ('protocol_response_not_array', 'protocol_entry_missing_field', 'protocol_unknown_event', 'protocol_duplicate_event', 'protocol_omitted_event', 'protocol_unparseable_json', 'protocol_off_contract'))",
					],
					[
						"response_byte_length",
						"INTEGER CHECK(response_byte_length IS NULL OR (typeof(response_byte_length) = 'integer' AND response_byte_length BETWEEN 0 AND 9007199254740991))",
					],
					[
						"response_entry_count",
						"INTEGER CHECK(response_entry_count IS NULL OR (typeof(response_entry_count) = 'integer' AND response_entry_count BETWEEN 0 AND 9007199254740991))",
					],
				] as const;
				for (const [name, declaration] of additions)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE monitor_failures ADD COLUMN ${name} ${declaration}`);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(29, new Date().toISOString());
			});
		}
		if (current < 30) {
			// Hot-path lookup indexes (deliveries by turn, monitor events by batch / open / authored)
			// and the age indexes behind the retention sweep. Predicates are spelled exactly as the
			// queries spell them so SQLite can prove they imply the partial index.
			this.withTransaction(() => {
				this.#database.exec(`
					CREATE INDEX IF NOT EXISTS inbound_messages_gc ON inbound_messages(received_at) WHERE state = 'done';
					CREATE INDEX IF NOT EXISTS deliveries_gc ON deliveries(state, updated_at);
					CREATE INDEX IF NOT EXISTS deliveries_turn ON deliveries(turn_id);
					CREATE INDEX IF NOT EXISTS monitor_events_gc ON monitor_events(updated_at) WHERE stage IN ('delivered','authored_no_delivery','failed_no_retry','skipped');
					CREATE INDEX IF NOT EXISTS monitor_events_open ON monitor_events(fired_at) WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped');
					CREATE INDEX IF NOT EXISTS monitor_events_batch ON monitor_events(batch_id);
					CREATE INDEX IF NOT EXISTS monitor_events_authored ON monitor_events(batch_id) WHERE stage = 'authored';
					CREATE INDEX IF NOT EXISTS monitor_events_monitor ON monitor_events(monitor_id, fired_at);
					CREATE INDEX IF NOT EXISTS lane_reports_gc ON lane_reports(updated_at) WHERE state IN ('consumed','fallback','undeliverable');
					CREATE INDEX IF NOT EXISTS monitor_slots_gc ON monitor_slots(slot_at);
					CREATE INDEX IF NOT EXISTS memory_intents_open ON memory_intents(created_at) WHERE state <> 'receipted';
				`);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(30, new Date().toISOString());
			});
		}
		if (current < 31) {
			// Migration 25 was edited after it shipped to add the AGENTS.md baseline
			// columns, so every database that recorded 25 before that edit is at a
			// version that claims the columns exist while they do not. Backfill them
			// here instead of re-running 25, which is already recorded.
			this.withTransaction(() => {
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(sessions)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("agents_md_epoch"))
					this.#database.exec("ALTER TABLE sessions ADD COLUMN agents_md_epoch INTEGER NOT NULL DEFAULT -1");
				if (!columns.has("agents_md_digest"))
					this.#database.exec("ALTER TABLE sessions ADD COLUMN agents_md_digest TEXT");
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(31, new Date().toISOString());
			});
		}
		if (current < 32) {
			// Issue #367: Add index to conversation_context (consumed_at, received_at) to support
			// efficient batched retention DELETE and unconsumed GROUP BY scan in contextMaintain().
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE INDEX IF NOT EXISTS conversation_context_retention ON conversation_context (consumed_at, received_at)",
				);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(32, new Date().toISOString());
			});
		}
		if (current < 33) {
			this.withTransaction(() => {
				this.#database.exec(`
					CREATE TABLE work_tasks (
						task_id TEXT PRIMARY KEY,
						lane_name TEXT NOT NULL UNIQUE,
						job_id TEXT NOT NULL UNIQUE,
						op_ref TEXT NOT NULL UNIQUE,
						thread_origin_key TEXT UNIQUE,
						surface_phase TEXT NOT NULL CHECK(surface_phase IN ('pending','claimed','bound','held')),
						dispatch_phase TEXT NOT NULL CHECK(dispatch_phase IN ('pending','prepared')),
						obligation_state TEXT NOT NULL CHECK(obligation_state IN ('awaiting_final','final_admitted','held')),
						version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version >= 0),
						record_json TEXT NOT NULL CHECK(json_valid(record_json)),
						CHECK((surface_phase = 'bound') = (thread_origin_key IS NOT NULL))
					);
					CREATE INDEX work_tasks_pending ON work_tasks(dispatch_phase, surface_phase, task_id);
					CREATE INDEX work_tasks_obligation ON work_tasks(obligation_state, task_id);
					CREATE TABLE work_controls (
						control_id TEXT PRIMARY KEY,
						task_id TEXT NOT NULL REFERENCES work_tasks(task_id),
						event_id TEXT NOT NULL UNIQUE,
						sequence INTEGER NOT NULL CHECK(typeof(sequence) = 'integer' AND sequence > 0),
						client_ref TEXT UNIQUE,
						notification_id TEXT NOT NULL UNIQUE,
						phase TEXT NOT NULL CHECK(phase IN ('pending','sending','accepted','refused','held')),
						version INTEGER NOT NULL CHECK(typeof(version) = 'integer' AND version >= 0),
						record_json TEXT NOT NULL CHECK(json_valid(record_json)),
						UNIQUE(task_id, sequence)
					);
					CREATE INDEX work_controls_pending ON work_controls(task_id, phase, sequence);
					CREATE TABLE work_task_sources (
						source_id TEXT PRIMARY KEY,
						task_id TEXT NOT NULL REFERENCES work_tasks(task_id),
						sequence INTEGER NOT NULL CHECK(typeof(sequence) = 'integer' AND sequence > 0),
						content_hash TEXT NOT NULL,
						delivery_id TEXT UNIQUE REFERENCES deliveries(delivery_id),
						record_json TEXT NOT NULL CHECK(json_valid(record_json)),
						UNIQUE(task_id, sequence)
					);
				`);
				this.#database
					.query("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
					.run(33, new Date().toISOString());
			});
		}
	}

	// --- Issue #20: per-conversation model override -------------------------

	/**
	 * Reads a conversation's model override. Returns undefined when none is set
	 * and, deliberately, also when the stored row fails to parse: a corrupt
	 * override must degrade to the configured default rather than propagate a
	 * malformed selector into a gjc spawn.
	 */
	conversationModelGet(originKey: string): ConversationModelRecord | undefined {
		const row = this.#database
			.query<{ selection_json: string; set_by: string | null; updated_at: string }, [string]>(
				"SELECT selection_json, set_by, updated_at FROM conversation_model WHERE origin_key = ?",
			)
			.get(originKey);
		if (!row) return undefined;
		let selection: unknown;
		try {
			selection = JSON.parse(row.selection_json);
		} catch {
			return undefined;
		}
		const parsed = parseModelSelection(selection);
		if (!parsed) return undefined;
		return {
			selection: parsed,
			...(row.set_by === null ? {} : { setBy: row.set_by }),
			updatedAt: row.updated_at,
		};
	}

	/** Writes (or replaces) a conversation's model override. */
	conversationModelSet(originKey: string, selection: GjcModelSelection, setBy?: string): void {
		this.#database
			.query(
				"INSERT OR REPLACE INTO conversation_model (origin_key, selection_json, set_by, updated_at) VALUES (?, ?, ?, ?)",
			)
			.run(originKey, JSON.stringify(selection), setBy ?? null, new Date().toISOString());
	}

	/** Drops a conversation's override. Reports whether a row was actually removed. */
	conversationModelClear(originKey: string): boolean {
		const before = this.#database
			.query<{ n: number }, [string]>("SELECT COUNT(*) AS n FROM conversation_model WHERE origin_key = ?")
			.get(originKey);
		this.#database.query("DELETE FROM conversation_model WHERE origin_key = ?").run(originKey);
		return (before?.n ?? 0) > 0;
	}
	// --- Issue #10: durable lane jobs ---------------------------------------

	/**
	 * Upserts a validated lane job record. Callers MUST pass an already
	 * schema-validated record (parseLaneJobRecord output); the database stores
	 * the JSON verbatim so reads can re-validate fail-closed.
	 */
	putLaneJob(job: {
		readonly jobId: string;
		readonly laneKey: string;
		readonly state: string;
		readonly createdAt: string;
		readonly updatedAt: string;
		readonly lane: { readonly branch: string; readonly worktreePath: string };
		readonly json: string;
	}): void {
		this.#assertNotQuarantined("work", job.jobId);
		this.#database
			.query(
				"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET lane_key = excluded.lane_key, branch = excluded.branch, worktree_path = excluded.worktree_path, state = excluded.state, record_json = excluded.record_json, updated_at = excluded.updated_at",
			)
			.run(
				job.jobId,
				job.laneKey,
				job.lane.branch,
				job.lane.worktreePath,
				job.state,
				job.json,
				job.createdAt,
				job.updatedAt,
			);
	}

	laneJobJson(jobId: string): string | undefined {
		return this.#database
			.query<{ record_json: string }, [string]>("SELECT record_json FROM lane_jobs WHERE job_id = ?")
			.get(jobId)?.record_json;
	}

	laneJobJsonByLaneKey(laneKey: string): string | undefined {
		return this.#database
			.query<{ record_json: string }, [string]>("SELECT record_json FROM lane_jobs WHERE lane_key = ?")
			.get(laneKey)?.record_json;
	}

	laneJobRows(includeQuarantined = false): Array<{
		job_id: string;
		lane_key: string;
		state: string;
		branch: string;
		worktree_path: string;
		updated_at: string;
	}> {
		return this.#database
			.query(
				`SELECT job_id, lane_key, state, branch, worktree_path, updated_at FROM lane_jobs WHERE ${includeQuarantined ? "1=1" : "NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'work' AND q.subject_id = lane_jobs.job_id)"} ORDER BY updated_at DESC`,
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

	get instanceId(): string {
		const row = this.#database
			.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?")
			.get("instance_id");
		if (!row) throw new DatabaseStartupError("integrity_check_failed", "meta.instance_id missing after migration");
		return row.value;
	}

	metaGet(key: string): string | undefined {
		return this.#database.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?").get(key)?.value;
	}

	metaSet(key: string, value: string): void {
		this.#database
			.query("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
			.run(key, value);
	}

	metaDelete(key: string): void {
		this.#database.query("DELETE FROM meta WHERE key = ?").run(key);
	}

	private integrityCheck(): void {
		const result = this.integrityCheckDetail();
		if (result !== "ok")
			throw new DatabaseStartupError("integrity_check_failed", `SQLite integrity_check failed: ${result}`);
	}
}

import { createHash } from "node:crypto";
import {
	type ChatMessagePayload,
	isSilentOutput,
	type OriginRef,
	originKey,
	type PromptStatusBody,
	ProtocolError,
	parseOriginKey,
	validateOriginRef,
	validateWorkStatusParams,
	validateWorkSteerParams,
	validateWorkTaskDispositionBasis,
	validateWorkTaskDispositionBasisParams,
	validateWorkTaskDispositionParams,
	validateWorkTaskSpec,
	validateWorkThreadBindParams,
	validateWorkThreadClaimParams,
	type WorkStartResult,
	type WorkStatusResult,
	type WorkSteerResult,
	type WorkTaskControlProjection,
	type WorkTaskDispositionBasisResult,
	type WorkTaskDispositionResult,
	type WorkTaskProjection,
	type WorkTaskRecoverResult,
	type WorkThreadBindResult,
	type WorkThreadClaimResult,
} from "@gajae-gateway/protocol";
import {
	acknowledgeHold,
	appendAttempt,
	applyReconciliation,
	closeAttempt,
	createLaneJobRecord,
	envelopeErrorCode,
	GjcCliError,
	hasNewCommit,
	type LaneJobRecord,
	newOpRef,
	OpRefRejectedError,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import type { GjcModelSelection } from "../config";
import { buildDeliveryPayload, isDiscordSnowflake } from "../delivery/delivery";
import {
	type GatewayDatabase,
	type LaneReportRow,
	type WorkAttemptAdmission,
	type WorkAttemptRuntime,
	type WorkAttemptSettleResult,
	WorkAttemptStateError,
	type WorkAttemptTerminalEvidence,
	type WorkControl,
	type WorkParent,
	type WorkReportRoot,
	type WorkTask,
	type WorkTaskEvidence,
	type WorkTaskQualifiedAdmissionScope,
	type WorkTaskSourceInput,
	workAttemptDeliveryId,
	workAttemptReportId,
	workTaskDispositionId,
	workTaskDispositionText,
	workTaskSourceDeliveryId,
} from "../store/db";
import { readFailedTransportCause } from "./failed-turn-evidence";
import {
	type LaneForceRetireReason,
	type LaneGovernor,
	laneJobIdentity,
	type TaskReleaseAssessment,
	workSessionKey,
} from "./lane-governor";
import { sanitizeDiagnostic } from "./rebind";
import { isSessionUnavailable, type SessionPort } from "./session-port";
import type { TailHandle } from "./tail-runner";
import { admitDedicatedWorktree, revalidateDedicatedWorktree } from "./worktree-admission";

const owners = new WeakSet<GatewayDatabase>();
/** Consecutive failed reconciliations between authority re-checks through recovery. */
const FAILURES_PER_READOPTION = 8;
/** Ceiling for the failure backoff between reconciliation polls. */
const MAX_FAILURE_BACKOFF_MS = 30_000;
const reasons = new Set([
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
/** Minimum spacing between streamed-frame activity writes for one attempt. */
const ACTIVITY_TOUCH_MS = 1_000;
const HOST_LOST_GRACE_MS = 30_000;
const refusalCodes = new Set([
	"busy",
	"steer_refused",
	"invalid_params",
	"not_running",
	"no_active_turn",
	"client_ref_conflict",
	"session_not_found",
]);
const pendingOutput = () => ({
	disposition: "pending" as const,
	reads: 0,
	nextReadAt: null,
	excerpt: null,
	proof: null,
	knownSilence: null,
});
function hasAcceptanceEvidence(runtime: WorkAttemptRuntime): boolean {
	return runtime.sendPhase === "accepted" || runtime.output.proof !== null || runtime.output.knownSilence !== null;
}
function workAttemptEndState(
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
interface Observer {
	readonly runtime: WorkAttemptRuntime;
	readonly generation: number;
	readonly abort: AbortController;
	readonly binding: { readonly sessionId: string | null; readonly epoch: number } | undefined;
	tail?: TailHandle;
	attaching?: Promise<void>;
	task?: Promise<void>;
	timer?: ReturnType<typeof setTimeout>;
	wakeRequested?: boolean;
	text?: string;
	touchedAt?: number;
	/** First of an unbroken run of host-gone verdicts; any other observation resets it. */
	goneSince?: number;
}
interface Waiter {
	readonly owner: object;
	readonly finish: (error?: Error) => void;
}
export interface WorkLaneManagerOptions {
	readonly database: GatewayDatabase;
	readonly port: SessionPort;
	readonly lanes: LaneGovernor;
	readonly ownerTarget?: () => OriginRef | undefined;
	readonly allowNested?: () => boolean;
	readonly personaHold?: (originKey: string) => string | undefined;
	readonly notifyPersona?: (originKey: string) => void;
	readonly deliverFallback?: (payload: ChatMessagePayload) => void;
	readonly brokerGeneration?: () => number;
	readonly pollMs?: number;
	readonly waitTimeoutMs?: number;
	/** How long consecutive host-gone verdicts must persist before a live observer settles `host_lost`. */
	readonly hostLostGraceMs?: number;
	readonly now?: () => number;
	/** Trusted server execution area, not caller-supplied routing data. */
	readonly coordinatorCwd?: (origin: OriginRef) => string | undefined;
	/** Adapter capability AND server active-persona guard; absence fails closed. */
	readonly taskSurfaceAvailable?: (origin: OriginRef, retainedAdmissionTaskId?: string) => boolean;
}
/** Server-authenticated context, deliberately separate from public work.start params. */
export interface WorkTaskAdmissionContext {
	readonly stableOrigin: OriginRef | null;
	readonly evidence: WorkTaskEvidence;
}
/** The server authenticates the author before calling this mapped-only entry. */
export interface MappedWorkEvent {
	readonly origin: OriginRef;
	readonly authorId: string;
	readonly eventId: string;
	readonly editId?: string;
	/** Original message creation time, never edit time or gateway arrival time. */
	readonly platformTimestamp: string | null;
	readonly body: string;
	readonly kind: "steer" | "cancel_request" | "reset_notice";
	readonly scope?: "read_only" | "code_mutating";
}
interface WorkInput {
	name: string;
	text: string;
	cwd: string;
	resume: boolean;
	model?: GjcModelSelection;
	callerSessionId?: string;
}
interface StartContext {
	readonly parent: WorkParent | null;
	readonly opRef?: string;
	readonly wakeReportId?: string;
}

/** Attempt ownership is independent of sockets, response deadlines and broker generations. */
export class WorkLaneManager {
	readonly #options: WorkLaneManagerOptions;
	readonly #db: GatewayDatabase;
	readonly #port: SessionPort;
	readonly #observers = new Map<string, Observer>();
	/** Consecutive reconciliation failures per open attempt; cleared by a successful tick or settlement. */
	readonly #failures = new Map<string, { failures: number; reason: string }>();
	readonly #waiters = new Map<string, Set<Waiter>>();
	readonly #detachedOwners = new WeakSet<object>();
	#generationRecovery?: Promise<void>;
	#recovery?: Promise<void>;
	#stopped = false;
	#stopPromise?: Promise<void>;
	constructor(options: WorkLaneManagerOptions) {
		if (owners.has(options.database)) throw new Error("work manager already registered");
		owners.add(options.database);
		this.#options = options;
		this.#db = options.database;
		this.#port = options.port;
		options.lanes.setRecoveryGate(() => this.recover());
		options.lanes.setForceRetireSettlement((name, opRef, reason, endedAt) =>
			this.settleForceRetiredAttempt(name, opRef, reason, endedAt),
		);
		options.lanes.setTaskReleaseAssessment((name) => this.assessTaskRelease(name));
	}
	#now(): number {
		return this.#options.now?.() ?? Date.now();
	}
	#at(): string {
		return new Date(this.#now()).toISOString();
	}
	#live(): void {
		if (this.#stopped) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
	}
	#binding(runtime: WorkAttemptRuntime): boolean {
		const binding = this.#db.getSessionRecord(runtime.sessionKey);
		return binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch;
	}
	#current(observer: Observer): boolean {
		return (
			!this.#stopped &&
			!this.#db.isBrokerQuarantined("work", observer.runtime.jobId) &&
			!observer.abort.signal.aborted &&
			this.#observers.get(observer.runtime.opRef) === observer &&
			observer.generation === (this.#options.brokerGeneration?.() ?? 0)
		);
	}
	#writeCurrent(observer: Observer): boolean {
		if (!this.#current(observer)) return false;
		const binding = this.#db.getSessionRecord(observer.runtime.sessionKey);
		return (
			this.#current(observer) &&
			binding?.sessionId === observer.binding?.sessionId &&
			binding?.epoch === observer.binding?.epoch
		);
	}
	#job(name: string, required = false): LaneJobRecord | undefined {
		const { jobId } = laneJobIdentity(name);
		this.#assertNotQuarantined(jobId, name);
		try {
			const { jobId, laneKey } = laneJobIdentity(name);
			const byId = this.#db.laneJobJson(jobId);
			const byLane = this.#db.laneJobJsonByLaneKey(laneKey);
			if (byId !== byLane) throw new Error("identity mismatch");
			if (byId === undefined) {
				if (required)
					throw new ProtocolError("invalid_params", "unknown work lane", { reasonCode: "unknown_work_lane", name });
				return undefined;
			}
			const job = parseLaneJobRecord(byId);
			if (job.jobId !== jobId) throw new Error("identity mismatch");
			const last = job.attempts.at(-1);
			const saved = last && this.#db.workAttemptGet(last.opRef);
			if (
				last &&
				saved &&
				(saved.jobId !== jobId ||
					saved.laneKey !== laneKey ||
					saved.cwd !== job.lane.worktreePath ||
					saved.sessionId !== last.sessionId ||
					saved.startedAt !== last.startedAt ||
					saved.settledAt !== (last.endedAt ?? null))
			)
				throw new Error("runtime/history mismatch");
			const open = this.#db.workAttemptOpenByLane(laneKey);
			if (open && (job.attempts.at(-1)?.opRef !== open.opRef || job.attempts.at(-1)?.endedAt !== undefined))
				throw new Error("runtime mismatch");
			return job;
		} catch (error) {
			if (error instanceof ProtocolError) throw error;
			throw new ProtocolError("verb_failed", "work lane state unavailable", { reasonCode: "lane_state_corrupt", name });
		}
	}
	/**
	 * Closes a dead lane's current attempt through the normal atomic settlement
	 * path. LaneGovernor calls this while it owns the lane mutation lock.
	 */
	settleForceRetiredAttempt(name: string, opRef: string, reason: LaneForceRetireReason, endedAt: string): boolean {
		const record = this.#job(name, true);
		if (!record) return false;
		const attempt = record.attempts.at(-1);
		if (!attempt || attempt.opRef !== opRef) return false;
		const runtime = this.#db.workAttemptGet(opRef);
		if (!runtime) return false;
		if (runtime.settledAt !== null) return attempt.endedAt === runtime.settledAt;
		if (attempt.endedAt !== undefined) return false;

		const recordedTerminal = runtime.terminal;
		const terminal: WorkAttemptTerminalEvidence = recordedTerminal ?? {
			kind: "local",
			observedAt: endedAt,
			reasonCode: reason,
		};
		const endState = recordedTerminal ? workAttemptEndState(terminal.reasonCode) : "attempt_ended";
		const errorCode = recordedTerminal
			? terminal.reasonCode === "end_turn"
				? undefined
				: terminal.reasonCode
			: "host_lost";
		const output =
			runtime.output.disposition === "pending"
				? { ...runtime.output, disposition: "unavailable" as const, nextReadAt: null }
				: runtime.output;
		const closed = closeAttempt({
			record,
			opRef,
			endState,
			errorCode,
			endedAt,
		});
		const text = reportText(name, endState, terminal.reasonCode, opRef, output);
		let decision: "report" | "suppressed" | "no_target" | "wake_unaccepted";
		let admission: WorkAttemptAdmission | undefined;
		if (runtime.wakeReportId !== null && !hasAcceptanceEvidence(runtime)) {
			decision = "wake_unaccepted";
		} else if (runtime.output.knownSilence !== null) {
			decision = "suppressed";
		} else if (runtime.parent === null) {
			decision = "no_target";
		} else {
			decision = "report";
			if (runtime.parent.kind === "persona") {
				const fallbackPayload = buildDeliveryPayload(runtime.opRef, runtime.parent.origin, text, runtime.deliveryId);
				if (!fallbackPayload) return false;
				const holdReason = this.#options.personaHold?.(runtime.parent.originKey);
				admission = {
					kind: "persona",
					row: {
						messageId: runtime.reportId,
						originKey: runtime.parent.originKey,
						originRefJson: JSON.stringify(runtime.parent.origin),
						body: text,
						receivedAt: endedAt,
					},
					fallbackPayload,
					...(holdReason ? { holdReason } : {}),
				};
			} else {
				const root = runtime.parent.root;
				admission = {
					kind: "lane",
					report: {
						reportId: runtime.reportId,
						parentName: runtime.parent.name,
						childName: name,
						childOpRef: runtime.opRef,
						body: text,
						root,
					},
					fallbackPayload: root
						? (buildDeliveryPayload(runtime.opRef, root.origin, text, runtime.deliveryId) ?? null)
						: null,
				};
			}
		}
		const settled = this.#settleWithTaskResult(
			undefined,
			opRef,
			runtime.version,
			closed,
			{ decision, settledAt: endedAt, terminal, ...(output !== runtime.output ? { output } : {}) },
			admission,
		);
		if (!settled) return false;
		this.#finishWaiters(opRef);
		for (const payload of [settled.fallbackPayload, settled.childFallback]) {
			if (!payload) continue;
			try {
				this.#options.deliverFallback?.(payload);
			} catch {
				console.error(`work_fallback_delivery_failed deliveryId=${payload.deliveryId}`);
			}
		}
		if (settled.runtime.decision === "reported" && settled.runtime.parent?.kind === "persona") {
			try {
				this.#options.notifyPersona?.(settled.runtime.parent.originKey);
			} catch {
				console.error(`work_persona_nudge_failed origin=${settled.runtime.parent.originKey}`);
			}
		}
		const parents = new Set([name]);
		if (settled.runtime.parent?.kind === "lane") parents.add(settled.runtime.parent.name);
		for (const parentName of parents) {
			void this.#drainLaneReports(parentName).catch(() => {
				console.error(`lane_report_drain_failed parent=${parentName}`);
			});
		}
		return true;
	}
	#assertNotQuarantined(jobId: string, name?: string): void {
		if (this.#db.isBrokerQuarantined("work", jobId))
			throw new ProtocolError("verb_failed", "work lane belongs to a quarantined broker authority", {
				reasonCode: "broker_authority_quarantined",
				jobId,
				...(name === undefined ? {} : { name }),
			});
	}
	#latestRuntime(name: string): WorkAttemptRuntime | undefined {
		const opRef = this.#job(name)?.attempts.at(-1)?.opRef;
		return opRef ? this.#db.workAttemptGet(opRef) : undefined;
	}
	#rootForLane(name: string): WorkReportRoot | null {
		const parent = this.#latestRuntime(name)?.parent;
		if (parent?.kind === "persona") return { originKey: parent.originKey, origin: parent.origin };
		if (parent?.kind === "lane") return parent.root;
		return null;
	}
	#assertNoNestedCycle(childName: string, parentName: string): void {
		const visited = new Set<string>();
		let name = parentName;
		for (let hop = 0; hop < 16; hop++) {
			if (name === childName || visited.has(name))
				throw new ProtocolError("invalid_params", "nested work lane would create a parent cycle", {
					reasonCode: "nested_lane_cycle",
					name: childName,
					parent: name,
				});
			visited.add(name);
			const parent = this.#latestRuntime(name)?.parent;
			if (parent?.kind !== "lane") return;
			name = parent.name;
		}
		throw new ProtocolError("invalid_params", "nested work lane parent chain exceeds 16 hops", {
			reasonCode: "nested_lane_cycle",
			name: childName,
			parent: name,
		});
	}
	#resolveCaller(input: WorkInput, mode: "start" | "run"): WorkParent | null {
		let callerOrigin: string | undefined;
		if (input.callerSessionId) callerOrigin = this.#db.originForSessionId(input.callerSessionId);
		const lanePrefix = "work/task/";
		const callerLane = callerOrigin?.startsWith(lanePrefix) ? callerOrigin.slice(lanePrefix.length) : undefined;
		let callerPersona: OriginRef | undefined;
		if (callerOrigin && callerLane === undefined) {
			try {
				const origin = parseOriginKey(callerOrigin);
				if (["discord", "slack", "telegram", "loopback"].includes(origin.platform)) callerPersona = origin;
			} catch {
				/* Unknown and non-persona origins fall back to ownerTarget for starts. */
			}
		}
		if (callerLane !== undefined && /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(callerLane)) {
			if (this.#options.allowNested?.() !== true) {
				console.error(`work_nested_refused verb=${mode} name=${input.name} parent=${callerLane}`);
				throw new ProtocolError(
					"unauthorized",
					`work.${mode} from a work lane is disabled (work.allowNested=false); finish your task and report back to your parent instead`,
					{ reasonCode: "nested_lane_forbidden", verb: mode, parent: callerLane },
				);
			}
			if (mode === "start") this.#assertNoNestedCycle(input.name, callerLane);
			const parent: WorkParent = { kind: "lane", name: callerLane, root: this.#rootForLane(callerLane) };
			const resolved = mode === "start" ? parent : null;
			console.error(
				`work_parent_resolved verb=${mode} name=${input.name} source=session kind=${mode === "start" ? "lane" : "none"}`,
			);
			return resolved;
		}
		if (mode === "run") {
			console.error(`work_parent_resolved verb=run name=${input.name} source=none kind=none`);
			return null;
		}
		if (callerPersona) {
			const parent: WorkParent = { kind: "persona", originKey: originKey(callerPersona), origin: callerPersona };
			console.error(`work_parent_resolved verb=start name=${input.name} source=session kind=persona`);
			return parent;
		}
		const owner = this.#options.ownerTarget?.();
		if (owner) {
			try {
				validateOriginRef(owner);
				if (["discord", "slack", "telegram", "loopback"].includes(owner.platform)) {
					const parent: WorkParent = { kind: "persona", originKey: originKey(owner), origin: structuredClone(owner) };
					console.error(`work_parent_resolved verb=start name=${input.name} source=owner kind=persona`);
					return parent;
				}
			} catch {
				/* Invalid owner origins cannot become report targets. */
			}
		}
		console.error(`work_parent_resolved verb=start name=${input.name} source=none kind=none`);
		return null;
	}
	#laneNotice(
		name: string,
		parent: WorkParent | null,
		epoch: number,
	): { readonly text: string; readonly hash: string } | undefined {
		const allowNested = this.#options.allowNested?.() === true;
		const text = laneSystemNotice({ name, parent, allowNested });
		const hash = createHash("sha256").update(text).digest("hex");
		const stored = this.#db.metaGet(`lane-notice:${workSessionKey(name)}`);
		if (stored) {
			try {
				const previous = JSON.parse(stored) as { epoch?: unknown; hash?: unknown };
				if (previous.epoch === epoch && previous.hash === hash) return undefined;
			} catch {
				/* Corrupt notice metadata fails closed by re-sending the notice. */
			}
		}
		return { text, hash };
	}
	#task(taskId: string): WorkTask {
		const task = this.#db.workTaskGet(taskId);
		if (!task) throw new ProtocolError("invalid_params", "unknown task");
		this.#assertNotQuarantined(task.jobId, task.laneName);
		return task;
	}
	#refuseTaskBypass(name: string): void {
		if (this.#db.workTaskByLane(name) || name.startsWith("fm-"))
			throw new ProtocolError("invalid_params", "task lanes require original mapped task admission");
	}
	#assertTaskSurface(task: WorkTask): void {
		if (!task.thread || task.surfacePhase !== "bound" || this.#options.taskSurfaceAvailable?.(task.thread) !== true)
			throw new ProtocolError("verb_failed", "mapped surface unavailable");
	}
	#worktreeProof(task: Pick<WorkTaskQualifiedAdmissionScope, "request">) {
		const coordinator = this.#options.coordinatorCwd?.(task.request.coordinator);
		if (!coordinator) throw new ProtocolError("invalid_params", "coordinator execution area unavailable");
		return admitDedicatedWorktree(task.request.cwd, coordinator);
	}
	#originalWorktreeProof(task: Pick<WorkTaskQualifiedAdmissionScope, "taskId" | "request">) {
		const source = this.#db.workTaskSourceGet(`worktree-admission-${task.taskId}`);
		if (!source || source.taskId !== task.taskId || source.kind !== "decision" || source.completeness !== "complete")
			throw new Error("original_worktree_proof_unavailable");
		const proof = JSON.parse(source.body) as ReturnType<typeof admitDedicatedWorktree>;
		if (JSON.stringify(this.#worktreeProof(task)) !== JSON.stringify(proof))
			throw new Error("dedicated_worktree_changed");
		revalidateDedicatedWorktree(proof);
		return proof;
	}
	taskProjection(taskId: string): WorkTaskProjection {
		const task = this.#task(taskId);
		return this.#taskProjection(task);
	}
	#taskProjection(task: WorkTask, dispositionBasis = false): WorkTaskProjection {
		const taskId = task.taskId;
		const request =
			"thread" in task.request.surface
				? {
						threadOrigin: task.request.surface.thread as Extract<
							WorkTaskProjection["surface"],
							{ phase: "bound" }
						>["origin"],
					}
				: {
						parentOrigin: task.request.surface.parent as Extract<
							WorkThreadClaimResult,
							{ create: true }
						>["parentOrigin"],
						...(task.request.surface.title ? { title: task.request.surface.title } : {}),
					};
		const surface: WorkTaskProjection["surface"] = task.thread
			? {
					phase: "bound",
					origin: task.thread as Extract<WorkTaskProjection["surface"], { phase: "bound" }>["origin"],
					...(task.surfaceClaimId ? { claimId: task.surfaceClaimId } : {}),
				}
			: task.surfacePhase === "held"
				? {
						phase: "held",
						reason: task.surfaceHoldReason!,
						...(task.surfaceClaimId ? { claimId: task.surfaceClaimId } : {}),
					}
				: task.surfacePhase === "claimed"
					? { phase: "claimed", request, claimId: task.surfaceClaimId! }
					: { phase: "pending", request };
		return {
			taskId,
			name: task.laneName,
			kind: task.request.kind,
			jobId: task.jobId,
			opRef: task.opRef,
			...(dispositionBasis ? { dispositionBasis: this.#dispositionBasis(task) } : {}),
			sessionId: task.sessionId,
			epoch: task.epoch,
			surface,
			obligation: task.obligationState,
			...(task.holdReason ? { holdReason: task.holdReason } : {}),
			finalReport: {
				reportId: task.terminalReportId,
				completeness: task.obligationState === "final_admitted" ? "complete" : "unavailable",
				disposition:
					task.obligationState === "final_admitted" ? "admitted" : task.obligationState === "held" ? "held" : "pending",
				...(task.holdReason ? { reason: task.holdReason } : {}),
			},
		};
	}
	#dispositionBasis(task: WorkTask) {
		const page = this.#db.workControlList(task.taskId, 0, 21);
		const held = page.slice(0, 20).filter((control) => control.phase === "held");
		if (
			held.some(
				(control) =>
					control.receipt !== null ||
					control.request.expectedOpRef !== task.opRef ||
					control.sessionId !== task.sessionId ||
					control.epoch !== task.epoch,
			)
		)
			throw new ProtocolError("verb_failed", "original held control identity unavailable");
		return validateWorkTaskDispositionBasis({
			taskId: task.taskId,
			jobId: task.jobId,
			expectedOpRef: task.opRef,
			sessionId: task.sessionId,
			epoch: task.epoch,
			cwd: task.request.cwd,
			requestHash: task.requestHash,
			expectedTaskVersion: task.version,
			controls: held.map((control) => ({
				kind: "control",
				controlId: control.controlId,
				eventId: control.request.evidence.eventId,
				clientRef: control.clientRef,
			})),
			controlsCompleteness: page.length > 20 ? "partial" : "complete",
			report: task.obligationState === "held" ? { kind: "report", reportId: task.terminalReportId } : null,
		});
	}
	async #admitTask(
		params: object & Record<"task", unknown>,
		context?: WorkTaskAdmissionContext,
	): Promise<WorkStartResult> {
		this.#live();
		const { task: rawTask, ...ordinary } = params;
		const spec = validateWorkTaskSpec(rawTask);
		const input = parseInput(ordinary);
		if (input.name !== `fm-${spec.taskId}` || input.resume) invalid("task");
		const origin = context?.stableOrigin;
		if (!origin || !context || !["discord", "slack", "telegram", "loopback"].includes(origin.platform))
			throw new ProtocolError("unauthorized", "task admission requires an authenticated persona origin");
		validateOriginRef(origin);
		if (
			originKey(context.evidence.origin) !== originKey(origin) ||
			(input.callerSessionId && this.#db.originForSessionId(input.callerSessionId) !== originKey(origin)) ||
			this.#db.workTaskByThread(originKey(origin))
		)
			throw new ProtocolError("unauthorized", "task caller identity mismatch or nested task");
		return this.#port.runExclusive(workSessionKey(input.name), async () => {
			const existing = this.#db.workTaskGet(spec.taskId);
			if (!existing && this.#job(input.name)) throw new ProtocolError("invalid_params", "task name has lane history");
			const admitted = this.#db.withTransaction(() => {
				const result = this.#db.workTaskCreateInTransaction({
					taskId: spec.taskId,
					opRef: existing?.opRef ?? newOpRef("firstmate"),
					request: {
						text: input.text,
						...(spec.context ? { context: spec.context } : {}),
						kind: spec.kind,
						cwd: input.cwd,
						...(input.model ? { model: input.model } : {}),
						coordinator: origin,
						evidence: context.evidence,
						surface: spec.surface.threadOrigin
							? { thread: spec.surface.threadOrigin }
							: { parent: spec.surface.parentOrigin, ...(spec.surface.title ? { title: spec.surface.title } : {}) },
					},
				});
				if (result.disposition === "conflict") throw new ProtocolError("invalid_params", "task assignment conflict");
				if (result.disposition === "created") {
					let proof: ReturnType<typeof admitDedicatedWorktree> | undefined;
					try {
						proof = this.#worktreeProof(result.record);
					} catch (error) {
						if (spec.kind === "code_mutating") throw error;
					}
					if (proof)
						this.#db.workTaskSourceAppendInTransaction({
							sourceId: `worktree-admission-${spec.taskId}`,
							taskId: spec.taskId,
							kind: "decision",
							body: JSON.stringify(proof),
							evidence: context.evidence,
							supersedes: null,
							completeness: "complete",
							controlId: null,
							reportId: null,
						});
				}
				return result.record;
			});
			if (admitted.surfacePhase === "held")
				return {
					started: false,
					held: true,
					taskId: admitted.taskId,
					jobId: admitted.jobId,
					opRef: admitted.opRef,
					state: "held",
					reason: admitted.surfaceHoldReason!,
				};
			if (admitted.dispatchPhase === "prepared") {
				const runtime = this.#db.workAttemptGet(admitted.opRef);
				if (runtime?.sendPhase === "accepted")
					return {
						started: true,
						taskId: admitted.taskId,
						jobId: admitted.jobId,
						opRef: admitted.opRef,
						sessionKey: workSessionKey(admitted.laneName),
						sessionId: admitted.sessionId!,
					};
				return {
					started: false,
					held: true,
					taskId: admitted.taskId,
					jobId: admitted.jobId,
					opRef: admitted.opRef,
					state: "held",
					reason: "original_dispatch_uncertain",
				};
			}
			if (admitted.request.kind === "code_mutating") this.#originalWorktreeProof(admitted);
			return {
				started: false,
				accepted: "durable",
				execution: "pending_surface",
				taskId: admitted.taskId,
				jobId: admitted.jobId,
				opRef: admitted.opRef,
			};
		});
	}
	async threadClaim(params: unknown): Promise<WorkThreadClaimResult> {
		const input = validateWorkThreadClaimParams(params);
		this.#live();
		const task = this.#task(input.taskId);
		return this.#port.runExclusive(workSessionKey(task.laneName), async () =>
			this.#db.withTransaction(() => {
				const current = this.#task(input.taskId);
				if (current.surfacePhase !== "pending" || "thread" in current.request.surface)
					return {
						...input,
						create: false,
						disposition: current.surfacePhase === "held" ? "held" : "duplicate",
						surface: this.taskProjection(input.taskId).surface,
					};
				if (current.request.kind === "code_mutating") this.#originalWorktreeProof(current);
				const claimed = this.#db.workTaskSurfaceInTransaction(input.taskId, current.version, {
					phase: "claimed",
					claimId: input.claimId,
					at: this.#at(),
				});
				if (!claimed) throw new Error("task_claim_raced");
				return {
					...input,
					create: true,
					parentOrigin: current.request.surface.parent as Extract<
						WorkThreadClaimResult,
						{ create: true }
					>["parentOrigin"],
					...(current.request.surface.title ? { title: current.request.surface.title } : {}),
				};
			}),
		);
	}
	/** Binding publishes an assignment marker, but does not establish a platform ingress floor. */
	async threadBind(params: unknown): Promise<WorkThreadBindResult> {
		const input = validateWorkThreadBindParams(params);
		this.#live();
		const task = this.#task(input.taskId);
		await this.recover();
		return this.#port.runExclusive(workSessionKey(task.laneName), async () => {
			const current = this.#task(input.taskId);
			if (current.surfacePhase === "bound" || current.surfacePhase === "held") {
				if (
					(input.outcome.kind === "bound" &&
						(!current.thread || originKey(current.thread) !== originKey(input.outcome.origin))) ||
					(current.surfaceClaimId !== null && current.surfaceClaimId !== input.claimId)
				)
					throw new ProtocolError("invalid_params", "task binding conflict");
				return {
					taskId: task.taskId,
					claimId: input.claimId,
					disposition: "duplicate",
					surface: this.taskProjection(task.taskId).surface as WorkThreadBindResult["surface"],
				};
			}
			if (input.outcome.kind === "bound" && this.#options.taskSurfaceAvailable?.(input.outcome.origin) !== true)
				throw new ProtocolError("invalid_params", "verified inactive surface required");
			this.#db.withTransaction(() => {
				const bound = this.#db.workTaskSurfaceInTransaction(
					task.taskId,
					current.version,
					input.outcome.kind === "bound"
						? {
								phase: "bound",
								thread: input.outcome.origin,
								claimId: current.surfaceClaimId === null ? null : input.claimId,
								at: this.#at(),
							}
						: { phase: "held", reason: input.outcome.reason, at: this.#at() },
				);
				if (!bound) throw new Error("task_bind_raced");
				if (bound.thread)
					this.#source(bound, {
						sourceId: `activation-${task.taskId}`,
						taskId: task.taskId,
						kind: "observation",
						body: `Assignment ${task.taskId}\n${task.request.text}\nScope: ${task.request.kind}; repository admission is not an OS sandbox.`,
						evidence: {
							principalId: "gateway",
							origin: bound.thread,
							eventId: input.claimId,
							editId: null,
							evidenceAt: this.#at(),
							observedAt: this.#at(),
						},
						supersedes: null,
						completeness: "complete",
						controlId: null,
						reportId: null,
					});
			});
			const bound = this.#task(task.taskId);
			if (bound.surfacePhase === "bound") await this.#dispatchTask(bound);
			return {
				taskId: task.taskId,
				claimId: input.claimId,
				disposition: bound.surfacePhase === "held" ? "held" : "recorded",
				surface: this.taskProjection(task.taskId).surface as WorkThreadBindResult["surface"],
			};
		});
	}
	async #dispatchTask(task: WorkTask): Promise<void> {
		if (task.dispatchPhase !== "pending" || task.surfacePhase !== "bound") return;
		// A pending row is never sent: preparation atomically consumes this sole dispatch permission.
		await this.#startLocked(
			{
				name: task.laneName,
				text: [
					`Declared scope: ${task.request.kind}. ${task.request.kind === "read_only" ? "Do not edit files or perform mutations." : "Use only the admitted workspace."}`,
					task.request.text,
					task.request.context,
				]
					.filter(Boolean)
					.join("\n\n"),
				cwd: task.request.cwd,
				resume: false,
				model: task.request.model,
			},
			"start",
			{
				opRef: task.opRef,
				parent: { kind: "persona", origin: task.request.coordinator, originKey: originKey(task.request.coordinator) },
			},
		);
	}
	#source(task: WorkTask, input: WorkTaskSourceInput): void {
		this.#assertTaskSurface(task);
		const deliveryId = workTaskSourceDeliveryId(task.taskId, input.sourceId, task.thread!);
		const payload = buildDeliveryPayload(
			task.opRef,
			task.thread!,
			input.body,
			deliveryId,
			undefined,
			input.reportId !== null,
		);
		if (!payload) throw new Error("task_payload_unavailable");
		this.#db.workTaskSourceAppendInTransaction(input, {
			...payload,
			workTask: { taskId: task.taskId, opRef: task.opRef, sourceId: input.sourceId, mappedOnly: true },
		});
	}
	/** Existing observation boundaries only. A retained fact is never refreshed or redelivered. */
	#taskObservation(
		task: WorkTask,
		fact: "checkpoint" | "reconciliation_unavailable" | "output_unavailable",
		at: string,
		headSha?: string,
	): void {
		this.#db.withTransaction(() => {
			const current = this.#db.workTaskGet(task.taskId);
			if (
				!current ||
				current.version !== task.version ||
				current.dispatchPhase !== "prepared" ||
				current.sessionId === null ||
				current.epoch === null
			)
				return;
			const binding = this.#db.getSessionRecord(workSessionKey(current.laneName));
			if (binding?.sessionId !== current.sessionId || binding.epoch !== current.epoch) return;
			const identity = {
				taskId: current.taskId,
				jobId: current.jobId,
				opRef: current.opRef,
				sessionId: current.sessionId,
				epoch: current.epoch,
			};
			const sourceId = `execution-${createHash("sha256").update(JSON.stringify({ identity, fact, headSha })).digest("hex")}`;
			if (this.#db.workTaskSourceGet(sourceId)) return;
			const mapped =
				current.thread !== null &&
				current.surfacePhase === "bound" &&
				this.#options.taskSurfaceAvailable?.(current.thread) === true;
			const input: WorkTaskSourceInput = {
				sourceId,
				taskId: current.taskId,
				kind: "observation",
				body: `${JSON.stringify(identity)}\n${
					fact === "checkpoint"
						? `Repository checkpoint observed: ${headSha}. This commit observation is not semantic progress, task success, or proof of authorship.`
						: "Original attempt reconciliation unavailable. Status/output evidence is incomplete; no failure of the task itself is inferred."
				}\nObservation category: ${fact}.\nSource ${sourceId}; first evidence observed ${at}.\nExecution coverage remains incomplete; no replay or control authority.${mapped ? "" : "\nMapped surface unavailable: audit only, no delivery intent or retroactive delivery promise."}`,
				evidence: {
					principalId: "gateway",
					origin: current.thread ?? current.request.coordinator,
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
			// Never truncate an identity into apparent validity or retain unbounded diagnostics.
			if (Buffer.byteLength(input.body, "utf8") > 8 * 1024) return;
			if (mapped) this.#source(current, input);
			else this.#db.workTaskSourceAppendInTransaction(input);
		});
	}
	#observeAttempt(
		observer: Observer,
		fact: "checkpoint" | "reconciliation_unavailable" | "output_unavailable",
		at: string,
		headSha?: string,
	): void {
		if (!this.#writeCurrent(observer) || !this.#binding(observer.runtime)) return;
		const original = observer.runtime;
		const task = this.#db.workTaskByLane(original.sessionKey.slice("work/task/".length));
		if (
			!task ||
			task.opRef !== original.opRef ||
			task.jobId !== original.jobId ||
			task.sessionId !== original.sessionId ||
			task.epoch !== original.epoch ||
			task.request.cwd !== original.cwd
		)
			return;
		const admission = this.#db.workTaskSourceGet(`worktree-admission-${task.taskId}`);
		if (admission) {
			try {
				if (admission.body !== JSON.stringify(this.#worktreeProof(task))) return;
			} catch {
				return;
			}
		} else if (task.request.kind === "code_mutating") return;
		const current = this.#db.workAttemptGet(original.opRef);
		if (
			!current ||
			current.settledAt ||
			current.sessionId !== original.sessionId ||
			current.epoch !== original.epoch ||
			current.cwd !== original.cwd ||
			current.jobId !== original.jobId
		)
			return;
		const job = this.#job(task.laneName);
		if (
			!job ||
			job.jobId !== original.jobId ||
			job.lane.worktreePath !== original.cwd ||
			!job.attempts.some(
				(attempt) =>
					attempt.opRef === original.opRef && attempt.sessionId === original.sessionId && attempt.endedAt === undefined,
			)
		)
			return;
		if (fact === "checkpoint" && (!headSha || !/^[0-9a-f]{40}$/.test(headSha))) return;
		this.#taskObservation(task, fact, at, headSha);
	}
	#observeFailure(
		observer: Observer,
		fact: "reconciliation_unavailable" | "output_unavailable" = "reconciliation_unavailable",
	): void {
		try {
			this.#observeAttempt(observer, fact, this.#at());
		} catch {
			/* Unattributable/corrupt task or unavailable transaction: never fabricate a source. */
		}
		const prefix = "work/task/fm-";
		if (observer.runtime.sessionKey.startsWith(prefix))
			this.#observeLinkedUnavailable(observer.runtime.sessionKey.slice(prefix.length));
	}
	#observeLinkedUnavailable(taskId: string): void {
		try {
			this.#db.withTransaction(() =>
				this.#db.workTaskRecordLinkedUnavailableInTransaction(
					taskId,
					this.#at(),
					(origin) => this.#options.taskSurfaceAvailable?.(origin, taskId) === true,
				),
			);
		} catch {
			// No independently valid admission anchor: no invented identity/source.
			// Existing recovery quarantine is retained; this path grants no authority.
		}
	}
	/**
	 * Server callback for an authenticated, whole-marker Discord delivery receipt.
	 * Caller MUST join this with confirmation in the same transaction. Current
	 * deliveryConfirmWithSettle owns its transaction; integration needs its joined seam.
	 * No SDK send and no inference from local times or thread-creation snowflakes.
	 */
	recordTaskActivationInTransaction(input: {
		readonly taskId: string;
		readonly deliveryId: string;
		readonly origin: OriginRef;
		readonly messageId: string;
		readonly evidenceAt: string;
		readonly observedAt: string;
		readonly principalId: string;
	}): void {
		this.#db.requireTransaction();
		const task = this.#task(input.taskId);
		this.#assertTaskSurface(task);
		const marker = this.#db.workTaskSourceGet(`activation-${task.taskId}`);
		const delivery = this.#db.deliveryGet(input.deliveryId);
		if (
			!marker ||
			marker.deliveryId !== input.deliveryId ||
			!delivery ||
			delivery.state !== "confirmed" ||
			delivery.turn_id !== task.opRef ||
			delivery.origin_key !== originKey(task.thread!) ||
			originKey(input.origin) !== originKey(task.thread!) ||
			!input.messageId ||
			!Number.isFinite(Date.parse(input.evidenceAt))
		)
			throw new ProtocolError("invalid_params", "original confirmed marker evidence required");
		const sourceId = `activation-floor-${task.taskId}`;
		const body = JSON.stringify({ deliveryId: input.deliveryId, messageId: input.messageId });
		const existing = this.#db.workTaskSourceGet(sourceId);
		if (existing) {
			if (
				existing.body !== body ||
				existing.evidence.evidenceAt !== input.evidenceAt ||
				existing.evidence.principalId !== input.principalId
			)
				throw new ProtocolError("invalid_params", "activation floor evidence conflict");
			return;
		}
		this.#db.workTaskSourceAppendInTransaction({
			sourceId,
			taskId: task.taskId,
			kind: "observation",
			body,
			evidence: {
				principalId: input.principalId,
				origin: task.thread!,
				eventId: input.messageId,
				editId: null,
				evidenceAt: input.evidenceAt,
				observedAt: input.observedAt,
			},
			supersedes: null,
			completeness: "complete",
			controlId: null,
			reportId: null,
		});
	}
	/** Post-commit notification drains only existing controls, never creates execution. */
	async drainTaskControls(taskId: string): Promise<void> {
		this.#live();
		const task = this.#task(taskId);
		await this.#port.runExclusive(workSessionKey(task.laneName), () => this.#drainTaskControlsLocked(task));
	}
	/** No persona fallback. Authentication is a required server precondition. */
	async admitMappedEvent(event: MappedWorkEvent): Promise<WorkTaskControlProjection> {
		this.#live();
		const task = this.#db.workTaskByThread(originKey(event.origin));
		if (!task) throw new ProtocolError("invalid_params", "unmapped task surface");
		return this.#port.runExclusive(workSessionKey(task.laneName), async () => {
			const current = this.#task(task.taskId);
			this.#assertTaskSurface(current);
			const activation = this.#db.workTaskSourceGet(`activation-floor-${task.taskId}`);
			const time = event.platformTimestamp === null ? NaN : Date.parse(event.platformTimestamp);
			if (!Number.isFinite(time) || (activation && time < Date.parse(activation.evidence.evidenceAt)))
				throw new ProtocolError("invalid_params", "missing or pre-activation platform evidence");
			const scope = event.scope ?? task.request.kind;
			let mutationProof: ReturnType<typeof admitDedicatedWorktree> | undefined;
			if (event.kind === "steer" && scope === "code_mutating") {
				try {
					mutationProof = this.#originalWorktreeProof(current);
				} catch {
					/* Refuse new mutation controls below, including already-granted scope. */
				}
			}
			const admitted = this.#db.withTransaction(() => {
				const evidence = {
					origin: event.origin,
					principalId: event.authorId,
					eventId: event.eventId,
					editId: event.editId ?? null,
					evidenceAt: event.platformTimestamp!,
					observedAt: this.#at(),
				};
				const sourceId = `mapped-${createHash("sha256")
					.update(JSON.stringify([task.taskId, event.origin, event.eventId, event.editId ?? null]))
					.digest("hex")}`;
				const priorSource = this.#db.workTaskSourceGet(sourceId);
				if (priorSource) evidence.observedAt = priorSource.evidence.observedAt;
				if (mutationProof)
					this.#db.workTaskSourceAppendInTransaction({
						sourceId,
						taskId: task.taskId,
						kind: "instruction",
						body: event.body,
						evidence,
						supersedes: null,
						completeness: "complete",
						controlId: null,
						reportId: null,
					});
				const result = this.#db.workControlAdmitInTransaction(
					{
						taskId: task.taskId,
						expectedOpRef: task.opRef,
						kind: event.kind,
						scope,
						body: event.body,
						evidence,
					},
					mutationProof
						? {
								taskId: task.taskId,
								opRef: task.opRef,
								cwd: task.request.cwd,
								kind: "code_mutating",
								eventId: event.eventId,
								sourceId,
								validatedAt: this.#at(),
							}
						: undefined,
				);
				if (result.disposition === "conflict")
					throw new ProtocolError("invalid_params", "mapped event payload conflict");
				if (result.disposition === "created") {
					let control = result.record;
					if (mutationProof)
						this.#db.workTaskSourceAppendInTransaction({
							sourceId: `worktree-${control.controlId}`,
							taskId: task.taskId,
							kind: "decision",
							body: JSON.stringify(mutationProof),
							evidence,
							supersedes: null,
							completeness: "complete",
							controlId: control.controlId,
							reportId: null,
						});
					this.#db.workTaskSourceAppendInTransaction({
						sourceId: `instruction-${control.controlId}`,
						taskId: task.taskId,
						kind: "instruction",
						body: control.request.body,
						evidence: control.request.evidence,
						supersedes: null,
						completeness: "complete",
						controlId: control.controlId,
						reportId: null,
					});
					// Admission must not depend on draining past an earlier uncertain send.
					// Existing controls retain their original outcome and uncertainty.
					if (event.kind === "steer" && scope === "code_mutating" && !mutationProof && control.phase === "pending") {
						const refused = this.#db.workControlTransitionInTransaction(control.controlId, control.version, {
							phase: "refused",
							identity: { opRef: current.opRef, sessionId: current.sessionId!, epoch: current.epoch! },
							at: this.#at(),
							reason: "dedicated_worktree_required",
						});
						if (!refused) throw new Error("control_transition_raced");
						control = refused;
					}
					this.#controlObservation(current, control);
				}
				return result.record;
			});
			await this.#drainTaskControlsLocked(current);
			const control = this.#db.workControlGet(admitted.controlId)!;
			return {
				route: "work_task",
				taskId: task.taskId,
				controlId: control.controlId,
				opRef: task.opRef,
				acceptance: "durable",
				delivery: control.phase === "sending" ? "held" : control.phase,
				...(control.reason ? { reason: control.reason } : {}),
			};
		});
	}
	#controlObservation(task: WorkTask, control: WorkControl): void {
		const body = `Task ${task.taskId}\nOperation ${task.opRef}\nBinding ${task.sessionId ?? "not_prepared"}/${task.epoch ?? "unknown"}\nControl ${control.controlId} #${control.sequence}: ${control.phase}${control.reason ? ` (${control.reason})` : ""}\nActor ${control.request.evidence.principalId}; source ${control.request.evidence.eventId}${control.request.evidence.editId ? ` edit ${control.request.evidence.editId}` : ""}\nSteering acceptance is not task completion.`;
		this.#source(task, {
			sourceId: `disposition-${control.controlId}-${control.phase}`,
			taskId: task.taskId,
			kind: "observation",
			body,
			evidence: { ...control.request.evidence, principalId: "gateway", observedAt: control.updatedAt },
			supersedes: null,
			completeness: control.phase === "held" ? "incomplete" : "complete",
			controlId: control.controlId,
			reportId: null,
		});
		// Policy D: a control disposition is not evidence that a previously
		// requested owner action became unnecessary. Keep detail without a cockpit notice.
		// Automatic clearance notices remain unsupported; do not infer them from steering.
	}
	#controlTransition(
		task: WorkTask,
		control: WorkControl,
		phase: "sending" | "accepted" | "refused" | "held",
		reason?: string,
		source?: "turn.steer" | "turn.steer_status",
	): WorkControl {
		return this.#db.withTransaction(() => {
			this.#assertTaskSurface(this.#task(task.taskId));
			const at = this.#at();
			const identity = { opRef: task.opRef, sessionId: task.sessionId!, epoch: task.epoch! };
			const next = this.#db.workControlTransitionInTransaction(control.controlId, control.version, {
				phase,
				identity,
				at,
				...(reason ? { reason } : {}),
				...(source && (phase === "accepted" || phase === "refused")
					? {
							receipt: {
								...identity,
								source,
								clientRef: control.clientRef!,
								eventId: control.request.evidence.eventId,
								outcome: phase,
								observedAt: at,
								evidence: reason ?? "original_steer_accepted",
							},
						}
					: {}),
			});
			if (!next) throw new Error("control_transition_raced");
			this.#controlObservation(task, next);
			return this.#db.workControlGet(next.controlId)!;
		});
	}
	#controlRuntime(task: WorkTask): WorkAttemptRuntime | undefined {
		const runtime = this.#originalTaskRuntime(task);
		return runtime && this.#binding(runtime) && this.#job(task.laneName)?.attempts.at(-1)?.opRef === task.opRef
			? runtime
			: undefined;
	}
	#originalTaskRuntime(task: WorkTask): WorkAttemptRuntime | undefined {
		if (this.#stopped || this.#db.isBrokerQuarantined("work", task.jobId)) return;
		const runtime = this.#db.workAttemptGet(task.opRef);
		return runtime &&
			runtime.sessionId === task.sessionId &&
			runtime.epoch === task.epoch &&
			runtime.jobId === task.jobId &&
			runtime.cwd === task.request.cwd &&
			runtime.sessionKey === workSessionKey(task.laneName)
			? runtime
			: undefined;
	}
	async #drainTaskControlsLocked(original: WorkTask): Promise<void> {
		let after = 0;
		for (;;) {
			const task = this.#task(original.taskId);
			if (task.dispatchPhase !== "prepared") return;
			this.#assertTaskSurface(task);
			const activation = this.#db.workTaskSourceGet(`activation-floor-${task.taskId}`);
			const controls = this.#db.workControlList(task.taskId, after);
			if (!controls.length) return;
			for (let control of controls) {
				after = control.sequence;
				if (control.phase === "accepted" || control.phase === "refused") continue;
				const generation = this.#options.brokerGeneration?.() ?? 0;
				let runtime = this.#controlRuntime(task);
				if (!runtime) return;
				if (control.phase === "sending" || (control.phase === "held" && control.sendingAt !== null)) {
					// Only the same steering reference can settle a torn send, never prompt status.
					const receipt = await this.#port
						.lookupSteerStatus({
							sessionId: runtime.sessionId,
							repo: runtime.cwd,
							clientRef: control.clientRef!,
						})
						.catch(() => undefined);
					if (generation !== (this.#options.brokerGeneration?.() ?? 0)) return;
					runtime = this.#controlRuntime(this.#task(task.taskId));
					if (!runtime) return;
					if (
						receipt?.clientRef === control.clientRef &&
						(receipt.status === "accepted" || receipt.status === "rejected")
					) {
						this.#controlTransition(
							task,
							control,
							receipt.status === "accepted" ? "accepted" : "refused",
							receipt.status === "rejected" ? "original_steer_rejected" : undefined,
							"turn.steer_status",
						);
						continue;
					}
					if (control.phase === "sending") this.#controlTransition(task, control, "held", "steer_receipt_unresolved");
					return;
				}
				if (
					control.phase === "held" &&
					control.request.kind === "cancel_request" &&
					control.sendingAt === null &&
					control.receipt === null &&
					control.reason === "local_operator_action_required"
				)
					continue;
				if (control.phase === "held") return;
				const explicit = this.#db.workTaskSourceGet(`route-${control.controlId}`);
				if (!activation && !explicit) return;
				const time = Date.parse(control.request.evidence.evidenceAt);
				const floor = activation ? Date.parse(activation.evidence.evidenceAt) : NaN;
				const eventId = control.request.evidence.eventId;
				const markerId = activation?.evidence.eventId ?? "";
				if (
					!explicit &&
					(time < floor ||
						(time === floor &&
							(!isDiscordSnowflake(eventId) || !isDiscordSnowflake(markerId) || BigInt(eventId) <= BigInt(markerId))))
				) {
					this.#controlTransition(task, control, "refused", "message_precedes_activation_marker");
					continue;
				}
				if (runtime.terminal || runtime.settledAt !== null) {
					this.#controlTransition(task, control, "refused", "original_attempt_terminal");
					continue;
				}
				if (runtime.sendPhase !== "accepted") return;
				try {
					if (control.request.scope === "code_mutating") this.#revalidateControlWorktree(task, control);
				} catch {
					this.#controlTransition(task, control, "refused", "dedicated_worktree_required");
					continue;
				}
				// No awaits between this final fence, the durable claim and the transport invocation.
				if (!this.#controlRuntime(this.#task(task.taskId))) return;
				control = this.#controlTransition(task, control, "sending");
				if (!this.#controlRuntime(this.#task(task.taskId))) return;
				try {
					if (control.request.scope === "code_mutating") this.#revalidateControlWorktree(task, control);
				} catch {
					this.#controlTransition(task, control, "held", "pretransport_scope_changed");
					return;
				}
				try {
					await this.#port.steer({
						sessionId: runtime.sessionId,
						repo: runtime.cwd,
						text: control.request.body,
						clientRef: control.clientRef!,
					});
				} catch (error) {
					if (generation !== (this.#options.brokerGeneration?.() ?? 0)) return;
					if (!this.#controlRuntime(this.#task(task.taskId))) return;
					if (definitiveSteerRefusal(error)) {
						this.#controlTransition(task, control, "refused", `steer_refused:${safeRefusal(error)}`, "turn.steer");
						continue;
					}
					this.#controlTransition(task, control, "held", "steer_receipt_unresolved");
					return;
				}
				if (generation !== (this.#options.brokerGeneration?.() ?? 0)) return;
				if (!this.#controlRuntime(this.#task(task.taskId))) return;
				this.#controlTransition(task, control, "accepted", undefined, "turn.steer");
			}
		}
	}
	#revalidateControlWorktree(
		task: Pick<WorkTaskQualifiedAdmissionScope, "taskId" | "request">,
		control: WorkControl,
	): void {
		const source = this.#db.workTaskSourceGet(`worktree-${control.controlId}`);
		if (
			!source ||
			source.taskId !== task.taskId ||
			source.controlId !== control.controlId ||
			source.kind !== "decision" ||
			source.completeness !== "complete"
		)
			throw new Error("dedicated_worktree_proof_unavailable");
		const proof = JSON.parse(source.body) as ReturnType<typeof admitDedicatedWorktree>;
		if (
			proof.requestedCwd !== task.request.cwd ||
			proof.requestedCoordinator !== this.#options.coordinatorCwd?.(task.request.coordinator)
		)
			throw new Error("dedicated_worktree_changed");
		revalidateDedicatedWorktree(proof);
	}
	#settleWithTaskResult(
		text: string | undefined,
		...args: Parameters<GatewayDatabase["workAttemptSettle"]>
	): WorkAttemptSettleResult | undefined {
		const runtime = this.#db.workAttemptGet(args[0]);
		if (!runtime || !this.#db.workTaskByLane(runtime.sessionKey.slice("work/task/".length)))
			return this.#db.workAttemptSettle(...args);
		return this.#db.withTransaction(() => {
			const result = this.#db.workAttemptSettleInTransaction(...args);
			if (result) this.#settleTaskResult(result.runtime, text);
			return result;
		});
	}
	/** Called only inside the existing settlement/publication transaction. */
	#settleTaskResult(runtime: WorkAttemptRuntime, text?: string): string | undefined {
		const task = this.#db.workTaskByLane(runtime.sessionKey.slice("work/task/".length));
		if (!task || task.obligationState === "final_admitted") return;
		if (task.opRef !== runtime.opRef || task.sessionId !== runtime.sessionId || task.epoch !== runtime.epoch)
			throw new Error("task_terminal_identity_mismatch");
		const mapped = !!task.thread && this.#options.taskSurfaceAvailable?.(task.thread) === true;
		const complete =
			mapped &&
			runtime.terminal?.kind === "broker" &&
			runtime.output.proof !== null &&
			runtime.output.disposition === "available" &&
			text !== undefined &&
			text.trim().length > 0 &&
			!isSilentOutput(text) &&
			Buffer.byteLength(text, "utf8") <= 16 * 1024;
		const reason = !mapped ? "mapped_surface_unavailable" : "original_output_incomplete";
		const at = this.#at();
		const next = this.#db.workTaskObligationInTransaction(task.taskId, task.version, {
			identity: { opRef: runtime.opRef, sessionId: runtime.sessionId, epoch: runtime.epoch },
			state: complete ? "final_admitted" : "held",
			reason: complete ? null : reason,
			at,
		});
		if (!next) throw new Error("task_obligation_raced");
		const sourceId = `${complete ? "final" : "hold"}-${task.taskId}-${complete ? "original" : reason}`;
		if (this.#db.workTaskSourceGet(sourceId)) return;
		const input: WorkTaskSourceInput = {
			sourceId,
			taskId: task.taskId,
			kind: "observation",
			body: complete
				? text!
				: `Task ${task.taskId}; original operation ${task.opRef}: ${reason}. Execution evidence: ${runtime.terminal?.kind ?? "unknown"}/${runtime.terminal?.reasonCode ?? "unknown"}. Final answer remains incomplete; no execution replay.`,
			evidence: {
				principalId: "gateway",
				origin: task.thread ?? task.request.coordinator,
				eventId: runtime.opRef,
				editId: null,
				evidenceAt:
					runtime.terminal?.status?.terminalAt === undefined
						? (runtime.terminal?.observedAt ?? at)
						: new Date(runtime.terminal.status.terminalAt).toISOString(),
				observedAt: at,
			},
			supersedes: null,
			completeness: complete ? "complete" : "incomplete",
			controlId: null,
			reportId: runtime.reportId,
		};
		if (mapped) this.#source(next, input);
		else this.#db.workTaskSourceAppendInTransaction(input);
		return mapped ? workTaskSourceDeliveryId(task.taskId, sourceId, task.thread!) : undefined;
	}
	#recoverCoordinatorReport(taskId: string): void {
		const task = this.#db.workTaskGet(taskId);
		if (!task) return;
		const key = originKey(task.request.coordinator);
		const admitted = this.#db.workTaskRecoverCoordinatorReport(taskId, this.#options.personaHold?.(key));
		if (admitted) this.#options.notifyPersona?.(admitted.originKey);
	}
	/** Observes only the original result. No bind, resume, send, or observer creation. */
	async recoverTaskReport(taskId: string): Promise<WorkTaskRecoverResult> {
		this.#live();
		const task = this.#task(taskId);
		this.#recoverCoordinatorReport(taskId);
		return this.#port.runExclusive(workSessionKey(task.laneName), async () => {
			const current = this.#task(taskId);
			const base = {
				taskId,
				jobId: current.jobId,
				opRef: current.opRef,
				sessionId: current.sessionId,
				epoch: current.epoch,
				reportId: current.terminalReportId,
				execution: "none" as const,
			};
			if (current.obligationState === "final_admitted")
				return { ...base, disposition: "unchanged", completeness: "complete" };
			const runtime = this.#originalTaskRuntime(current);
			if (!runtime || runtime.terminal?.kind !== "broker" || runtime.settledAt === null)
				return {
					...base,
					disposition: "held",
					completeness: "unavailable",
					reason: "original_terminal_evidence_unavailable",
				};
			const output = await this.#port
				.fetchWorkerOutput({
					sessionId: runtime.sessionId,
					repo: runtime.cwd,
					opRef: runtime.opRef,
					notBeforeMs: Math.max(Date.parse(runtime.startedAt), runtime.terminal.status?.startedAt ?? 0),
					terminalIdentity: runtime.terminal.status,
					isCurrent: () =>
						this.#db.workTaskGet(taskId)?.version === current.version &&
						this.#originalTaskRuntime(current)?.version === runtime.version,
				})
				.catch(() => undefined);
			if (
				this.#db.workTaskGet(taskId)?.version !== current.version ||
				this.#originalTaskRuntime(current)?.version !== runtime.version
			)
				return { ...base, disposition: "held", completeness: "unavailable", reason: "original_identity_changed" };
			if (
				output?.status !== "proven" ||
				output.provenance.source !== "turn.result" ||
				output.provenance.fullness !== "original" ||
				output.provenance.contentVersion !== 1 ||
				output.provenance.sessionId !== current.sessionId ||
				output.provenance.opRef !== current.opRef ||
				output.provenance.clientRef !== current.opRef ||
				output.provenance.repo !== current.request.cwd ||
				!Number.isFinite(output.provenance.terminalAt) ||
				output.provenance.terminalAt !== runtime.terminal.status?.terminalAt ||
				output.provenance.commandId !== runtime.terminal.status?.commandId ||
				output.provenance.turnId !== runtime.terminal.status?.turnId ||
				output.provenance.byteLength !== Buffer.byteLength(output.text, "utf8") ||
				output.provenance.byteLength > 16 * 1024 ||
				!output.text.trim() ||
				isSilentOutput(output.text)
			)
				return {
					...base,
					disposition: "held",
					completeness: "unavailable",
					reason: output?.status === "proven" ? "late_report_reconciliation_required" : "original_output_incomplete",
				};
			this.#assertTaskSurface(current);
			const deliveryId = workTaskSourceDeliveryId(taskId, `supplement-${taskId}-original`, current.thread!);
			const payload = buildDeliveryPayload(current.opRef, current.thread!, output.text, deliveryId, undefined, true);
			if (!payload) throw new Error("task_payload_unavailable");
			this.#db.withTransaction(() => {
				if (
					!this.#db.workTaskSupplementInTransaction(
						taskId,
						current.version,
						{
							identity: { opRef: runtime.opRef, sessionId: runtime.sessionId, epoch: runtime.epoch },
							text: output.text,
							proof: output.provenance,
							at: this.#at(),
						},
						{
							...payload,
							workTask: { taskId, opRef: current.opRef, sourceId: `supplement-${taskId}-original`, mappedOnly: true },
						},
					)
				)
					throw new Error("task_supplement_raced");
			});
			const after = this.#task(taskId);
			return {
				...base,
				reportId: after.terminalReportId,
				disposition: after.obligationState === "final_admitted" ? "reconciled" : "held",
				completeness: after.obligationState === "final_admitted" ? "complete" : "unavailable",
				...(deliveryId ? { supplementalDeliveryId: deliveryId } : {}),
				...(after.holdReason ? { reason: after.holdReason } : {}),
			};
		});
	}
	/** Synchronous original-proof eligibility. The governor alone verifies closure and releases capacity. */
	assessTaskRelease(name: string): TaskReleaseAssessment {
		try {
			const task = this.#db.workTaskByLane(name);
			if (!task) return { kind: "unmapped" };
			const hold = (reason: string): TaskReleaseAssessment => ({ kind: "hold", reason });
			const runtime = this.#controlRuntime(task);
			const job = this.#job(name);
			const last = job?.attempts.at(-1);
			if (
				task.dispatchPhase !== "prepared" ||
				!runtime ||
				runtime.settledAt === null ||
				runtime.terminal?.kind !== "broker" ||
				runtime.terminal.status?.receiptState !== "present" ||
				!["terminal_ok", "failed"].includes(runtime.terminal.status.status) ||
				!job ||
				!last ||
				last.opRef !== task.opRef ||
				last.sessionId !== task.sessionId ||
				last.endedAt === undefined ||
				job.lane.worktreePath !== task.request.cwd ||
				job.attempts.some((attempt) => attempt.endedAt === undefined)
			)
				return hold("task_original_terminal_proof_unavailable");
			const authority = this.#db.inspectBrokerAuthority().authority;
			if (!authority) return hold("task_owned_binding_unavailable");
			const owned = this.#db.assertOwnedSession(runtime.sessionId, runtime.cwd, authority);
			if (owned.originKey !== runtime.sessionKey || owned.epoch !== runtime.epoch)
				return hold("task_owned_binding_changed");
			// The immutable admission request, not the latest steering scope, decides
			// whether dedicated isolation must still be proven. Read-only admission
			// without that proof owns host capacity only, never filesystem cleanup.
			if (task.request.kind === "code_mutating" || this.#db.workTaskSourceGet(`worktree-admission-${task.taskId}`)) {
				try {
					this.#originalWorktreeProof(task);
				} catch {
					return hold("task_original_worktree_proof_unavailable");
				}
			}
			let after = 0;
			for (;;) {
				const controls = this.#db.workControlList(task.taskId, after);
				for (const control of controls) {
					if (
						control.request.expectedOpRef !== task.opRef ||
						control.sessionId !== task.sessionId ||
						control.epoch !== task.epoch
					)
						return hold("task_control_identity_unavailable");
					if (control.request.kind === "steer" && !["accepted", "refused"].includes(control.phase))
						return hold("task_remote_control_unresolved");
					if (
						control.request.kind !== "steer" &&
						!["accepted", "refused"].includes(control.phase) &&
						(control.request.kind !== "cancel_request" ||
							control.phase !== "held" ||
							control.reason !== "local_operator_action_required" ||
							control.sendingAt !== null ||
							control.receipt !== null)
					)
						return hold("task_control_execution_uncertain");
				}
				if (controls.length < 50) break;
				after = controls.at(-1)!.sequence;
			}
			return {
				kind: "eligible",
				taskId: task.taskId,
				taskVersion: task.version,
				jobId: task.jobId,
				opRef: task.opRef,
				sessionId: runtime.sessionId,
				epoch: runtime.epoch,
				cwd: runtime.cwd,
			};
		} catch {
			return { kind: "hold", reason: "task_release_evidence_unavailable" };
		}
	}
	/** Read-only debt view. Eligibility is not closure or worktree deletion permission. */
	taskDebt(name: string) {
		const task = this.#db.workTaskByLane(name);
		if (!task) return undefined;
		const runtime = this.#db.workAttemptGet(task.opRef);
		let after = 0;
		let controls = 0;
		let remoteControls = 0;
		for (;;) {
			const page = this.#db.workControlList(task.taskId, after);
			for (const control of page) {
				if (!["accepted", "refused"].includes(control.phase)) {
					controls++;
					if (control.request.kind === "steer") remoteControls++;
				}
			}
			if (page.length < 50) break;
			after = page.at(-1)!.sequence;
		}
		const exactTerminal = runtime?.terminal?.kind === "broker" && this.#controlRuntime(task) !== undefined;
		const safeToReleaseExecution = this.assessTaskRelease(name).kind === "eligible";
		return {
			taskId: task.taskId,
			opRef: task.opRef,
			sessionId: task.sessionId,
			epoch: task.epoch,
			obligation: task.obligationState,
			unresolvedControls: controls,
			unresolvedSteers: remoteControls,
			exactTerminal,
			safeToReleaseExecution,
			safeToCleanupWorktree: false,
		};
	}
	async start(params: unknown, context?: WorkTaskAdmissionContext): Promise<WorkStartResult> {
		if (params && typeof params === "object" && "task" in params) return this.#admitTask(params, context);
		return this.#start(parseInput(params), "start");
	}
	async run(params: unknown, owner: object, signal?: AbortSignal) {
		const started = await this.#start(parseInput(params), "run");
		if (!started.started) {
			const { started: _, ...held } = started;
			return held;
		}
		await this.#wait(started.opRef, owner, signal);
		const runtime = this.#db.workAttemptGet(started.opRef)!;
		const reason = runtime.terminal?.reasonCode ?? "terminal_uncertain";
		if (reason !== "end_turn") throw workError("work attempt did not complete", reason, runtime);
		const text = this.#observers.get(runtime.opRef)?.text;
		if (text === undefined) throw workError("work output unavailable", "output_unavailable", runtime);
		return { held: false as const, text, jobId: runtime.jobId, opRef: runtime.opRef, sessionKey: runtime.sessionKey };
	}
	async #start(input: WorkInput, mode: "start" | "run"): Promise<WorkStartResult> {
		this.#live();
		this.#refuseTaskBypass(input.name);
		const parent = this.#resolveCaller(input, mode);
		this.#job(input.name);
		await this.recover();
		this.#live();
		const sessionKey = workSessionKey(input.name);
		return this.#port.runExclusive(sessionKey, () => this.#startLocked(input, mode, { parent }));
	}
	async #startLocked(input: WorkInput, mode: "start" | "run", context: StartContext): Promise<WorkStartResult> {
		this.#live();
		const task = this.#db.workTaskByLane(input.name);
		if (task && (context.opRef !== task.opRef || task.dispatchPhase !== "pending" || task.surfacePhase !== "bound"))
			throw new ProtocolError("invalid_params", "original task cannot be resumed or retasked");
		const proof = task?.request.kind === "code_mutating" ? this.#originalWorktreeProof(task) : undefined;
		if (task) this.#assertTaskSurface(task);
		const sessionKey = workSessionKey(input.name);
		let job = this.#job(input.name);
		const { jobId } = laneJobIdentity(input.name);
		if (job && job.lane.worktreePath !== input.cwd)
			throw new ProtocolError("invalid_params", "work lane cwd mismatch", {
				reasonCode: "lane_cwd_mismatch",
				name: input.name,
			});
		const open = job?.attempts.find((attempt) => attempt.endedAt === undefined);
		if (open)
			throw new ProtocolError("invalid_params", "attempt already open; use work.steer or wait", {
				reasonCode: "attempt_open",
				jobId,
				opRef: open.opRef,
				sessionId: open.sessionId,
			});
		if (job && (job.state === "awaiting_operator" || job.state === "stalled")) {
			if (!input.resume)
				return {
					started: false,
					held: true,
					jobId,
					state: job.state,
					reason: `the job is ${job.state}; reconcile, then resume explicitly`,
				};
			job = acknowledgeHold({ record: job, note: "operator resumed the work lane", at: this.#at() });
		}
		if (!job) {
			const facts = await collectRepoFacts(input.cwd);
			job = createLaneJobRecord({
				jobId,
				branch: facts?.branch ?? `work/${input.name.toLowerCase()}`,
				worktreePath: input.cwd,
				baselineSha: facts?.headSha,
			});
		}
		const binding = await this.#port.runExclusive("work/admission", async () => {
			this.#live();
			this.#options.lanes.assertAdmission(input.name);
			try {
				return await this.#port.bind({
					originKey: sessionKey,
					epoch: this.#db.getSessionRecord(sessionKey)?.epoch ?? 0,
					repo: input.cwd,
					codingRegister: true,
					...(input.model ? { model: input.model } : {}),
				});
			} catch {
				throw new ProtocolError("verb_failed", "work lane bind failed", {
					reasonCode: "bind_failed",
					name: input.name,
				});
			}
		});
		this.#live();
		const opRef = context.opRef ?? newOpRef(`work-${input.name.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`);
		const notice = mode === "start" ? this.#laneNotice(input.name, context.parent, binding.epoch) : undefined;
		const runtime = makeRuntime(
			this.#db,
			input.name,
			binding.sessionId,
			binding.epoch,
			input.cwd,
			this.#at(),
			opRef,
			mode,
			mode === "start" ? context.parent : null,
			context.wakeReportId ?? null,
			notice?.hash ?? null,
		);
		if (proof) revalidateDedicatedWorktree(proof);
		if (task) this.#assertTaskSurface(task);
		job = appendAttempt(job, { opRef, sessionId: binding.sessionId, startedAt: runtime.startedAt });
		this.#db.workAttemptPrepare(runtime, job);
		const observer = this.#register(runtime);
		this.#attach(observer);
		await observer.attaching;
		if (proof) revalidateDedicatedWorktree(proof);
		if (task) {
			this.#assertTaskSurface(this.#task(task.taskId));
			if (
				!this.#writeCurrent(observer) ||
				(proof && JSON.stringify(this.#worktreeProof(task)) !== JSON.stringify(proof))
			)
				throw workError("task dispatch identity changed before transport", "task_dispatch_fenced", runtime);
		}
		let accepted = false;
		let rejected = false;
		let source: "receipt" | "status" = "receipt";
		try {
			const receipt = await this.#port.send({
				sessionId: runtime.sessionId,
				repo: runtime.cwd,
				opRef,
				text: input.text,
				...(observer.tail ? { relay: observer.tail } : {}),
				...(notice ? { systemPreamble: notice.text } : {}),
				codingRegister: true,
				// Only this bind receipt can prove the requested model was applied at startup.
				...(input.model && binding.startupModelApplied !== true ? { model: input.model } : {}),
			});
			observer.tail?.correlate(opRef, receipt);
			accepted = receipt.operationRef === opRef && receipt.sessionId === runtime.sessionId;
		} catch (error) {
			rejected = definitiveRefusal(error);
			const code =
				typeof error === "object" && error !== null && "details" in error
					? (error.details as { code?: unknown } | undefined)?.code
					: undefined;
			console.error(
				`work_send_error opRef=${opRef} code=${typeof code === "string" && /^[a-z_]{1,64}$/.test(code) ? code : "transport_unavailable"}`,
			);
		}
		if (!this.#writeCurrent(observer)) {
			console.error(
				`work_send_fenced opRef=${opRef} generation=${observer.generation} currentGeneration=${this.#options.brokerGeneration?.() ?? 0} binding=${this.#binding(runtime)}`,
			);
			this.#live();
			throw workError("work send acceptance uncertain", "send_acceptance_uncertain", runtime);
		}
		let latest = this.#db.workAttemptGet(opRef)!;
		if (!accepted && !rejected) {
			try {
				accepted = provesAcceptance(await this.#query(latest));
				source = "status";
			} catch {
				/* Observation owns recovery; never replay. */
			}
		}
		if (!this.#writeCurrent(observer)) {
			console.error(
				`work_send_reconcile_fenced opRef=${opRef} generation=${observer.generation} currentGeneration=${this.#options.brokerGeneration?.() ?? 0} binding=${this.#binding(runtime)}`,
			);
			this.#live();
			throw workError("work send acceptance uncertain", "send_acceptance_uncertain", runtime);
		}
		latest =
			this.#db.workAttemptUpdate(
				opRef,
				latest.version,
				accepted
					? { sendPhase: "accepted", sendEvidence: { source, observedAt: this.#at() } }
					: {
							sendPhase: "uncertain",
							...(rejected
								? { terminal: { kind: "local", observedAt: this.#at(), reasonCode: "send_rejected" } as const }
								: {}),
						},
			) ?? latest;
		this.#schedule(observer, 0);
		if (rejected) throw workError("work send rejected", "send_rejected", latest);
		if (!accepted) throw workError("work send acceptance uncertain", "send_acceptance_uncertain", latest);
		if (task) await this.#drainTaskControlsLocked(this.#task(task.taskId));
		return { started: true, jobId, opRef, sessionKey, sessionId: runtime.sessionId };
	}
	async status(params: unknown): Promise<WorkStatusResult> {
		const input = validateWorkStatusParams(params);
		const name = parseName(input);
		// Fence the durable original before looking up any lane or live operation.
		const task = input.taskId !== undefined ? this.#task(input.taskId) : this.#db.workTaskByLane(name);
		if (task) {
			if (task.laneName !== name || (input.expectedOpRef !== undefined && task.opRef !== input.expectedOpRef))
				throw new ProtocolError("invalid_params", "original task identity mismatch");
			let op: PromptStatusBody | null = null;
			// Pending admission has no runtime and must never acquire one through a read.
			const runtime = task.dispatchPhase === "prepared" ? this.#controlRuntime(task) : undefined;
			const generation = this.#options.brokerGeneration?.() ?? 0;
			if (runtime) {
				try {
					op = await this.#query(runtime);
				} catch {
					// Missing original evidence is uncertainty, not a replacement-session lookup.
				}
				if (!this.#controlRuntime(task) || generation !== (this.#options.brokerGeneration?.() ?? 0)) op = null;
			}
			return this.#db.withTransaction(() => {
				const current = this.#task(task.taskId);
				if (current.version !== task.version) op = null;
				const job = this.#job(name);
				const attempt = job?.attempts.find(
					(value) => value.opRef === current.opRef && value.sessionId === current.sessionId,
				);
				return {
					jobId: current.jobId,
					state:
						attempt && job
							? job.state
							: current.surfacePhase === "held" || current.obligationState === "held"
								? "held"
								: current.dispatchPhase,
					sessionId: current.sessionId ?? "",
					lastActivityAt:
						runtime && this.#binding(runtime)
							? (this.#db.workLaneRows().find((row) => row.origin_key === workSessionKey(name))?.last_activity_at ??
								null)
							: null,
					attempt: attempt
						? {
								opRef: attempt.opRef,
								startedAt: attempt.startedAt,
								...(attempt.endedAt ? { endedAt: attempt.endedAt } : {}),
								...(attempt.endState ? { endState: attempt.endState } : {}),
							}
						: null,
					op,
					task: this.#taskProjection(current, true),
				};
			});
		}
		const job = this.#job(name, true)!;
		const key = workSessionKey(name);
		const binding = this.#db.getSessionRecord(key);
		const attempt = job.attempts.at(-1);
		let op: PromptStatusBody | null = null;
		if (attempt && binding?.sessionId === attempt.sessionId) {
			try {
				op = await this.#query({
					jobId: job.jobId,
					opRef: attempt.opRef,
					sessionId: attempt.sessionId,
					cwd: job.lane.worktreePath,
				});
			} catch (error) {
				// A settled attempt whose host is gone has no live op to report.
				if (attempt.endedAt === undefined || !isSessionUnavailable(error))
					throw workError("work status unavailable", "status_unavailable", { ...attempt, jobId: job.jobId });
			}
			const current = this.#db.getSessionRecord(key);
			if (current?.sessionId !== binding.sessionId || current.epoch !== binding.epoch) op = null;
		}
		return {
			jobId: job.jobId,
			state: job.state,
			sessionId: this.#db.getSessionRecord(key)?.sessionId ?? "",
			lastActivityAt: this.#db.workLaneRows().find((row) => row.origin_key === key)?.last_activity_at ?? null,
			attempt: attempt
				? {
						opRef: attempt.opRef,
						startedAt: attempt.startedAt,
						...(attempt.endedAt ? { endedAt: attempt.endedAt } : {}),
						...(attempt.endState ? { endState: attempt.endState } : {}),
					}
				: null,
			op,
		};
	}
	#dispositionOwner(callerSessionId: string | undefined, context?: WorkTaskAdmissionContext): OriginRef {
		if (
			!context?.stableOrigin ||
			originKey(context.stableOrigin) !== originKey(context.evidence.origin) ||
			context.evidence.editId !== null ||
			!["discord", "slack", "telegram", "loopback"].includes(context.stableOrigin.platform)
		)
			throw new ProtocolError("unauthorized", "disposition requires authenticated owner evidence");
		const origin = context.stableOrigin;
		if (callerSessionId) {
			const key = this.#db.originForSessionId(callerSessionId);
			const row = this.#db
				.sessionIdentityRows()
				.find((entry) => entry.origin_key === key && entry.gjc_session_id === callerSessionId);
			if (
				!key ||
				key.startsWith("work/") ||
				key !== originKey(origin) ||
				!row?.origin_ref_json ||
				originKey(validateOriginRef(JSON.parse(row.origin_ref_json))) !== key ||
				this.#db.inboundHasQuarantinedNonterminalTurn(key)
			)
				throw new ProtocolError("unauthorized", "disposition caller identity changed");
		}
		if (
			(origin.platform === "discord" && this.#db.workTaskDiscordLocator(origin.conversationId)) ||
			this.#db.workTaskByThread(originKey(origin))
		)
			throw new ProtocolError("unauthorized", "mapped caller cannot dispose tasks");
		return origin;
	}
	#negativeDispositionScope(taskId: string) {
		const scope = this.#db.workTaskQualifiedAdmissionScopeInTransaction(taskId);
		if (scope.request.kind === "code_mutating" || this.#db.workTaskSourceGet(`worktree-admission-${taskId}`))
			this.#originalWorktreeProof(scope);
		return scope;
	}
	async dispositionBasis(params: unknown, context?: WorkTaskAdmissionContext): Promise<WorkTaskDispositionBasisResult> {
		const input = validateWorkTaskDispositionBasisParams(params);
		this.#live();
		this.#dispositionOwner(input.callerSessionId, context);
		return this.#port.runExclusive(workSessionKey(`fm-${input.taskId}`), async () =>
			this.#db.withTransaction(() => {
				this.#live();
				this.#dispositionOwner(input.callerSessionId, context);
				const selected = this.#db.workTaskDispositionBasis(input.taskId);
				if (selected.kind === "unavailable") return selected;
				if (selected.kind === "validation_unavailable") {
					this.#negativeDispositionScope(input.taskId);
				} else {
					const task = this.#task(input.taskId);
					if (task.request.kind === "code_mutating" || this.#db.workTaskSourceGet(`worktree-admission-${task.taskId}`))
						this.#originalWorktreeProof(task);
				}
				// Qualification time belongs to the retained fact, never this read's ingress context.
				return selected;
			}),
		);
	}
	async disposition(params: unknown, context?: WorkTaskAdmissionContext): Promise<WorkTaskDispositionResult> {
		const input = validateWorkTaskDispositionParams(params);
		this.#live();
		if (
			!context?.stableOrigin ||
			originKey(context.stableOrigin) !== originKey(context.evidence.origin) ||
			context.evidence.eventId !== input.eventId ||
			context.evidence.editId !== null ||
			!["discord", "slack", "telegram", "loopback"].includes(context.stableOrigin.platform)
		)
			throw new ProtocolError("unauthorized", "disposition requires authenticated owner evidence");
		const origin = context.stableOrigin;
		const qualification = input.validationUnavailable;
		if (qualification) {
			this.#dispositionOwner(input.callerSessionId, context);
			return this.#port.runExclusive(workSessionKey(`fm-${input.taskId}`), async () =>
				this.#db.withTransaction(() => {
					this.#live();
					this.#dispositionOwner(input.callerSessionId, context);
					const scope = this.#negativeDispositionScope(input.taskId);
					if (qualification.scope !== scope.request.kind)
						throw new ProtocolError("invalid_params", "original disposition scope mismatch");
					if (input.target.kind === "control") {
						const control = this.#db.workControlGet(input.target.controlId);
						if (control?.request.taskId === scope.taskId && control.request.scope === "code_mutating") {
							this.#originalWorktreeProof(scope);
							this.#revalidateControlWorktree(scope, control);
						}
					}
					return this.#db.workTaskNegativeDispositionInTransaction(
						input,
						context.evidence,
						(destination) => this.#options.taskSurfaceAvailable?.(destination, input.taskId) === true,
					);
				}),
			);
		}
		const task = this.#task(input.taskId);
		return this.#port.runExclusive(workSessionKey(task.laneName), async () =>
			this.#db.withTransaction(() => {
				this.#live();
				const current = this.#task(input.taskId);
				if (input.callerSessionId) {
					const key = this.#db.originForSessionId(input.callerSessionId);
					const row = this.#db
						.sessionIdentityRows()
						.find((entry) => entry.origin_key === key && entry.gjc_session_id === input.callerSessionId);
					if (
						!key ||
						key.startsWith("work/") ||
						key !== originKey(origin) ||
						!row?.origin_ref_json ||
						originKey(validateOriginRef(JSON.parse(row.origin_ref_json))) !== key ||
						this.#db.inboundHasQuarantinedNonterminalTurn(key)
					)
						throw new ProtocolError("unauthorized", "disposition caller identity changed");
				}
				if (
					this.#db.workTaskByThread(originKey(origin)) ||
					(origin.platform === "discord" && this.#db.workTaskDiscordLocator(origin.conversationId))
				)
					throw new ProtocolError("unauthorized", "mapped caller cannot dispose tasks");
				// Revalidate the original declared scope, never a replacement checkout.
				if (
					current.request.kind === "code_mutating" ||
					this.#db.workTaskSourceGet(`worktree-admission-${current.taskId}`)
				)
					this.#originalWorktreeProof(current);
				if (input.target.kind === "control") {
					const control = this.#db.workControlGet(input.target.controlId);
					if (control?.request.taskId === current.taskId && control.request.scope === "code_mutating")
						this.#revalidateControlWorktree(current, control);
				}
				const sourceId = workTaskDispositionId(current.taskId, input.eventId, context.evidence.origin);
				const existing = this.#db.workTaskSourceGet(sourceId);
				let delivery: ChatMessagePayload | undefined;
				// Missing mapped presentation never redirects the administrative decision.
				// A retry cannot create an intent absent from its original atomic receipt.
				if (
					(!existing || existing.deliveryId !== null) &&
					current.thread &&
					current.surfacePhase === "bound" &&
					this.#options.taskSurfaceAvailable?.(current.thread) === true
				) {
					const payload = buildDeliveryPayload(
						current.opRef,
						current.thread,
						workTaskDispositionText(input),
						workTaskSourceDeliveryId(current.taskId, sourceId, current.thread),
						undefined,
						false,
					);
					if (!payload) throw new Error("task_payload_unavailable");
					delivery = {
						...payload,
						workTask: {
							taskId: current.taskId,
							opRef: current.opRef,
							sourceId,
							mappedOnly: true,
						},
					};
				}
				return this.#db.workTaskDispositionInTransaction(input, context.evidence, delivery);
			}),
		);
	}
	async steer(params: unknown, context?: WorkTaskAdmissionContext): Promise<WorkSteerResult> {
		const input = validateWorkSteerParams(params);
		if (input.taskId !== undefined) {
			this.#live();
			if (
				!context?.stableOrigin ||
				originKey(context.stableOrigin) !== originKey(context.evidence.origin) ||
				context.evidence.eventId !== input.eventId ||
				!["discord", "slack", "telegram", "loopback"].includes(context.stableOrigin.platform)
			)
				throw new ProtocolError("unauthorized", "explicit steering requires authenticated non-work caller evidence");
			const task = this.#task(input.taskId);
			if (task.opRef !== input.expectedOpRef || task.laneName !== input.name)
				throw new ProtocolError("invalid_params", "original task identity mismatch");
			return this.#port.runExclusive(workSessionKey(task.laneName), async (): Promise<WorkSteerResult> => {
				const current = this.#task(task.taskId);
				this.#assertTaskSurface(current);
				const scope = input.kind ?? current.request.kind;
				const proof = scope === "code_mutating" ? this.#originalWorktreeProof(current) : undefined;
				if (proof) revalidateDedicatedWorktree(proof);
				const control = this.#db.withTransaction(() => {
					const sourceId = `explicit-${createHash("sha256")
						.update(JSON.stringify([current.taskId, context.evidence.origin, input.eventId, context.evidence.editId]))
						.digest("hex")}`;
					const priorSource = this.#db.workTaskSourceGet(sourceId);
					this.#db.workTaskSourceAppendInTransaction({
						sourceId,
						taskId: current.taskId,
						kind: "instruction",
						body: input.text,
						evidence: {
							...context.evidence,
							observedAt: priorSource?.evidence.observedAt ?? context.evidence.observedAt,
						},
						supersedes: null,
						completeness: "complete",
						controlId: null,
						reportId: null,
					});
					const result = this.#db.workControlAdmitInTransaction(
						{
							taskId: current.taskId,
							expectedOpRef: input.expectedOpRef!,
							kind: "steer",
							scope,
							body: input.text,
							evidence: context.evidence,
						},
						proof
							? {
									taskId: current.taskId,
									opRef: current.opRef,
									cwd: current.request.cwd,
									kind: "code_mutating",
									eventId: input.eventId!,
									sourceId,
									validatedAt: this.#at(),
								}
							: undefined,
						{ taskId: current.taskId, opRef: current.opRef, thread: current.thread!, sourceId },
					);
					if (result.disposition === "conflict") throw new ProtocolError("invalid_params", "control payload conflict");
					if (result.disposition === "created") {
						if (proof)
							this.#db.workTaskSourceAppendInTransaction({
								sourceId: `worktree-${result.record.controlId}`,
								taskId: current.taskId,
								kind: "decision",
								body: JSON.stringify(proof),
								evidence: context.evidence,
								supersedes: null,
								completeness: "complete",
								controlId: result.record.controlId,
								reportId: null,
							});
						this.#controlObservation(current, result.record);
					}
					return result.record;
				});
				await this.#drainTaskControlsLocked(current);
				const latest = this.#db.workControlGet(control.controlId)!;
				const base = {
					route: "work_task" as const,
					taskId: current.taskId,
					opRef: current.opRef,
					controlId: latest.controlId,
					acceptance: "durable" as const,
					...(latest.reason ? { reason: latest.reason } : {}),
				};
				return latest.phase === "accepted"
					? { ...base, delivery: "accepted", steered: true, clientRef: latest.clientRef! }
					: { ...base, delivery: latest.phase === "sending" ? "held" : latest.phase, steered: false };
			});
		}
		const name = parseName(params);
		this.#refuseTaskBypass(name);
		const text = (params as { text?: unknown }).text;
		if (typeof text !== "string" || !text) invalid("text");
		this.#live();
		const job = this.#job(name, true)!;
		const captured = job.attempts.find((attempt) => attempt.endedAt === undefined);
		if (!captured)
			throw new ProtocolError("invalid_params", "no open attempt to steer", {
				reasonCode: "no_open_attempt",
				jobId: job.jobId,
			});
		await this.recover();
		this.#live();
		return this.#port.runExclusive(workSessionKey(name), async (): Promise<WorkSteerResult> => {
			this.#live();
			const current = this.#job(name, true)!;
			const open = current.attempts.at(-1);
			if (open?.opRef !== captured.opRef || open.endedAt !== undefined)
				throw new ProtocolError("invalid_params", "no open attempt to steer", {
					reasonCode: "no_open_attempt",
					jobId: job.jobId,
				});
			const runtime = this.#db.workAttemptGet(open.opRef);
			const clientRef = newOpRef("work-steer");
			if (!runtime || !this.#binding(runtime) || runtime.terminal)
				throw workError(
					"work steer acceptance uncertain",
					"steer_acceptance_uncertain",
					{ ...open, jobId: job.jobId },
					clientRef,
				);
			try {
				await this.#port.steer({ sessionId: open.sessionId, repo: current.lane.worktreePath, text, clientRef });
			} catch (error) {
				if (definitiveSteerRefusal(error)) return { steered: false, reason: `steer_refused:${safeRefusal(error)}` };
				throw workError("work steer acceptance uncertain", "steer_acceptance_uncertain", runtime, clientRef);
			}
			return { steered: true, clientRef };
		});
	}
	async #query(runtime: { jobId: string; sessionId: string; cwd: string; opRef: string }): Promise<PromptStatusBody> {
		this.#assertNotQuarantined(runtime.jobId);
		const report = await this.#port.status({
			sessionId: runtime.sessionId,
			repo: runtime.cwd,
			opRef: runtime.opRef,
			priority: "background",
		});
		if (
			report.operationRef !== runtime.opRef ||
			!report.status ||
			!["accepted", "in_flight", "terminal_ok", "failed", "unknown"].includes(report.status.status) ||
			(report.status.clientRef !== undefined && report.status.clientRef !== runtime.opRef)
		)
			throw new Error("invalid status identity");
		return safeStatus(report.status);
	}
	#register(runtime: WorkAttemptRuntime): Observer {
		const existing = this.#observers.get(runtime.opRef);
		if (existing && this.#current(existing)) return existing;
		const binding = this.#db.getSessionRecord(runtime.sessionKey);
		const observer: Observer = {
			runtime,
			generation: this.#options.brokerGeneration?.() ?? 0,
			abort: new AbortController(),
			binding: binding ? { sessionId: binding.sessionId, epoch: binding.epoch } : undefined,
		};
		this.#observers.set(runtime.opRef, observer);
		return observer;
	}
	#schedule(observer: Observer, delay: number): void {
		if (!this.#current(observer)) return;
		if (observer.task) {
			if (delay === 0) observer.wakeRequested = true;
			return;
		}
		if (observer.timer) {
			if (delay !== 0) return;
			clearTimeout(observer.timer);
			observer.timer = undefined;
		}
		const opRef = observer.runtime.opRef;
		observer.timer = setTimeout(() => {
			observer.timer = undefined;
			if (!this.#current(observer)) return;
			observer.task = this.#tick(observer)
				.then(
					() => {
						this.#failures.delete(opRef);
					},
					(error: unknown) => this.#failed(observer, error),
				)
				.finally(() => {
					observer.task = undefined;
					if (!this.#current(observer)) return;
					if (this.#db.workAttemptGet(opRef)?.settledAt !== null) {
						this.#observers.delete(opRef);
						this.#failures.delete(opRef);
						return;
					}
					// The lane was rebound under this observer: it can never write again,
					// so polling is pure waste. Recovery re-proves authority and settles.
					if (!this.#writeCurrent(observer)) return this.#readopt(observer, "binding_changed");
					const failures = this.#failures.get(opRef)?.failures ?? 0;
					if (failures > 0 && failures % FAILURES_PER_READOPTION === 0)
						return this.#readopt(observer, "reconciliation_unavailable");
					if (observer.wakeRequested) {
						observer.wakeRequested = false;
						return this.#schedule(observer, 0);
					}
					const pollMs = this.#options.pollMs ?? 250;
					// When a tail is attached, frames wake the observer; use a slow fallback
					// cadence instead of polling every 250ms.
					const basePollMs = observer.tail && !this.#options.pollMs ? 5_000 : pollMs;
					this.#schedule(
						observer,
						failures ? Math.min(basePollMs * 2 ** (failures - 1), MAX_FAILURE_BACKOFF_MS) : basePollMs,
					);
				})
				// This chain runs off a timer with nothing awaiting it: a throw here (an
				// unreadable runtime row, #401) would be an unhandled rejection that kills
				// the gateway. Stop observing only this attempt; recovery reports it.
				.catch((error: unknown) => this.#dropObserver(observer, error));
		}, delay);
	}
	#dropObserver(observer: Observer, error: unknown): void {
		const opRef = observer.runtime.opRef;
		this.#observeFailure(observer);
		console.error(`work_observer_failed opRef=${JSON.stringify(opRef)} error=${failureReason(error)}`);
		observer.abort.abort();
		if (observer.timer) clearTimeout(observer.timer);
		observer.timer = undefined;
		if (this.#observers.get(opRef) === observer) this.#observers.delete(opRef);
		this.#failures.delete(opRef);
	}
	/** Counts the failure and reports it once per distinct reason instead of on every poll. */
	#failed(observer: Observer, error: unknown): void {
		const opRef = observer.runtime.opRef;
		this.#observeFailure(observer);
		const prior = this.#failures.get(opRef);
		const reason = failureReason(error);
		const failures = (prior?.failures ?? 0) + 1;
		this.#failures.set(opRef, { failures, reason });
		if (this.#current(observer) && prior?.reason !== reason)
			console.error(`work_reconciliation_unavailable opRef=${opRef} failures=${failures} reason=${reason}`);
	}
	/**
	 * Retires this observer and hands the open attempt back to recovery, which
	 * re-reads status and broker liveness and either resumes observation on a
	 * live, still-bound session or records a local terminal reason.
	 */
	#readopt(observer: Observer, cause: "binding_changed" | "reconciliation_unavailable"): void {
		const opRef = observer.runtime.opRef;
		console.error(
			`work_observer_readopted opRef=${opRef} cause=${cause} failures=${this.#failures.get(opRef)?.failures ?? 0}`,
		);
		observer.abort.abort();
		void (async () => {
			await this.recover();
			// A recovery pass already past this attempt when it was retired cannot
			// have readopted it; a pass started after retirement always does.
			const current = this.#observers.get(opRef);
			if (
				!this.#stopped &&
				(!current || current.abort.signal.aborted) &&
				this.#db.workAttemptGet(opRef)?.settledAt === null
			)
				await this.recover();
		})().catch(() => {
			/* Recovery reports its own failures; the attempt stays open and observable. */
		});
	}
	#attach(observer: Observer): void {
		if (observer.tail || observer.attaching || !this.#writeCurrent(observer) || !this.#binding(observer.runtime))
			return;
		observer.attaching = (async () => {
			const runtime = observer.runtime;
			const tail = await this.#port.attachTail({
				sessionId: runtime.sessionId,
				brokerGeneration: observer.generation,
				repo: runtime.cwd,
				originKey: runtime.sessionKey,
				onFrame: () => {
					if (!this.#writeCurrent(observer)) return;
					this.#schedule(observer, 0);
					this.#touch(observer);
				},
			});
			if (!this.#writeCurrent(observer)) {
				await tail.close();
				return;
			}
			observer.tail = tail;
			await tail.ready;
			if (this.#writeCurrent(observer) && this.#db.workAttemptGet(runtime.opRef)?.settledAt === null) {
				tail.beginTurn(runtime.opRef);
				tail.setTurnRunning(true);
			}
		})()
			.catch(async () => {
				const tail = observer.tail;
				observer.tail = undefined;
				await tail?.close();
			})
			.finally(() => {
				observer.attaching = undefined;
			});
	}
	/**
	 * Streamed turn frames are the attempt's activity: `work jobs` `last=` must
	 * show when the worker last did something, not when the attempt started, so
	 * a stalled lane is visible. Throttled; the write is fenced to the binding.
	 */
	#touch(observer: Observer): void {
		const now = this.#now();
		if (observer.touchedAt !== undefined && now - observer.touchedAt < ACTIVITY_TOUCH_MS) return;
		observer.touchedAt = now;
		try {
			this.#db.workAttemptActivity(observer.runtime.opRef, new Date(now).toISOString());
		} catch {
			console.error(`work activity update unavailable opRef=${observer.runtime.opRef}`);
		}
	}
	/**
	 * Positive evidence that the attempt's host is gone: the SDK router already
	 * answered `session_unavailable` for the operation, and an independent
	 * inspect confirms the id is disowned or not live. Broker unavailability
	 * yields neither and never settles an attempt.
	 */
	async #hostGone(runtime: { sessionId: string; cwd: string }, statusError: unknown): Promise<boolean> {
		if (!isSessionUnavailable(statusError) || !this.#port.liveness) return false;
		try {
			const live = await this.#port.liveness({ sessionId: runtime.sessionId, repo: runtime.cwd });
			return live.disowned || live.live === false;
		} catch {
			return false;
		}
	}
	async #tick(observer: Observer): Promise<void> {
		if (!this.#writeCurrent(observer)) return;
		let runtime = this.#db.workAttemptGet(observer.runtime.opRef)!;
		if (runtime.settledAt) return;
		if (!runtime.terminal) {
			this.#attach(observer);
			let status: PromptStatusBody | undefined;
			try {
				status = await this.#query(runtime);
				observer.goneSince = undefined;
			} catch (error) {
				if (!(await this.#hostGone(runtime, error))) {
					observer.goneSince = undefined;
					throw error;
				}
				// A router that is still re-indexing hosts (e.g. just restarted) may
				// briefly disown a live one: require the verdict to persist.
				observer.goneSince ??= this.#now();
				if (this.#now() - observer.goneSince < (this.#options.hostLostGraceMs ?? HOST_LOST_GRACE_MS)) return;
			}
			if (!this.#writeCurrent(observer)) return;
			if (status?.status === "unknown") return;
			if (!status) console.error(`work_host_lost opRef=${runtime.opRef} session=${runtime.sessionId} source=observer`);
			const terminal = status
				? terminalEvidence(status, this.#at())
				: { kind: "local" as const, observedAt: this.#at(), reasonCode: "host_lost" };
			const wasAccepted = runtime.sendPhase === "accepted";
			runtime =
				this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
					...(status && provesAcceptance(status)
						? {
								sendPhase: "accepted" as const,
								sendEvidence: runtime.sendEvidence ?? { source: "status" as const, observedAt: this.#at() },
							}
						: {}),
					...(terminal ? { terminal } : {}),
				}) ?? runtime;
			if (!runtime.terminal) {
				const task =
					!wasAccepted && runtime.sendPhase === "accepted"
						? this.#db.workTaskByLane(runtime.sessionKey.slice("work/task/".length))
						: undefined;
				if (task) await this.#port.runExclusive(runtime.sessionKey, () => this.#drainTaskControlsLocked(task));
				return;
			}
		}
		if (runtime.output.disposition === "pending" && runtime.output.knownSilence) {
			const silent = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
				output: { ...runtime.output, disposition: "silent", nextReadAt: null },
			});
			if (!silent) return;
			runtime = silent;
		}
		if (runtime.output.disposition === "pending") {
			if (runtime.output.nextReadAt && this.#now() < Date.parse(runtime.output.nextReadAt)) return;
			if (runtime.output.reads >= 3 || runtime.terminal?.kind === "local") {
				runtime =
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						output: { ...runtime.output, disposition: "unavailable", nextReadAt: null },
					}) ?? runtime;
			} else {
				const reads = runtime.output.reads + 1;
				const claimed = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
					output: {
						...runtime.output,
						reads,
						nextReadAt: reads === 3 ? null : new Date(this.#now() + (reads === 1 ? 1000 : 5000)).toISOString(),
					},
				});
				if (!claimed) return;
				runtime = claimed;
				const result = await this.#port
					.fetchWorkerOutput({
						sessionId: runtime.sessionId,
						repo: runtime.cwd,
						opRef: runtime.opRef,
						notBeforeMs: Math.max(Date.parse(runtime.startedAt), runtime.terminal?.status?.startedAt ?? 0),
						terminalIdentity: runtime.terminal?.status,
						signal: observer.abort.signal,
						isCurrent: () => this.#writeCurrent(observer),
						...(observer.tail ? { relay: observer.tail } : {}),
					})
					.catch(() => {
						this.#observeFailure(observer, "output_unavailable");
						return { status: "absent" as const, code: "transport_error" as const };
					});
				if (!this.#writeCurrent(observer)) return;
				if (result.status === "proven") {
					const proof = {
						...result.provenance,
						epoch: runtime.epoch,
						observedAtMs: result.observedAtMs,
						attribution: "operation_ref" as const,
					};
					const silent = isSilentOutput(result.text);
					const terminal = runtime.terminal!;
					// A terminal first observed with receiptState=missing whose same-op
					// final body then proves present is a late receipt, not a missing one
					// (#248): re-derive the end state from the reconciled receipt.
					const reconciled =
						terminal.kind === "broker" && terminal.status?.receiptState === "missing"
							? terminalEvidence({ ...terminal.status, receiptState: "present" }, terminal.observedAt)
							: undefined;
					const updated = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						...(reconciled ? { terminal: reconciled } : {}),
						output: {
							...runtime.output,
							disposition: silent ? "silent" : "available",
							nextReadAt: null,
							excerpt: utf8Prefix(result.text),
							proof,
							knownSilence: silent ? proof : null,
						},
					});
					if (!updated) return;
					runtime = updated;
					if (
						(runtime.mode === "run" && this.#waiters.has(runtime.opRef)) ||
						this.#db.workTaskByLane(runtime.sessionKey.slice("work/task/".length))
					)
						observer.text = Buffer.byteLength(result.text, "utf8") <= 16 * 1024 ? result.text : undefined;
				} else if (result.status === "unavailable" || reads >= 3) {
					runtime =
						this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
							output: { ...runtime.output, disposition: "unavailable", nextReadAt: null },
						}) ?? runtime;
				} else return;
			}
		}
		if (runtime.output.disposition !== "pending") await this.#settle(observer, runtime);
	}
	async #settle(observer: Observer, runtime: WorkAttemptRuntime): Promise<void> {
		const facts = await collectRepoFacts(runtime.cwd);
		const factsAt = this.#at();
		const result = await this.#port.runExclusive(runtime.sessionKey, async () => {
			if (!this.#writeCurrent(observer)) return;
			const current = this.#db.workAttemptGet(runtime.opRef);
			if (!current || current.version !== runtime.version || current.settledAt) return;
			const name = runtime.sessionKey.slice("work/task/".length);
			const at = this.#at();
			const reason = runtime.terminal!.reasonCode;
			// Extract transport cause for sdk_failed/terminal_missing_receipt failures
			let output = runtime.output;
			if (
				(reason === "sdk_failed" || reason === "terminal_missing_receipt") &&
				output.disposition === "unavailable" &&
				!output.transportCause
			) {
				try {
					const terminalAt = runtime.terminal?.status?.terminalAt ?? Date.parse(runtime.terminal?.observedAt ?? "");
					if (Number.isFinite(terminalAt)) {
						const input = {
							sessionId: runtime.sessionId,
							repo: runtime.cwd,
							startedAtMs: Date.parse(runtime.startedAt),
							terminalAtMs: terminalAt,
						};
						// Try port method first (for testing), then file-based reader
						let cause = await this.#port.failedTransportCause?.(input);
						if (!cause) {
							cause = await readFailedTransportCause(undefined, input);
						}
						if (cause) {
							output = { ...output, transportCause: cause };
							const parts = [`kind=${cause.kind}`];
							if (cause.nativeErrorCode) parts.push(`nativeErrorCode=${cause.nativeErrorCode}`);
							if (cause.http2RstCode !== undefined) parts.push(`http2RstCode=${cause.http2RstCode}`);
							if (cause.status !== undefined) parts.push(`status=${cause.status}`);
							console.error(`work_transport_failure opRef=${runtime.opRef} ${parts.join(" ")}`);
						}
					}
				} catch (error) {
					console.error(`work_transport_cause_read_error opRef=${runtime.opRef} reason=${failureReason(error)}`);
				}
			}
			const endState = workAttemptEndState(reason);
			let job = closeAttempt({
				record: this.#job(name, true)!,
				opRef: runtime.opRef,
				endState,
				errorCode: reason === "end_turn" ? undefined : reason,
				endedAt: at,
			});
			if (facts) {
				const progressed = hasNewCommit({
					...facts,
					observedAt: at,
					knownCheckpoints: job.checkpoints,
					baselineSha: job.baselineSha,
				});
				job = applyReconciliation({
					record: job,
					repository: { ...facts, observedAt: at },
					classification: progressed ? "progressed" : endState === "completed" ? "held" : "stalled",
				});
				if (progressed) this.#observeAttempt(observer, "checkpoint", factsAt, facts.headSha);
			}

			const text = reportText(name, endState, reason, runtime.opRef, output);
			let decision: "report" | "suppressed" | "no_target" | "wake_unaccepted";
			let admission: WorkAttemptAdmission | undefined;
			if (runtime.wakeReportId !== null && !hasAcceptanceEvidence(runtime)) {
				decision = "wake_unaccepted";
			} else if (runtime.output.knownSilence !== null) {
				decision = "suppressed";
			} else if (runtime.parent === null) {
				decision = "no_target";
			} else {
				decision = "report";
				if (runtime.parent.kind === "persona") {
					const fallbackPayload = buildDeliveryPayload(runtime.opRef, runtime.parent.origin, text, runtime.deliveryId)!;
					const holdReason = this.#options.personaHold?.(runtime.parent.originKey);
					admission = {
						kind: "persona",
						row: {
							messageId: runtime.reportId,
							originKey: runtime.parent.originKey,
							originRefJson: JSON.stringify(runtime.parent.origin),
							body: text,
							receivedAt: at,
						},
						fallbackPayload,
						...(holdReason ? { holdReason } : {}),
					};
				} else {
					const root = runtime.parent.root;
					admission = {
						kind: "lane",
						report: {
							reportId: runtime.reportId,
							parentName: runtime.parent.name,
							childName: name,
							childOpRef: runtime.opRef,
							body: text,
							root,
						},
						fallbackPayload: root
							? (buildDeliveryPayload(runtime.opRef, root.origin, text, runtime.deliveryId) ?? null)
							: null,
					};
				}
			}
			const settled = this.#settleWithTaskResult(
				observer.text,
				runtime.opRef,
				runtime.version,
				job,
				{ decision, settledAt: at, ...(output !== runtime.output ? { output } : {}) },
				admission,
			);
			if (!settled) return undefined;
			this.#finishWaiters(runtime.opRef);
			return settled;
		});

		if (result) {
			const settled = result.runtime;
			const parentLabel =
				settled.parent?.kind === "persona"
					? settled.parent.originKey
					: settled.parent?.kind === "lane"
						? `lane:${settled.parent.name}`
						: "none";
			console.error(`work_report decision=${settled.decision} parent=${parentLabel} reportId=${settled.reportId}`);
			if (settled.parent?.kind === "lane" && settled.decision === "reported")
				console.error(
					`lane_report_transition id=${settled.reportId} parent=${settled.parent.name} from=none to=pending reason=admitted`,
				);
			if (settled.parent?.kind === "lane" && settled.decision === "fallback")
				console.error(`lane_report_fallback id=${settled.reportId} parent=${settled.parent.name}`);
			if (settled.parent?.kind === "lane" && settled.decision === "no_target")
				console.error(`lane_report_undeliverable id=${settled.reportId} parent=${settled.parent.name}`);
			if (settled.decision === "wake_unaccepted") {
				const child = this.#db.laneReportGet(settled.wakeReportId!)!;
				const outcome = ["fallback", "undeliverable", "pending"].includes(child.state) ? "refused" : "ambiguous";
				console.error(
					`work_report decision=wake_unaccepted reportId=${settled.wakeReportId} childReportId=${child.report_id} outcome=${outcome}`,
				);
			}
			for (const payload of [result.fallbackPayload, result.childFallback]) {
				if (!payload) continue;
				try {
					this.#options.deliverFallback?.(payload);
				} catch {
					console.error(`work_fallback_delivery_failed deliveryId=${payload.deliveryId}`);
				}
			}
			if (settled.decision === "reported" && settled.parent?.kind === "persona") {
				try {
					this.#options.notifyPersona?.(settled.parent.originKey);
				} catch {
					console.error(`work_persona_nudge_failed origin=${settled.parent.originKey}`);
				}
			}
			const drain = async (parentName: string) => {
				try {
					await this.#drainLaneReports(parentName);
				} catch {
					console.error(`lane_report_drain_failed parent=${parentName}`);
				}
			};
			if (settled.parent?.kind === "lane") await drain(settled.parent.name);
			await drain(settled.sessionKey.slice("work/task/".length));
			if (result.requeuedParent) await drain(result.requeuedParent);
		}
		if (this.#current(observer) && this.#db.workAttemptGet(runtime.opRef)?.settledAt) {
			observer.tail?.setTurnRunning(false);
			await observer.tail?.close();
			observer.tail = undefined;
		}
	}
	#finishWaiters(opRef: string): void {
		for (const waiter of [...(this.#waiters.get(opRef) ?? [])]) waiter.finish();
	}
	async #drainLaneReports(parentName: string): Promise<void> {
		if (this.#stopped) return;
		if (this.#db.workTaskByLane(parentName)) return;
		await this.#port.runExclusive(workSessionKey(parentName), async () => {
			if (this.#stopped) return;
			let job: LaneJobRecord | undefined;
			let unavailable = this.#db.isBrokerQuarantined("work", laneJobIdentity(parentName).jobId);
			if (!unavailable) {
				try {
					job = this.#job(parentName, true);
				} catch {
					unavailable = true;
				}
			}
			for (let row of this.#db.laneReportsByParent(parentName)) {
				if (this.#stopped) return;
				if (["consumed", "fallback", "undeliverable"].includes(row.state)) continue;
				if (unavailable || !job) {
					await this.#resolveUnavailableLaneReport(row);
					continue;
				}
				if (row.state === "held") {
					if (row.claim_kind === "wake") {
						const runtime = this.#attemptByOpRef(row.claim_ref);
						if (runtime && hasAcceptanceEvidence(runtime) && this.#db.laneReportConsume(row.report_id, row.claim_ref!))
							this.#logLaneReportTransition(row, "held", "consumed", "wake_acceptance_proven");
					} else if (row.claim_kind === "steer") {
						await this.#replayLaneSteer(row, job);
					}
					continue;
				}
				if (row.state === "claimed" && row.claim_kind === "wake") {
					if (this.#attemptByOpRef(row.claim_ref)) continue;
					const open = job.attempts.find((attempt) => attempt.endedAt === undefined);
					if (open) {
						if (!this.#db.laneReportRequeue(row.report_id, row.claim_ref!)) continue;
						this.#logLaneReportTransition(row, "claimed", "pending", "wake_unprepared_parent_open");
						row = this.#db.laneReportGet(row.report_id)!;
					} else {
						await this.#wakeLaneReport(row, job);
						try {
							job = this.#job(parentName, true);
						} catch {
							unavailable = true;
							job = undefined;
						}
						continue;
					}
				}
				if (row.state === "claimed" && row.claim_kind === "steer") {
					await this.#replayLaneSteer(row, job);
					continue;
				}
				if (row.state !== "pending") continue;
				const open = job.attempts.find((attempt) => attempt.endedAt === undefined);
				if (open) {
					const runtime = this.#attemptByOpRef(open.opRef);
					if (runtime && !runtime.terminal && this.#binding(runtime))
						await this.#claimAndSteerLaneReport(row, job, runtime);
					continue;
				}
				if (job.state === "awaiting_operator" || job.state === "stalled") {
					this.#fallbackLaneReport(row, "parent_held");
					continue;
				}
				await this.#wakeLaneReport(row, job);
				try {
					job = this.#job(parentName, true);
				} catch {
					unavailable = true;
					job = undefined;
				}
			}
		});
	}

	#attemptByOpRef(opRef: string | null): WorkAttemptRuntime | undefined {
		if (opRef === null) return undefined;
		try {
			return this.#db.workAttemptGet(opRef);
		} catch {
			return undefined;
		}
	}

	#reportRoot(row: LaneReportRow): WorkReportRoot | null {
		return row.root_json === null ? null : (JSON.parse(row.root_json) as WorkReportRoot);
	}

	#laneReportText(row: LaneReportRow): string {
		return `[Report from child work lane ${row.child_name}; not from a human. Integrate it into your task; your own final answer still goes to your parent.]\n\n${row.body}`;
	}

	#laneFallbackPayload(row: LaneReportRow): ChatMessagePayload | undefined {
		const root = this.#reportRoot(row);
		if (root === null) return undefined;
		const { jobId } = laneJobIdentity(row.child_name);
		return buildDeliveryPayload(
			row.child_op_ref,
			root.origin,
			row.body,
			workAttemptDeliveryId(this.#db.instanceId, jobId, row.child_op_ref),
		);
	}

	#logLaneReportTransition(row: LaneReportRow, from: string, to: string, reason: string): void {
		console.error(
			`lane_report_transition id=${row.report_id} parent=${row.parent_name} from=${from} to=${to} reason=${reason}`,
		);
	}

	#fallbackLaneReport(row: LaneReportRow, reason: string): void {
		const payload = this.#laneFallbackPayload(row);
		if (payload === undefined) {
			if (this.#db.laneReportUndeliverable(row.report_id))
				this.#logLaneReportTransition(row, row.state, "undeliverable", reason);
			return;
		}
		if (!this.#db.laneReportFallback(row.report_id, payload)) return;
		this.#logLaneReportTransition(row, row.state, "fallback", reason);
		try {
			this.#options.deliverFallback?.(payload);
		} catch {
			console.error(`work_fallback_delivery_failed deliveryId=${payload.deliveryId}`);
		}
	}

	#resolveUnavailableLaneReport(row: LaneReportRow): void {
		if (row.state === "pending") {
			this.#fallbackLaneReport(row, "parent_quarantined");
			return;
		}
		if (row.state === "held") {
			if (row.claim_kind === "wake") {
				const runtime = this.#attemptByOpRef(row.claim_ref);
				if (runtime && hasAcceptanceEvidence(runtime) && this.#db.laneReportConsume(row.report_id, row.claim_ref!))
					this.#logLaneReportTransition(row, "held", "consumed", "wake_acceptance_proven");
			}
			return;
		}
		if (row.state !== "claimed") return;
		if (row.claim_kind === "wake" && !this.#attemptByOpRef(row.claim_ref)) {
			if (!this.#db.laneReportRequeue(row.report_id, row.claim_ref!)) return;
			this.#logLaneReportTransition(row, "claimed", "pending", "wake_unprepared_parent_quarantined");
			this.#fallbackLaneReport(this.#db.laneReportGet(row.report_id)!, "parent_quarantined");
			return;
		}
		if (this.#db.laneReportHold(row.report_id, "parent_quarantined_ambiguous"))
			this.#logLaneReportTransition(row, "claimed", "held", "parent_quarantined_ambiguous");
	}

	async #claimAndSteerLaneReport(row: LaneReportRow, job: LaneJobRecord, runtime: WorkAttemptRuntime): Promise<void> {
		const claimSeq = row.claim_seq + 1;
		const clientRef = laneSteerClientRef(row.report_id, claimSeq);
		const claimed = this.#db.laneReportClaim(row.report_id, "steer", clientRef, runtime.opRef);
		if (!claimed) return;
		this.#logLaneReportTransition(row, "pending", "claimed", "steer_claimed");
		await this.#sendLaneSteer(claimed, job, runtime);
	}

	async #replayLaneSteer(row: LaneReportRow, job: LaneJobRecord): Promise<void> {
		const runtime = this.#attemptByOpRef(row.claim_target_op_ref);
		if (!runtime) {
			if (this.#db.laneReportHold(row.report_id, "steer_acceptance_uncertain"))
				this.#logLaneReportTransition(row, row.state, "held", "steer_acceptance_uncertain");
			return;
		}
		await this.#sendLaneSteer(row, job, runtime);
	}

	async #sendLaneSteer(row: LaneReportRow, job: LaneJobRecord, runtime: WorkAttemptRuntime): Promise<void> {
		const clientRef = row.claim_ref!;
		try {
			await this.#port.steer({
				sessionId: runtime.sessionId,
				repo: job.lane.worktreePath,
				text: this.#laneReportText(row),
				clientRef,
			});
			if (this.#db.laneReportConsume(row.report_id, clientRef))
				this.#logLaneReportTransition(row, row.state, "consumed", "steer_accepted");
		} catch (error) {
			if (definitiveSteerRefusal(error)) {
				if (safeRefusal(error) === "session_not_found") {
					if (this.#db.laneReportHold(row.report_id, "steer_acceptance_uncertain"))
						this.#logLaneReportTransition(row, row.state, "held", "steer_acceptance_uncertain");
					return;
				}
				if (this.#db.laneReportRequeue(row.report_id, clientRef))
					this.#logLaneReportTransition(row, row.state, "pending", "steer_refused");
				return;
			}
			console.error(`lane_report_steer_hold id=${row.report_id} parent=${row.parent_name} clientRef=${clientRef}`);
		}
	}

	async #wakeLaneReport(row: LaneReportRow, job: LaneJobRecord): Promise<void> {
		let claimed = row;
		let opRef = row.claim_ref;
		if (row.state === "pending") {
			opRef = laneWakeOpRef(row.parent_name, row.report_id, row.claim_seq + 1);
			const result = this.#db.laneReportClaim(row.report_id, "wake", opRef, null);
			if (!result) return;
			claimed = result;
			this.#logLaneReportTransition(row, "pending", "claimed", "wake_claimed");
		}
		if (!opRef || claimed.claim_kind !== "wake" || claimed.claim_ref !== opRef) return;
		const last = job.attempts.at(-1);
		const inheritedParent = last ? (this.#attemptByOpRef(last.opRef)?.parent ?? null) : null;
		try {
			const outcome = await this.#startLocked(
				{
					name: row.parent_name,
					text: this.#laneReportText(row),
					cwd: job.lane.worktreePath,
					resume: false,
				},
				"start",
				{ parent: inheritedParent, opRef, wakeReportId: row.report_id },
			);
			if (outcome.started) return;
		} catch {
			if (this.#attemptByOpRef(opRef)) return;
		}
		if (this.#attemptByOpRef(opRef)) return;
		const fresh = this.#db.laneReportGet(row.report_id);
		if (!fresh || fresh.state !== "claimed") return;
		if (!this.#db.laneReportRequeue(row.report_id, opRef)) return;
		this.#logLaneReportTransition(fresh, "claimed", "pending", "wake_unprepared");
		const parentJob = this.#job(row.parent_name);
		if (parentJob && (parentJob.state === "awaiting_operator" || parentJob.state === "stalled"))
			this.#fallbackLaneReport(this.#db.laneReportGet(row.report_id)!, "parent_held");
	}

	/** Registers recovery work, never waits for a worker's terminal transition. */
	recover(): Promise<void> {
		if (this.#stopped) return Promise.resolve();
		if (this.#generationRecovery) return this.#generationRecovery;
		if (this.#recovery) return this.#recovery;
		const recovery = this.#recover().finally(() => {
			if (this.#recovery === recovery) this.#recovery = undefined;
		});
		this.#recovery = recovery;
		return recovery;
	}
	async #recover(): Promise<void> {
		let taskCursor = "";
		do {
			const page = this.#db.workTaskKeys(20, taskCursor);
			for (const key of page.keys) {
				if (this.#stopped) return;
				try {
					this.#recoverCoordinatorReport(key.taskId);
				} catch (error) {
					console.error(`work_review_recovery_held taskId=${key.taskId} reason=${failureReason(error)}`);
				}
			}
			if (!page.nextTaskId) break;
			taskCursor = page.nextTaskId;
		} while (!this.#stopped);
		const reportedInvalidAttempts = new Set<string>();
		const quarantineInvalidAttempt = (error: WorkAttemptStateError) => {
			if (!error.opRef || reportedInvalidAttempts.has(error.opRef)) return;
			console.error(`work_recovery_invalid_attempt opRef=${JSON.stringify(error.opRef)} assertion=${error.assertion}`);
			reportedInvalidAttempts.add(error.opRef);
			const jobId = this.#db.workAttemptJobId(error.opRef);
			if (jobId) this.#db.workAttemptQuarantineInvalid(jobId);
		};
		for (const row of this.#db.laneJobRows()) {
			if (this.#stopped) return;
			if (!row.lane_key.startsWith("work-")) continue;
			const name = row.lane_key.slice(5);
			await this.#port.runExclusive(workSessionKey(name), async () => {
				if (this.#stopped) return;
				let job: LaneJobRecord | undefined;
				try {
					job = this.#job(name);
				} catch {
					return;
				}
				const open = job?.attempts.find((attempt) => attempt.endedAt === undefined);
				if (!job || !open) return;
				try {
					if (this.#db.workAttemptGet(open.opRef)) return;
				} catch (error) {
					if (!(error instanceof WorkAttemptStateError)) throw error;
					quarantineInvalidAttempt(error);
					return;
				}
				try {
					if (this.#db.workTaskByLane(name)) {
						console.error(`work_task_recovery_held lane=${name} reason=missing_original_runtime`);
						return;
					}
				} catch {
					console.error(`work_task_recovery_held lane=${name} reason=invalid_task`);
					return;
				}
				this.#db.workAttemptPrepare(
					makeRuntime(
						this.#db,
						name,
						open.sessionId,
						this.#db.getSessionRecord(workSessionKey(name))?.epoch ?? 0,
						job.lane.worktreePath,
						open.startedAt,
						open.opRef,
						"historical",
						null,
						null,
					),
					job,
				);
			});
		}
		let after = "";
		while (!this.#stopped) {
			let invalidAfter = after;
			const rows = this.#db.workAttemptOpen(100, after, (error) => {
				if (error.opRef && error.opRef > invalidAfter) invalidAfter = error.opRef;
				quarantineInvalidAttempt(error);
			});
			const lastValid = rows.at(-1)?.opRef ?? after;
			const nextAfter = invalidAfter > lastValid ? invalidAfter : lastValid;
			if (!rows.length && nextAfter === after) break;
			for (const runtime of rows) {
				const prior = this.#observers.get(runtime.opRef);
				if (prior && this.#writeCurrent(prior)) continue;
				if (prior) {
					prior.abort.abort();
					if (prior.timer) clearTimeout(prior.timer);
					await prior.attaching;
					await prior.task;
					await prior.tail?.close();
				}
				if (this.#stopped) return;
				const observer = this.#register(runtime);
				if (runtime.terminal) {
					this.#schedule(observer, 0);
					continue;
				}
				let status: PromptStatusBody | undefined;
				let statusError: unknown;
				try {
					status = await this.#query(runtime);
				} catch (error) {
					/* Liveness decides whether authority can be recovered. */
					statusError = error;
					this.#observeFailure(observer);
				}
				if (!this.#current(observer)) continue;
				const terminal = status && terminalEvidence(status, this.#at());
				if (terminal && this.#writeCurrent(observer)) {
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						...(provesAcceptance(status!)
							? {
									sendPhase: "accepted" as const,
									sendEvidence: runtime.sendEvidence ?? { source: "status" as const, observedAt: this.#at() },
								}
							: {}),
						terminal,
					});
					this.#schedule(observer, 0);
					continue;
				}
				let live: { live: boolean | undefined; disowned: boolean } = { live: undefined, disowned: false };
				try {
					live = (await this.#port.liveness?.({ sessionId: runtime.sessionId, repo: runtime.cwd })) ?? live;
				} catch {
					/* Indeterminate authority is not a replay permit. */
				}
				if (!this.#current(observer)) continue;
				if (live.live === true && !live.disowned && this.#binding(runtime)) {
					this.#attach(observer);
					this.#schedule(observer, 0);
					continue;
				}
				if (this.#writeCurrent(observer)) {
					// The router itself answered session_unavailable for the operation
					// and inspect agrees: the host died with the attempt open.
					const hostLost = isSessionUnavailable(statusError) && (live.disowned || live.live === false);
					if (hostLost)
						console.error(`work_host_lost opRef=${runtime.opRef} session=${runtime.sessionId} source=recovery`);
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						terminal: {
							kind: "local",
							observedAt: this.#at(),
							reasonCode: hostLost
								? "host_lost"
								: live.disowned || !this.#binding(runtime)
									? "session_disowned"
									: live.live === false
										? "session_dead"
										: "recovery_indeterminate",
						},
					});
					this.#schedule(observer, 0);
				}
			}
			after = nextAfter;
		}
		if (this.#stopped) return;
		let afterTaskId = "";
		for (;;) {
			const page = this.#db.workTaskKeys(20, afterTaskId);
			for (const key of page.keys) {
				if (this.#stopped) return;
				try {
					const task = this.#task(key.taskId);
					await this.#port.runExclusive(workSessionKey(task.laneName), async () => {
						const current = this.#task(task.taskId);
						if (current.surfacePhase !== "bound") return;
						if (current.dispatchPhase === "pending") await this.#dispatchTask(current);
						else await this.#drainTaskControlsLocked(current);
					});
				} catch {
					this.#observeLinkedUnavailable(key.taskId);
					console.error(`work_task_recovery_held taskId=${key.taskId}`);
				}
			}
			if (!page.nextTaskId) break;
			afterTaskId = page.nextTaskId;
		}
		const parents = new Set([
			...this.#db.laneReportParentNames(),
			...this.#db
				.laneJobRows(true)
				.filter((row) => row.lane_key.startsWith("work-"))
				.map((row) => row.lane_key.slice("work-".length)),
		]);
		for (const parentName of parents) {
			if (this.#stopped) return;
			try {
				await this.#drainLaneReports(parentName);
			} catch {
				// Task validation and report lookup can fail before a drain acquires
				// its lane lock. Preserve that parent's debt without blocking peers.
				console.error(`lane_report_drain_failed parent=${parentName}`);
			}
		}
	}
	onBrokerGeneration(): Promise<void> {
		if (this.#generationRecovery) return this.#generationRecovery;
		const prior = this.#recovery;
		const recovery = (async () => {
			await prior;
			let generation: number;
			do {
				generation = this.#options.brokerGeneration?.() ?? 0;
				await this.#detachObservers();
				if (!this.#stopped) await this.#recover();
			} while (!this.#stopped && generation !== (this.#options.brokerGeneration?.() ?? 0));
		})();
		this.#generationRecovery = recovery.finally(() => {
			this.#generationRecovery = undefined;
		});
		this.#recovery = this.#generationRecovery.finally(() => {
			this.#recovery = undefined;
		});
		return this.#recovery;
	}
	#wait(opRef: string, owner: object, signal?: AbortSignal): Promise<void> {
		this.#live();
		if (this.#detachedOwners.has(owner)) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
		if (this.#db.workAttemptGet(opRef)?.settledAt) return Promise.resolve();
		return new Promise<void>((resolve, reject) => {
			const set = this.#waiters.get(opRef) ?? new Set<Waiter>();
			this.#waiters.set(opRef, set);
			let timer: ReturnType<typeof setTimeout> | undefined;
			const abort = () =>
				waiter.finish(
					new ProtocolError("verb_failed", "work wait detached; attempt remains observable", {
						reasonCode: "work_wait_detached",
					}),
				);
			const waiter: Waiter = {
				owner,
				finish: (error) => {
					if (!set.delete(waiter)) return;
					if (!set.size) this.#waiters.delete(opRef);
					if (timer) clearTimeout(timer);
					signal?.removeEventListener("abort", abort);
					error ? reject(error) : resolve();
				},
			};
			set.add(waiter);
			timer = setTimeout(
				() =>
					waiter.finish(
						workError(
							"work wait timed out; attempt remains observable",
							"work_wait_timeout",
							this.#db.workAttemptGet(opRef)!,
						),
					),
				this.#options.waitTimeoutMs ?? 30 * 60_000,
			);
			signal?.addEventListener("abort", abort, { once: true });
			if (signal?.aborted) abort();
		});
	}
	detachWaiters(owner: object): void {
		this.#detachedOwners.add(owner);
		for (const set of this.#waiters.values())
			for (const waiter of [...set])
				if (waiter.owner === owner) waiter.finish(new ProtocolError("gateway_shutting_down", "gateway is stopping"));
	}
	async #detachObservers(): Promise<void> {
		const observers = [...this.#observers.values()];
		for (const observer of observers) {
			observer.abort.abort();
			if (observer.timer) clearTimeout(observer.timer);
		}
		await Promise.all(
			observers.map(async (observer) => {
				await observer.attaching;
				await observer.task;
				await observer.tail?.close();
			}),
		);
		this.#observers.clear();
	}
	stop(): Promise<void> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#stopped = true;
		for (const set of this.#waiters.values())
			for (const waiter of [...set]) waiter.finish(new ProtocolError("gateway_shutting_down", "gateway is stopping"));
		const lanesStopped = this.#options.lanes.stop();
		this.#stopPromise = (async () => {
			await this.#detachObservers();
			await this.#recovery;
			await lanesStopped;
			owners.delete(this.#db);
		})();
		return this.#stopPromise;
	}
}

export function laneSystemNotice(input: {
	readonly name: string;
	readonly parent: WorkParent | null;
	readonly allowNested: boolean;
}): string {
	const parent =
		input.parent?.kind === "persona"
			? `persona conversation ${input.parent.originKey}`
			: input.parent?.kind === "lane"
				? `work lane ${input.parent.name}`
				: "none (status-only)";
	const nested = input.allowNested ? "allowed" : "refused";
	return [
		`You are work lane ${input.name}.`,
		`Parent: ${parent}.`,
		"Your final answer goes to your parent as an internal report, never to a human.",
		"Never post to chat or use gajaeway chat.",
		`Nested work.start and work.run are ${nested} (work.allowNested=${input.allowNested}).`,
	].join("\n");
}
function makeRuntime(
	db: GatewayDatabase,
	name: string,
	sessionId: string,
	epoch: number,
	cwd: string,
	startedAt: string,
	opRef: string,
	mode: WorkAttemptRuntime["mode"],
	parent: WorkParent | null,
	wakeReportId: string | null,
	noticeHash: string | null = null,
): WorkAttemptRuntime {
	const { jobId, laneKey } = laneJobIdentity(name);
	return {
		jobId,
		laneKey,
		sessionKey: workSessionKey(name),
		sessionId,
		epoch,
		cwd,
		startedAt,
		opRef,
		mode,
		parent: parent ? structuredClone(parent) : null,
		reportId: workAttemptReportId(db.instanceId, jobId, opRef),
		wakeReportId,
		noticeHash,
		sendPhase: mode === "historical" ? "uncertain" : "prepared",
		sendEvidence: null,
		terminal: null,
		output: pendingOutput(),
		deliveryId: workAttemptDeliveryId(db.instanceId, jobId, opRef),
		decision: "undecided",
		settledAt: null,
		version: 0,
	};
}
function invalid(field: string): never {
	throw new ProtocolError("invalid_params", "invalid work parameters", { reasonCode: "invalid_work_params", field });
}
function parseName(params: unknown): string {
	const name = (params as { name?: unknown } | null)?.name;
	if (typeof name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name)) invalid("name");
	return name;
}

function laneReportClaimHash(reportId: string, claimSeq: number): string {
	return createHash("sha256").update(`${reportId}|${claimSeq}`).digest("hex");
}

function laneSteerClientRef(reportId: string, claimSeq: number): string {
	return `gw-lr-${laneReportClaimHash(reportId, claimSeq).slice(0, 32)}`;
}

function laneWakeOpRef(parentName: string, reportId: string, claimSeq: number): string {
	const slug = parentName.toLowerCase().replace(/[^a-z0-9]+/g, "-");
	return `gw-work-${slug}-lr-${laneReportClaimHash(reportId, claimSeq).slice(0, 24)}`;
}
function parseInput(params: unknown): WorkInput {
	const inputFields = ["name", "text", "cwd", "resume", "model", "callerSessionId"];
	const input = params as Record<string, unknown>;
	if (input && typeof input === "object" && !Array.isArray(input))
		for (const field of Object.keys(input)) if (!inputFields.includes(field)) invalid(field);
	const name = parseName(params);
	if (typeof input.text !== "string" || !input.text) invalid("text");
	if (input.cwd !== undefined) {
		if (typeof input.cwd !== "string" || !input.cwd.startsWith("/") || input.cwd.length > 4096) invalid("cwd");
		for (let index = 0; index < input.cwd.length; index++) {
			if (input.cwd.charCodeAt(index) < 32) invalid("cwd");
		}
	}
	if (input.resume !== undefined && typeof input.resume !== "boolean") invalid("resume");
	let model: GjcModelSelection | undefined;
	if (input.model !== undefined) {
		if (typeof input.model === "string" && input.model) model = input.model;
		else if (
			input.model &&
			typeof input.model === "object" &&
			!Array.isArray(input.model) &&
			Object.keys(input.model).length === 1 &&
			typeof (input.model as { preset?: unknown }).preset === "string" &&
			(input.model as { preset: string }).preset
		)
			model = { preset: (input.model as { preset: string }).preset };
		else invalid("model");
	}
	let callerSessionId: string | undefined;
	if (input.callerSessionId !== undefined) {
		if (typeof input.callerSessionId !== "string" || !/^[A-Za-z0-9-]{1,128}$/.test(input.callerSessionId))
			invalid("callerSessionId");
		callerSessionId = input.callerSessionId;
	}
	return {
		name,
		text: input.text,
		cwd: (input.cwd as string) ?? process.cwd(),
		resume: input.resume === true,
		model,
		...(callerSessionId === undefined ? {} : { callerSessionId }),
	};
}
function workError(
	message: string,
	reasonCode: string,
	runtime: { jobId: string; opRef: string; sessionId: string },
	clientRef?: string,
): ProtocolError {
	return new ProtocolError("verb_failed", message, {
		reasonCode,
		jobId: runtime.jobId,
		opRef: runtime.opRef,
		sessionId: runtime.sessionId,
		...(clientRef ? { clientRef } : {}),
	});
}
/** Bounded, secret-free error class and message for a log line. */
function failureReason(error: unknown): string {
	const text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
	return sanitizeDiagnostic(text).slice(0, 200) || "unknown";
}
function safeRefusal(error: unknown): string {
	const code =
		error instanceof OpRefRejectedError
			? error.code
			: error instanceof GjcCliError
				? envelopeErrorCode(error.details)
				: undefined;
	return code && refusalCodes.has(code) ? code : "sdk_refused";
}
function definitiveRefusal(error: unknown): boolean {
	return (
		error instanceof OpRefRejectedError ||
		(error instanceof GjcCliError && refusalCodes.has(envelopeErrorCode(error.details) ?? ""))
	);
}
function definitiveSteerRefusal(error: unknown): boolean {
	if (
		error instanceof GjcCliError &&
		error.details &&
		typeof error.details === "object" &&
		(error.details as { refused?: unknown }).refused === true
	)
		return true;
	return definitiveRefusal(error);
}
function safeStatus(status: PromptStatusBody): PromptStatusBody {
	const result: { -readonly [K in keyof PromptStatusBody]: PromptStatusBody[K] } = { status: status.status };
	for (const key of ["commandId", "turnId", "clientRef"] as const) {
		const value = status[key];
		if (value !== undefined) {
			if (typeof value !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(value))
				throw new Error("invalid status identity");
			result[key] = value;
		}
	}
	for (const key of ["acceptedAt", "startedAt", "terminalAt"] as const) {
		const value = status[key];
		if (value !== undefined) {
			if (!Number.isFinite(value)) throw new Error("invalid status time");
			result[key] = value;
		}
	}
	if (status.receiptState !== undefined) {
		if (!["absent", "present", "missing", "unknown"].includes(status.receiptState))
			throw new Error("invalid receipt state");
		result.receiptState = status.receiptState;
	}
	if (status.outcome)
		result.outcome = {
			...(status.outcome.reason
				? { reason: reasons.has(status.outcome.reason) ? status.outcome.reason : "stopped_incomplete" }
				: {}),
			...(["stopped", "success", "failure", "cancelled", "completed"].includes(status.outcome.kind ?? "")
				? { kind: status.outcome.kind }
				: {}),
			...(["client_cancel", "runtime", "broker", "receipt"].includes(status.outcome.provenance ?? "")
				? { provenance: status.outcome.provenance }
				: {}),
		};
	if (status.error) result.error = { code: reasons.has(status.error.code ?? "") ? status.error.code : "sdk_failed" };
	return result;
}
/** Called only after #query has validated the operation and client reference. */
function provesAcceptance(status: PromptStatusBody): boolean {
	return (
		status.status === "accepted" ||
		status.status === "in_flight" ||
		((status.status === "terminal_ok" || status.status === "failed") && status.receiptState === "present")
	);
}

function terminalEvidence(status: PromptStatusBody, observedAt: string): WorkAttemptTerminalEvidence | undefined {
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
/** Bound the leading excerpt without splitting a Unicode scalar. */
export function utf8Prefix(text: string, maxBytes = 2048): string {
	let bytes = 0;
	let end = 0;
	for (const scalar of text) {
		const length = Buffer.byteLength(scalar, "utf8");
		if (bytes + length > maxBytes) break;
		bytes += length;
		end += scalar.length;
	}
	return text.slice(0, end);
}
export type LaneCommit = { readonly sha: string; readonly subject: string; readonly committed_at: string };

/**
 * The lane's HEAD commit (issue #67): the only progress signal that survives an
 * op dying. Null when the worktree yields no commit; never invented. Async so a
 * `work.jobs` over hundreds of lanes never blocks the event loop (#407).
 */
export async function laneLastCommit(worktreePath: string): Promise<LaneCommit | null> {
	try {
		const child = Bun.spawn(["git", "-C", worktreePath, "log", "-1", "--format=%H%x00%cI%x00%s"], {
			stdout: "pipe",
			stderr: "ignore",
		});
		const [stdout, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
		if (exitCode !== 0) return null;
		const [sha, at, subject] = stdout.replace(/\n$/, "").split("\0");
		const committed = Date.parse(at ?? "");
		if (!sha || !/^[0-9a-f]{40}$/.test(sha) || subject === undefined || Number.isNaN(committed)) return null;
		return { sha, subject: utf8Prefix(subject, 256), committed_at: new Date(committed).toISOString() };
	} catch {
		return null;
	}
}

/** HEAD commits for many worktrees: one git call per distinct path, at most `concurrency` at a time. */
export async function laneLastCommits(
	worktreePaths: readonly string[],
	concurrency = 16,
): Promise<Map<string, LaneCommit | null>> {
	const unique = [...new Set(worktreePaths)];
	const result = new Map<string, LaneCommit | null>();
	let next = 0;
	const worker = async () => {
		while (next < unique.length) {
			const path = unique[next++]!;
			result.set(path, await laneLastCommit(path));
		}
	};
	await Promise.all(Array.from({ length: Math.min(concurrency, unique.length) }, worker));
	return result;
}

/** Shapes the complete lane report under the 2048-byte UTF-8 storage budget. */
export function reportText(
	name: string,
	endState: string,
	reason: string,
	opRef: string,
	output: WorkAttemptRuntime["output"],
): string {
	const label = endState === "completed" ? "completed" : endState === "failed" ? "failed" : "attempt_ended";
	const lead = reason === "end_turn" ? "" : `${reason}: `;
	const head = `[lane ${name}] ${label}: ${lead}`;
	let body =
		output.disposition === "unavailable"
			? reason === "terminal_missing_receipt"
				? `final_response_missing opRef=${opRef}`
				: "output_unavailable"
			: (output.excerpt ?? "");
	// Include transport cause details if present
	if (reason === "terminal_missing_receipt" && output.transportCause) {
		const transport = output.transportCause;
		const parts: string[] = [body, `cause=transport`];
		if (transport.nativeErrorCode) parts.push(transport.nativeErrorCode);
		if (transport.http2RstCode !== undefined) parts.push(`http2RstCode=${transport.http2RstCode}`);
		if (transport.status !== undefined) parts.push(`status=${transport.status}`);
		if (transport.requestBytes !== undefined) parts.push(`requestBytes=${transport.requestBytes}`);
		if (transport.retryMaxAttempts !== undefined) parts.push(`retryMaxAttempts=${transport.retryMaxAttempts}`);
		if (transport.endpointClass !== undefined) parts.push(`endpointClass=${transport.endpointClass}`);
		body = parts.join(" ");
	}
	const content = utf8Prefix(body, 2048 - Buffer.byteLength(head, "utf8"));
	const text = head + content;
	if (Buffer.byteLength(text, "utf8") > 2048) throw new Error("lane report exceeded its UTF-8 byte budget");
	return text;
}
async function collectRepoFacts(
	worktreePath: string,
): Promise<{ headSha?: string; dirtyFiles: number; branch?: string } | undefined> {
	try {
		const head = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "HEAD"], { stdout: "pipe", stderr: "pipe" });
		const branch = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const status = Bun.spawnSync(["git", "-C", worktreePath, "status", "--porcelain"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const headSha = head.stdout.toString().trim();
		if (head.exitCode !== 0 || status.exitCode !== 0 || !/^[0-9a-f]{40}$/.test(headSha)) return undefined;
		return {
			headSha,
			dirtyFiles: status.stdout
				.toString()
				.split("\n")
				.filter((line) => line.trim()).length,
			branch: branch.stdout.toString().trim() || undefined,
		};
	} catch {
		return undefined;
	}
}

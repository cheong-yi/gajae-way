import { Database } from "bun:sqlite";
import { afterEach, expect, spyOn, test } from "bun:test";
import { readFileSync, realpathSync, writeFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import { closeAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { LaneGovernor, laneJobIdentity, workSessionKey } from "../src/orchestrator/lane-governor";
import {
	SESSION_CREATE_READINESS_MS,
	sessionCreateRef,
	type SessionBindInput,
	type SessionPort,
	type SessionSteerStatusInput,
	type SessionSteerStatusResult,
} from "../src/orchestrator/session-port";
import { type MappedWorkEvent, WorkLaneManager, type WorkLaneManagerOptions } from "../src/orchestrator/work-lane";
import { admitDedicatedWorktree, assertManagedWorktreeIgnored } from "../src/orchestrator/worktree-admission";
import { buildWorkTaskContext, buildWorkTaskReviewTurnContext } from "../src/server/work-task-context";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort, steerRefused } from "./session-port.fake";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const close of cleanup.splice(0).reverse()) await close();
});
const coordinator: OriginRef = { platform: "discord", kind: "channel", conversationId: "100", boundaryId: "1" };
const thread = {
	platform: "discord",
	kind: "thread",
	conversationId: "200",
	parentId: "100",
	boundaryId: "1",
} as const;
const taskId = "b7654d21-6806-4fdc-89ef-e2f9c038e4f9";
const claimId = "29e0f8d7-b069-44bc-a747-5890b89ea174";
const name = `fm-${taskId}`;
const at = "2026-10-06T05:00:00.000Z";
const context = {
	stableOrigin: coordinator,
	evidence: {
		principalId: "owner",
		origin: coordinator,
		eventId: "assignment",
		editId: null,
		evidenceAt: at,
		observedAt: at,
	},
};
class TaskPort extends ScriptedSessionPort {
	readonly lookups: SessionSteerStatusInput[] = [];
	lookup: SessionSteerStatusResult | undefined;
	async lookupSteerStatus(input: SessionSteerStatusInput): Promise<SessionSteerStatusResult> {
		this.lookups.push(input);
		return this.lookup ?? { status: "unknown", clientRef: input.clientRef };
	}
}
async function fixture(
	options: Partial<WorkLaneManagerOptions> = {},
	portOptions: ConstructorParameters<typeof ScriptedSessionPort>[0] = {},
	owned = false,
) {
	const directory = await mkdtemp(join(tmpdir(), "firstmate-lane-"));
	const db = await GatewayDatabase.open(join(directory, "gateway.db"));
	/** Native managed-allocation fixture state: one original result per persisted idempotency key. */
	const native = {
		allocations: new Map<string, { readonly sessionId: string; readonly executionCwd: string }>(),
		/** Simulates a failed/uncertain create before any checkout is allocated. */
		failCreate: false,
		/** Returns an arbitrary SDK execution identity instead of allocating the default nested checkout. */
		override: undefined as undefined | ((input: SessionBindInput) => string),
	};
	const port = new TaskPort({
		...portOptions,
		onBind: async (input) => {
			const sessionId = db.getSessionRecord(input.originKey)?.sessionId ?? crypto.randomUUID();
			if (!owned) db.putSession(input.originKey, sessionId);
			const managed = input.managedWorktree;
			if (!managed) return sessionId;
			const original = native.allocations.get(managed.idempotencyKey);
			// Lookup-only (or a repeated create attempt) recovers the original result; it never allocates again.
			if (original) return original;
			if (managed.lookupOnly) return sessionId;
			if (native.failCreate) throw new Error("uncertain native create outcome");
			const executionCwd = native.override
				? native.override(input)
				: allocateNativeCheckout(input.repo, managed.idempotencyKey);
			const identity = { sessionId, executionCwd };
			native.allocations.set(managed.idempotencyKey, identity);
			return identity;
		},
	});
	if (owned) {
		attachTestBrokerOwnership(db, port, join(directory, "canonical-agent"));
		db.recordOwnedBinding({
			originKey: originKey(coordinator),
			sessionId: "persona-session",
			epoch: 0,
			repo: directory,
			authority: db.inspectBrokerAuthority().authority!,
		});
	} else db.putSession(originKey(coordinator), "persona-session");
	const lanes = new LaneGovernor({ database: db, sessionPort: port, maxLanes: 4 });
	const settings = { database: db, port, lanes, pollMs: 5, taskSurfaceAvailable: () => true, ...options };
	let manager = new WorkLaneManager(settings);
	cleanup.push(async () => {
		await manager.stop();
		db.close();
		await rm(directory, { recursive: true, force: true });
	});
	const input = {
		name,
		text: "Investigate the issue without editing",
		cwd: directory,
		callerSessionId: "persona-session",
		task: { taskId, kind: "read_only" as const, surface: { parentOrigin: coordinator } },
	};
	return {
		directory,
		db,
		port,
		lanes,
		input,
		native,
		get manager() {
			return manager;
		},
		restart: async () => {
			await manager.stop();
			manager = new WorkLaneManager(settings);
			await manager.recover();
		},
		admit: () => manager.start(input, context),
		bind: async (confirm = true) => {
			await manager.threadClaim({ taskId, claimId });
			const result = await manager.threadBind({ taskId, claimId, outcome: { kind: "bound", origin: thread } });
			if (confirm) {
				const marker = db.workTaskSourceGet(`activation-${taskId}`)!;
				// Models a joined, authenticated adapter receipt; not a claim that the wire confirm supports it yet.
				db.withTransaction(() => {
					db.deliveryUpdate(marker.deliveryId!, "confirmed");
					manager.recordTaskActivationInTransaction({
						taskId,
						deliveryId: marker.deliveryId!,
						origin: thread,
						messageId: "marker-1",
						evidenceAt: at,
						observedAt: at,
						principalId: "discord-adapter",
					});
				});
				await manager.drainTaskControls(taskId);
			}
			return result;
		},
	};
}
function event(eventId = "event-1", extra: Partial<MappedWorkEvent> = {}): MappedWorkEvent {
	return {
		origin: thread,
		authorId: "owner",
		eventId,
		platformTimestamp: "2026-10-06T05:00:00.001Z",
		body: "Focus on the failure evidence",
		kind: "steer",
		...extra,
	};
}

function executionSources(db: GatewayDatabase) {
	return db.workTaskSources(taskId)!.sources.filter((source) => source.sourceId.startsWith("execution-"));
}
function runGit(cwd: string, ...args: string[]): string {
	const result = Bun.spawnSync(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe" });
	if (result.exitCode !== 0) throw new Error(result.stderr.toString());
	return result.stdout.toString().trim();
}
function gitExitCode(cwd: string, ...args: string[]): number {
	return Bun.spawnSync(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe" }).exitCode;
}
/** A fixture repository ignoring GJC's default `/.worktrees` nested checkout bucket. */
async function initRepo(directory: string): Promise<void> {
	await mkdir(directory, { recursive: true });
	runGit(directory, "init");
	await writeFile(join(directory, ".gitignore"), ".worktrees/\n");
	runGit(directory, "add", ".gitignore");
	runGit(directory, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "fixture");
}
async function directoryExists(path: string): Promise<boolean> {
	try {
		await readdir(path);
		return true;
	} catch {
		return false;
	}
}
/**
 * The SDK's native default: a distinct real registered linked checkout under
 * `<source>/.worktrees`, with `/.worktrees` ignored inside the source checkout.
 */
function allocateNativeCheckout(source: string, idempotencyKey: string): string {
	const root = realpathSync(source);
	const ignorePath = join(root, ".gitignore");
	let ignore = "";
	try {
		ignore = readFileSync(ignorePath, "utf8");
	} catch {
		/* A source checkout without an ignore file gets one. */
	}
	if (!ignore.split("\n").some((line) => line.trim() === ".worktrees/"))
		writeFileSync(
			ignorePath,
			ignore === "" || ignore.endsWith("\n") ? `${ignore}.worktrees/\n` : `${ignore}\n.worktrees/\n`,
		);
	const checkout = join(root, ".worktrees", idempotencyKey);
	const result = Bun.spawnSync(["git", "-C", root, "worktree", "add", checkout], {
		stdout: "pipe",
		stderr: "pipe",
		env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_"))),
	});
	if (result.exitCode !== 0) throw new Error(`fixture native checkout failed: ${result.stderr.toString()}`);
	return checkout;
}

for (const availability of ["healthy", "held", "absent"] as const)
	test(`original final is independent of ${availability} coordinator and missing original handoff recovers without replay`, async () => {
		let held = availability === "held";
		const wakes: string[] = [];
		const f = await fixture({
			personaHold: () => (held ? "fixture-held" : undefined),
			notifyPersona: (key) => wakes.push(key),
		});
		await f.admit();
		await f.bind();
		const task = f.db.workTaskGet(taskId)!;
		const raw = new Database(join(f.directory, "gateway.db"));
		cleanup.push(async () => raw.close());
		if (availability === "absent") raw.query("DELETE FROM sessions WHERE origin_key = ?").run(originKey(coordinator));
		const answer = "한".repeat(5449) + "x\nOwner decision: choose red or blue?";
		expect(Buffer.byteLength(answer)).toBe(16384);
		f.port.complete(task.opRef, answer);
		await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
		const source = f.db.workTaskOriginalSource(taskId)!;
		expect(source.body).toBe(answer);
		expect(source.deliveryId).not.toBeNull();
		const original = f.db.workAttemptGet(task.opRef)!;
		expect(original.decision).toBe(availability === "healthy" ? "reported" : "fallback");
		const historicalDelivery = f.db.deliveryGet(original.deliveryId);
		held = false;
		f.db.putSession(originKey(coordinator), "persona-session");
		f.db.updateActivity(originKey(coordinator), JSON.stringify(coordinator));
		await f.restart();
		const report = raw
			.query<{ body: string; state: string }, [string]>("SELECT body, state FROM inbound_messages WHERE message_id = ?")
			.get(original.reportId)!;
		expect(report.state).toBe("pending");
		if (historicalDelivery) expect(report.body).toBe(JSON.parse(historicalDelivery.payload_json).text);
		expect(f.db.workAttemptGet(task.opRef)).toEqual(original);
		expect(f.db.deliveryGet(original.deliveryId)).toEqual(historicalDelivery);
		const beforeWakes = wakes.length;
		raw
			.query("UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE message_id = ?")
			.run(original.reportId);
		await f.restart();
		await f.manager.recoverTaskReport(taskId);
		expect(
			raw
				.query<{ state: string }, [string]>("SELECT state FROM inbound_messages WHERE message_id = ?")
				.get(original.reportId)?.state,
		).toBe("done");
		expect(wakes.length).toBe(beforeWakes);
		expect(f.db.workTaskPendingReviews(originKey(coordinator)).items[0]?.pending).toBe(true);
		expect(f.db.workTaskOriginalSource(taskId)).toEqual(source);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.resumes).toHaveLength(0);
	});

test("coordinator review fences exact original and retains unanswered question independently of later quiet review", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.complete(task.opRef, "Original answer. Owner must select an option.");
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
	const settledTask = f.db.workTaskGet(taskId);
	f.db.updateActivity(originKey(coordinator), JSON.stringify(coordinator));
	const original = f.db.workTaskOriginalSource(taskId)!;
	const input = {
		taskId,
		expectedOpRef: task.opRef,
		reportId: original.reportId!,
		sourceId: original.sourceId,
		contentHash: original.contentHash,
		reviewId: crypto.randomUUID(),
		expectedReviewId: null,
		callerSessionId: "persona-session",
		callerEpoch: 0,
		fullRead: true as const,
		disposition: "owner_question" as const,
		rationale: "Complete original inspected; scope requires owner's selection.",
		question: "Choose red or blue?",
	};
	const deliveriesBefore = f.db.deliveryRows().length;
	expect(() => f.db.workTaskReview({ ...input, callerEpoch: 1 })).toThrow("identity");
	expect(() => f.db.workTaskReview({ ...input, contentHash: "0".repeat(64) })).toThrow("identity");
	expect(() => f.db.workTaskReview({ ...input, expectedOpRef: "wrong-op" })).toThrow("identity");
	expect(() => f.db.workTaskReview({ ...input, reportId: "wrong-report" })).toThrow("identity");
	expect(() => f.db.workTaskReview({ ...input, callerSessionId: task.sessionId! })).toThrow("identity");
	const foreign = { ...coordinator, conversationId: "999" } as OriginRef;
	f.db.putSession(originKey(foreign), "foreign-persona");
	f.db.updateActivity(originKey(foreign), JSON.stringify(foreign));
	expect(() => f.db.workTaskReview({ ...input, callerSessionId: "foreign-persona" })).toThrow("identity");
	const write = spyOn(f.db, "deliveryCreateInTransaction").mockImplementation(() => {
		throw new Error("fixture notice crash");
	});
	expect(() => f.db.workTaskReview(input)).toThrow("fixture notice crash");
	expect(f.db.workTaskSourceGet(`review-${input.reviewId}`)).toBeUndefined();
	expect(f.db.workTaskReviewLocator(taskId)?.pending).toBe(true);
	write.mockRestore();
	const recorded = f.db.workTaskReview(input);
	expect(recorded.execution).toBe("none");
	expect(f.db.deliveryRows()).toHaveLength(deliveriesBefore + 1);
	const notice = f.db.deliveryGet(recorded.deliveryId!)!;
	expect(notice.origin_key).toBe(originKey(coordinator));
	expect(JSON.parse(notice.payload_json).text).toContain(input.question);
	expect(f.db.workTaskReview(input).disposition).toBe("duplicate");
	expect(f.db.deliveryRows()).toHaveLength(deliveriesBefore + 1);
	expect(() => f.db.workTaskReview({ ...input, question: "Changed?" })).toThrow("conflict");
	expect(() => f.db.workTaskReview({ ...input, reviewId: crypto.randomUUID() })).toThrow("order");
	const { question: _question, ...quiet } = input;
	f.db.workTaskReview({
		...quiet,
		reviewId: crypto.randomUUID(),
		expectedReviewId: input.reviewId,
		disposition: "no_exception",
		rationale: "Review correction only; not an answer to the earlier question.",
	});
	await f.restart();
	const state = f.db.workTaskReviewLocator(taskId)!;
	expect(state.pending).toBe(false);
	expect(state.ownerQuestions).toEqual([
		{ reviewId: input.reviewId, question: input.question, deliveryId: recorded.deliveryId! },
	]);
	expect(f.db.workTaskPendingReviews(originKey(coordinator)).items).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(deliveriesBefore + 1);
	f.db.deliveryUpdate(recorded.deliveryId!, "confirmed");
	f.db.retentionSweep(new Date("2099-01-01T00:00:00Z"));
	expect(f.db.deliveryGet(recorded.deliveryId!)?.payload_json).toBe(notice.payload_json);
	expect(f.db.workTaskReviewLocator(taskId)?.ownerQuestions).toHaveLength(1);
	expect(f.db.workTaskGet(taskId)).toEqual(settledTask);
	expect(f.port.sends).toHaveLength(1);
});

test("pending semantic review survives safe execution resource release", async () => {
	const f = await fixture({}, {}, true);
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.complete(task.opRef, "Complete original retained; no semantic review recorded.");
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
	const original = f.db.workTaskOriginalSource(taskId);
	expect(f.db.workTaskReviewLocator(taskId)?.pending).toBe(true);
	expect(f.manager.assessTaskRelease(name).kind).toBe("eligible");
	expect(await f.lanes.retire(name, "operator")).toMatchObject({ retired: true, closed: true });
	expect(f.lanes.activeLanes()).toHaveLength(0);
	await f.restart();
	expect(f.db.workTaskOriginalSource(taskId)).toEqual(original);
	expect(f.db.workTaskPendingReviews(originKey(coordinator)).items.map((item) => item.taskId)).toContain(taskId);
	expect(f.port.sends).toHaveLength(1);
});

test("pending review keysets reach later tasks and retained originals beyond source excerpt limits after restart", async () => {
	const f = await fixture();
	await f.manager.stop();
	const lanes = new LaneGovernor({ database: f.db, sessionPort: f.port, maxLanes: 64 });
	const manager = new WorkLaneManager({
		database: f.db,
		port: f.port,
		lanes,
		pollMs: 5,
		taskSurfaceAvailable: () => true,
	});
	cleanup.push(async () => manager.stop());
	const ids: string[] = [];
	for (let index = 0; index < 23; index++) {
		const id = `00000000-0000-4000-8000-${index.toString().padStart(12, "0")}`;
		ids.push(id);
		const localThread = { ...thread, conversationId: String(1000 + index) };
		await manager.start(
			{ ...f.input, name: `fm-${id}`, task: { ...f.input.task, taskId: id } },
			{ ...context, evidence: { ...context.evidence, eventId: `assignment:${id}` } },
		);
		const localClaim = crypto.randomUUID();
		await manager.threadClaim({ taskId: id, claimId: localClaim });
		await manager.threadBind({ taskId: id, claimId: localClaim, outcome: { kind: "bound", origin: localThread } });
		const task = f.db.workTaskGet(id)!;
		f.port.complete(task.opRef, `Original report ${index}`);
		await until(() => f.db.workTaskGet(id)?.obligationState === "final_admitted");
	}
	const original = f.db.workTaskOriginalSource(ids[0]!)!;
	for (let index = 0; index < 55; index++)
		f.db.withTransaction(() =>
			f.db.workTaskSourceAppendInTransaction({
				sourceId: `discussion-${index}`,
				taskId: ids[0]!,
				kind: "observation",
				body: `Later evidence ${index}`,
				evidence: { ...context.evidence, eventId: `discussion:${index}` },
				supersedes: null,
				completeness: "incomplete",
				controlId: null,
				reportId: null,
			}),
		);
	await manager.stop();
	const caller = { originKey: originKey(coordinator), sessionId: "persona-session", epoch: 0 };
	const firstTurn = buildWorkTaskReviewTurnContext(f.db, caller);
	expect(firstTurn).toContain(ids[0]!);
	expect(firstTurn).not.toContain(ids[22]!);
	const reopened = await GatewayDatabase.open(join(f.directory, "gateway.db"));
	try {
		const laterTurn = buildWorkTaskReviewTurnContext(reopened, caller);
		expect(laterTurn).toContain(ids[22]!);
		expect(laterTurn).not.toContain(ids[0]!);
		expect(buildWorkTaskReviewTurnContext(reopened, caller)).toContain(ids[0]!);
		const first = reopened.workTaskPendingReviews(originKey(coordinator));
		expect(first.items).toHaveLength(20);
		expect(first.items[0]?.sourceId).toBe(original.sourceId);
		expect(first.nextTaskId).not.toBeNull();
		const second = reopened.workTaskPendingReviews(originKey(coordinator), first.nextTaskId!);
		expect(second.items).toHaveLength(3);
		expect(second.nextTaskId).toBeNull();
		expect([...first.items, ...second.items].map((item) => item.taskId)).toEqual(ids);
		expect(reopened.workTaskPendingReviews("discord/channel/foreign").items).toHaveLength(0);
		const raw = new Database(join(f.directory, "gateway.db"));
		try {
			raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(ids[0]!);
		} finally {
			raw.close();
		}
		const damaged = reopened.workTaskPendingReviews(originKey(coordinator));
		expect(damaged.unavailableTaskIds).toContain(ids[0]!);
		expect(damaged.items.map((item) => item.taskId)).toContain(ids[19]!);
		expect(
			reopened.workTaskPendingReviews(originKey(coordinator), damaged.nextTaskId!).items.map((item) => item.taskId),
		).toContain(ids[22]!);
	} finally {
		reopened.close();
	}
	expect(f.port.sends).toHaveLength(23);
	expect(f.port.resumes).toHaveLength(0);
});

test("real repository reconciliation retains one original checkpoint with immutable evidence and mapped intent", async () => {
	let clock = Date.parse(at);
	const f = await fixture({ now: () => clock });
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
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
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
	// A later settlement failure must not refresh the already observed checkpoint.
	const settle = spyOn(f.db, "workAttemptSettleInTransaction").mockImplementation(() => {
		throw new Error("settlement unavailable");
	});
	cleanup.push(async () => {
		settle.mockRestore();
	});
	f.port.complete(task.opRef, "Original result");
	await until(() => executionSources(f.db).some((source) => source.body.includes(sha)));
	const source = executionSources(f.db).find((source) => source.body.includes(sha))!;
	expect(source.completeness).toBe("incomplete");
	expect(source.evidence).toMatchObject({ origin: thread, evidenceAt: at, observedAt: at });
	for (const identity of [taskId, task.jobId, task.opRef, task.sessionId!]) expect(source.body).toContain(identity);
	expect(source.body).toContain(`"epoch":${task.epoch}`);
	expect(source.body).toContain("not semantic progress");
	const delivery = f.db.deliveryGet(source.deliveryId!)!;
	expect(JSON.parse(delivery.payload_json)).toMatchObject({
		origin: thread,
		text: source.body,
		workTask: { taskId, opRef: task.opRef, sourceId: source.sourceId, mappedOnly: true },
	});
	clock += 60_000;
	git(
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"changed checkpoint",
	);
	const changedSha = git("rev-parse", "HEAD");
	await until(() => executionSources(f.db).some((item) => item.body.includes(changedSha)));
	const changed = executionSources(f.db).find((item) => item.body.includes(changedSha))!;
	expect(changed.sourceId).not.toBe(source.sourceId);
	expect(changed.evidence.observedAt).toBe(new Date(clock).toISOString());
	settle.mockRestore();
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	await f.restart();
	expect(executionSources(f.db).filter((item) => item.body.includes(sha))).toEqual([source]);
	const selected = buildWorkTaskContext(f.db, { taskId }, () => new Date(clock));
	expect(selected.manifest.find((entry) => entry.sourceId === source.sourceId)).toMatchObject({
		evidenceAt: at,
		observedAt: at,
		completeness: "partial",
		revision: source.contentHash,
	});
	expect(selected.items.find((entry) => entry.sourceId === source.sourceId)?.text).toContain(
		"source completeness incomplete",
	);
	expect(selected.renderedAt).not.toBe(source.evidence.observedAt);
	expect(f.port.sends).toHaveLength(1);
});

test("observer exception persists only a bounded original fact once across recovery and selection", async () => {
	let clock = Date.parse(at);
	const f = await fixture({ now: () => clock });
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const status = spyOn(f.port, "status").mockRejectedValue(new Error("secret-token raw stdout private transcript"));
	cleanup.push(async () => {
		status.mockRestore();
	});
	await until(() => executionSources(f.db).length === 1);
	const source = executionSources(f.db)[0]!;
	expect(source.body).toContain("reconciliation unavailable");
	expect(source.body).not.toContain("secret-token");
	expect(source.body).not.toContain("transcript");
	expect(Buffer.byteLength(source.body)).toBeLessThan(2048);
	expect(source.completeness).toBe("incomplete");
	expect(source.deliveryId).not.toBeNull();
	const first = buildWorkTaskContext(f.db, { taskId }, () => new Date(clock));
	clock += 60_000;
	await f.restart();
	expect(executionSources(f.db)).toEqual([source]);
	const repeated = buildWorkTaskContext(f.db, { taskId, continuation: first.continuation }, () => new Date(clock));
	expect(repeated.snapshot).toBe("same");
	expect(repeated.manifest.find((entry) => entry.sourceId === source.sourceId)?.observedAt).toBe(at);
	expect(f.db.workTaskGet(taskId)?.obligationState).toBe("awaiting_final");
	expect(f.port.sends).toHaveLength(1);
	status.mockRestore();
	const output = spyOn(f.port, "fetchWorkerOutput").mockRejectedValue(new Error("unavailable private output"));
	cleanup.push(async () => {
		output.mockRestore();
	});
	f.port.complete(task.opRef, "Independent original completion");
	await until(() =>
		executionSources(f.db).some((item) => item.body.includes("Observation category: output_unavailable")),
	);
	const outputFact = executionSources(f.db).find((item) =>
		item.body.includes("Observation category: output_unavailable"),
	)!;
	expect(outputFact.sourceId).not.toBe(source.sourceId);
	expect(outputFact.evidence.observedAt).toBe(new Date(clock).toISOString());
	expect(outputFact.body).not.toContain("private output");
	output.mockRestore();
	clock += 10_000;
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
});

test("observation source and whole mapped intent roll back together before retry", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const before = f.db.deliveryRows().map((row) => row.delivery_id);
	const append = f.db.workTaskSourceAppendInTransaction.bind(f.db);
	let failed = false;
	const write = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation((input, payload) => {
		const source = append(input, payload);
		if (input.sourceId.startsWith("execution-")) {
			failed = true;
			throw new Error("rollback joined observation");
		}
		return source;
	});
	const status = spyOn(f.port, "status").mockRejectedValue(new Error("unavailable"));
	cleanup.push(async () => {
		write.mockRestore();
		status.mockRestore();
	});
	await until(() => failed);
	expect(executionSources(f.db)).toHaveLength(0);
	expect(f.db.deliveryRows().map((row) => row.delivery_id)).toEqual(before);
	write.mockRestore();
	await until(() => executionSources(f.db).length === 1);
	const source = executionSources(f.db)[0]!;
	expect(f.db.deliveryGet(source.deliveryId!)).toBeDefined();
	expect(f.db.deliveryRows().filter((row) => row.delivery_id === source.deliveryId)).toHaveLength(1);
});

test("unavailable mapped surface keeps immutable null-intent audit without later fallback or resend", async () => {
	let available = true;
	const f = await fixture({ taskSurfaceAvailable: () => available });
	await f.admit();
	await f.bind();
	available = false;
	const before = f.db.deliveryRows().map((row) => row.delivery_id);
	const status = spyOn(f.port, "status").mockRejectedValue(new Error("unavailable"));
	cleanup.push(async () => {
		status.mockRestore();
	});
	await until(() => executionSources(f.db).length === 1);
	const source = executionSources(f.db)[0]!;
	expect(source.deliveryId).toBeNull();
	expect(source.evidence.origin).toEqual(thread);
	available = true;
	await f.restart();
	expect(executionSources(f.db)).toEqual([source]);
	expect(f.db.deliveryRows().map((row) => row.delivery_id)).toEqual(before);
});

test("status exception after original binding replacement cannot publish against the replacement", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	let called = false;
	const status = spyOn(f.port, "status").mockImplementation(async () => {
		f.db.rebindEpoch(workSessionKey(name));
		f.db.putSession(workSessionKey(name), "replacement-session");
		called = true;
		throw new Error("original query failed after replacement");
	});
	cleanup.push(async () => {
		status.mockRestore();
	});
	await until(() => called);
	await f.manager.stop();
	expect(executionSources(f.db)).toHaveLength(0);
	expect(f.port.sends).toHaveLength(1);
});

test("task status discards original query evidence when binding epoch changes in flight", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.complete(task.opRef, "Original terminal evidence");
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	const query = f.port.status.bind(f.port);
	const status = spyOn(f.port, "status").mockImplementation(async (input) => {
		const result = await query(input);
		f.db.rebindEpoch(workSessionKey(name));
		f.db.putSession(workSessionKey(name), task.sessionId!);
		return result;
	});
	const before = f.db.workTaskGet(taskId);
	const result = await f.manager.status({ name, taskId, expectedOpRef: task.opRef });
	expect(status).toHaveBeenCalledTimes(1);
	expect(result.op).toBeNull();
	expect(result.sessionId).toBe(task.sessionId!);
	expect(result.task).toMatchObject({
		taskId,
		opRef: task.opRef,
		sessionId: task.sessionId,
		epoch: task.epoch,
		obligation: "final_admitted",
	});
	expect(f.db.workTaskGet(taskId)).toEqual(before);
	status.mockClear();
	expect((await f.manager.status({ name, taskId, expectedOpRef: task.opRef })).op).toBeNull();
	expect(status).not.toHaveBeenCalled();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
	status.mockRestore();
});

test("typed explicit steering preserves source, deduplicates across restart, and refuses missing authority", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind(false);
	const task = f.db.workTaskGet(taskId)!;
	const params = {
		name,
		taskId,
		expectedOpRef: task.opRef,
		eventId: "cli:owner-steer",
		text: "Inspect original evidence",
	};
	const local = { ...context, evidence: { ...context.evidence, eventId: params.eventId } };
	await expect(f.manager.steer(params)).rejects.toThrow();
	await expect(f.manager.steer(params, { ...local, stableOrigin: null })).rejects.toThrow();
	await expect(f.manager.steer({ ...params, expectedOpRef: "gw-wrong" }, local)).rejects.toThrow();
	expect(await f.manager.steer(params, local)).toMatchObject({
		route: "work_task",
		delivery: "accepted",
		steered: true,
	});
	const control = f.db.workControlList(taskId)[0]!;
	expect(control.request.evidence.origin).toEqual(coordinator);
	expect(f.db.workTaskSourceGet(`route-${control.controlId}`)?.kind).toBe("decision");
	await f.restart();
	expect(
		await f.manager.steer(params, { ...local, evidence: { ...local.evidence, observedAt: "2026-10-06T06:00:00Z" } }),
	).toMatchObject({ delivery: "accepted", controlId: control.controlId });
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
});

test("ambiguous equal-time mapped ingress cannot cross the marker floor", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	expect(await f.manager.admitMappedEvent(event("ambiguous", { platformTimestamp: at }))).toMatchObject({
		delivery: "refused",
		reason: "message_precedes_activation_marker",
	});
	expect(f.port.steers).toHaveLength(0);
});

test("one explicit source event has independent outcomes and identities for two named tasks", async () => {
	let calls = 0;
	const f = await fixture(
		{},
		{
			onSteer: () => {
				if (++calls === 2) throw steerRefused();
			},
		},
	);
	await f.admit();
	await f.bind(false);
	const secondId = "c7654d21-6806-4fdc-89ef-e2f9c038e4f9";
	const secondName = `fm-${secondId}`;
	await f.manager.start({ ...f.input, name: secondName, task: { ...f.input.task, taskId: secondId } }, context);
	const secondClaim = crypto.randomUUID();
	await f.manager.threadClaim({ taskId: secondId, claimId: secondClaim });
	await f.manager.threadBind({
		taskId: secondId,
		claimId: secondClaim,
		outcome: {
			kind: "bound",
			origin: { ...thread, conversationId: "201" },
		},
	});
	const eventId = "discord:wider-direction";
	const evidence = { ...context, evidence: { ...context.evidence, eventId } };
	for (const id of [taskId, secondId]) {
		const task = f.db.workTaskGet(id)!;
		expect(
			await f.manager.steer(
				{
					name: task.laneName,
					taskId: id,
					expectedOpRef: task.opRef,
					eventId,
					text: "Inspect the same incident in your own scope",
				},
				evidence,
			),
		).toMatchObject({ delivery: id === taskId ? "accepted" : "refused" });
	}
	const first = f.db.workControlList(taskId)[0]!;
	const second = f.db.workControlList(secondId)[0]!;
	expect(first.controlId).not.toBe(second.controlId);
	expect(first.clientRef).not.toBe(second.clientRef);
	expect(first.request.evidence).toEqual(second.request.evidence);
	expect(first.request.evidence.origin).toEqual(coordinator);
	expect(f.port.steers).toHaveLength(2);
});

test("typed control source and admission roll back together", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const fail = spyOn(f.db, "workControlAdmitInTransaction").mockImplementation(() => {
		throw new Error("admission failed");
	});
	const params = { name, taskId, expectedOpRef: task.opRef, eventId: "cli:rollback", text: "Inspect evidence" };
	await expect(
		f.manager.steer(params, { ...context, evidence: { ...context.evidence, eventId: params.eventId } }),
	).rejects.toThrow();
	fail.mockRestore();
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
	expect(
		await f.manager.steer(params, { ...context, evidence: { ...context.evidence, eventId: params.eventId } }),
	).toMatchObject({ delivery: "accepted" });
});

test("unavailable original result can later supplement once without rewriting settled history", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	await f.manager.admitMappedEvent(event("local-cancel", { kind: "cancel_request" }));
	expect(f.manager.taskDebt(name)).toMatchObject({ exactTerminal: false, safeToReleaseExecution: false });
	const unavailable = spyOn(f.port, "fetchWorkerOutput").mockResolvedValue({
		status: "unavailable",
		code: "output_unavailable",
	});
	f.port.complete(task.opRef, "Late complete original answer");
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	const before = f.db.workAttemptGet(task.opRef)!;
	expect(before.output.proof).toBeNull();
	expect(f.manager.taskDebt(name)).toMatchObject({
		exactTerminal: true,
		safeToReleaseExecution: false,
		safeToCleanupWorktree: false,
		unresolvedControls: 1,
	});
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "held" });
	unavailable.mockRestore();
	const original = await f.port.fetchWorkerOutput({
		sessionId: before.sessionId,
		repo: before.cwd,
		opRef: before.opRef,
		notBeforeMs: Date.parse(before.startedAt),
		terminalIdentity: before.terminal!.status,
		isCurrent: () => true,
	});
	if (original.status !== "proven") throw new Error("fixture must retain original result");
	const wrong = spyOn(f.port, "fetchWorkerOutput").mockResolvedValue({
		...original,
		provenance: { ...original.provenance, sessionId: "wrong-session" },
	});
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "held" });
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)).toBeUndefined();
	wrong.mockRestore();
	const append = f.db.workTaskSourceAppendInTransaction.bind(f.db);
	const fail = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation((source, delivery) => {
		if (source.sourceId === `supplement-${taskId}-original`) throw new Error("supplement write failure");
		return append(source, delivery);
	});
	await expect(f.manager.recoverTaskReport(taskId)).rejects.toThrow("supplement write failure");
	fail.mockRestore();
	expect(f.db.workTaskSourceGet(`supplement-proof-${taskId}-original`)).toBeUndefined();
	await f.restart();
	const results = await Promise.all([f.manager.recoverTaskReport(taskId), f.manager.recoverTaskReport(taskId)]);
	expect(results.map((result) => result.disposition).sort()).toEqual(["reconciled", "unchanged"]);
	expect(f.db.workAttemptGet(task.opRef)).toEqual(before);
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)?.body).toBe("Late complete original answer");
	expect(f.db.workTaskGet(taskId)?.obligationState).toBe("final_admitted");
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

/** Outage kills the session host; a restart settles local uncertainty before the broker result returns. */
async function outageSettledFixture() {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	f.port.setSessionState(task.sessionId!, { live: false });
	await f.restart();
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt !== null);
	const settled = f.db.workAttemptGet(task.opRef)!;
	expect(settled.terminal).toMatchObject({ kind: "local", reasonCode: "session_dead" });
	expect(settled.output).toMatchObject({ disposition: "unavailable" });
	expect(f.db.workTaskGet(taskId)).toMatchObject({ obligationState: "held" });
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	// The settled local history survives another restart under the outage.
	await f.restart();
	const restore = (text: string) => {
		f.port.setSessionState(task.sessionId!, { live: true });
		f.port.complete(task.opRef, text);
	};
	return { f, task, restore };
}

test("locally settled outage admits one original supplement without execution or release", async () => {
	const { f, task, restore } = await outageSettledFixture();
	// The original result is still absent: no output read, no supplement, capacity held.
	const readsBefore = f.port.workerOutputReads.length;
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({
		disposition: "held",
		completeness: "unavailable",
		reason: "original_terminal_evidence_unavailable",
	});
	expect(f.port.workerOutputReads).toHaveLength(readsBefore);
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)).toBeUndefined();
	expect(f.manager.assessTaskRelease(name).kind).toBe("hold");
	expect(f.manager.taskDebt(name)).toMatchObject({ exactTerminal: false, safeToReleaseExecution: false });

	restore("Recovered original answer after outage");
	const raw = new Database(join(f.directory, "gateway.db"));
	cleanup.push(async () => raw.close());
	const report = () =>
		raw.query("SELECT * FROM inbound_messages WHERE message_id = ?").get(f.db.workAttemptGet(task.opRef)!.reportId);
	const snapshot = () => ({
		runtime: f.db.workAttemptGet(task.opRef),
		job: f.db.laneJobJson(task.jobId),
		report: report(),
		task: f.db.workTaskGet(taskId),
		sources: f.db.workTaskSources(taskId),
		controls: f.db.workControlList(taskId),
		deliveries: (f.db.workTaskSources(taskId)?.sources ?? [])
			.filter((source) => source.deliveryId !== null)
			.map((source) => f.db.deliveryGet(source.deliveryId!)),
	});
	const before = snapshot();
	const first = await f.manager.recoverTaskReport(taskId);
	expect(first).toMatchObject({ disposition: "reconciled", completeness: "complete" });
	expect(first.supplementalDeliveryId).toBeDefined();
	const afterFirst = snapshot();
	const second = await f.manager.recoverTaskReport(taskId);
	expect(second).toMatchObject({ disposition: "unchanged", completeness: "complete" });
	expect(snapshot()).toEqual(afterFirst);
	expect(afterFirst.runtime).toEqual(before.runtime);
	expect(afterFirst.job).toBe(before.job);
	expect(afterFirst.report).toEqual(before.report);
	expect(afterFirst.controls).toEqual(before.controls);
	expect(f.db.workTaskGet(taskId)).toMatchObject({ obligationState: "final_admitted", holdReason: null });
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)?.body).toBe("Recovered original answer after outage");
	const proofSource = JSON.parse(f.db.workTaskSourceGet(`supplement-proof-${taskId}-original`)!.body);
	expect(proofSource.status).toMatchObject({
		status: "terminal_ok",
		receiptState: "present",
		clientRef: task.opRef,
	});
	expect(proofSource.identity).toEqual({ opRef: task.opRef, sessionId: task.sessionId, epoch: task.epoch });
	// Result admission never releases capacity settled as local uncertainty.
	expect(f.manager.assessTaskRelease(name).kind).toBe("hold");
	expect(f.manager.taskDebt(name)).toMatchObject({ exactTerminal: false, safeToReleaseExecution: false });
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
	expect(f.port.workerOutputReads.length).toBeGreaterThan(readsBefore);
});

test("local recovery holds on missing, mismatched or raced original evidence", async () => {
	const { f, task, restore } = await outageSettledFixture();
	const runtime = f.db.workAttemptGet(task.opRef)!;
	type ReportedStatus = Awaited<ReturnType<TaskPort["status"]>>["status"];
	const status: ReportedStatus = {
		status: "terminal_ok",
		receiptState: "present",
		clientRef: task.opRef,
		commandId: "command-original",
		turnId: "turn-original",
		startedAt: Date.parse(runtime.startedAt),
		terminalAt: Date.now(),
	};
	const statusProbe = (body: ReportedStatus) =>
		spyOn(f.port, "status").mockResolvedValue({
			operationRef: task.opRef,
			status: body,
			summaryCompleted: true,
		});
	const expectHeld = async (reason: string) => {
		expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "held", reason });
		expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)).toBeUndefined();
	};
	const readsBefore = f.port.workerOutputReads.length;

	// The original turn is still running: nonterminal status never becomes evidence.
	await expectHeld("original_terminal_evidence_unavailable");
	restore("Recovered original answer after outage");

	const offline = spyOn(f.port, "status").mockRejectedValue(new Error("broker offline"));
	await expectHeld("original_terminal_evidence_unavailable");
	offline.mockRestore();
	const probes: Array<ReportedStatus> = [
		// wrong identity: #query refuses a foreign clientRef
		{ ...status, clientRef: "gw-foreign-op" },
		// receipt never proven present
		{ ...status, receiptState: "missing" },
		// terminal older than the attempt start
		{ ...status, terminalAt: Date.parse(runtime.startedAt) - 1 },
		// terminal beyond the observation time
		{ ...status, terminalAt: Date.now() + 60_000 },
	];
	for (const body of probes) {
		const probe = statusProbe(body);
		await expectHeld("original_terminal_evidence_unavailable");
		probe.mockRestore();
	}
	const inflight = statusProbe({
		status: "in_flight",
		clientRef: task.opRef,
		commandId: status.commandId,
		turnId: status.turnId,
	});
	await expectHeld("original_terminal_evidence_unavailable");
	inflight.mockRestore();
	const anonymous = statusProbe({
		status: "terminal_ok",
		receiptState: "present",
		clientRef: task.opRef,
		startedAt: status.startedAt,
		terminalAt: status.terminalAt,
	});
	await expectHeld("original_terminal_evidence_unavailable");
	anonymous.mockRestore();
	// Status-level refusal happens before any output read.
	expect(f.port.workerOutputReads).toHaveLength(readsBefore);

	const realOutput = await f.port.fetchWorkerOutput({
		sessionId: task.sessionId!,
		repo: runtime.cwd,
		opRef: task.opRef,
		notBeforeMs: Date.parse(runtime.startedAt),
		isCurrent: () => true,
	});
	if (realOutput.status !== "proven") throw new Error("fixture must retain the original result");
	const wrongOutput = spyOn(f.port, "fetchWorkerOutput").mockResolvedValue({
		...realOutput,
		provenance: { ...realOutput.provenance, sessionId: "wrong-session" },
	});
	await expectHeld("late_report_reconciliation_required");
	wrongOutput.mockRestore();

	const realFetch = f.port.fetchWorkerOutput.bind(f.port);
	const raced = spyOn(f.port, "fetchWorkerOutput").mockImplementation(async (input) => {
		const live = f.db.workTaskGet(taskId)!;
		f.db.withTransaction(() =>
			f.db.workTaskObligationInTransaction(taskId, live.version, {
				identity: { opRef: live.opRef, sessionId: live.sessionId!, epoch: live.epoch! },
				state: "held",
				reason: "concurrent_writer",
				at: new Date().toISOString(),
			}),
		);
		return realFetch(input);
	});
	await expectHeld("original_identity_changed");
	raced.mockRestore();

	// The intact original then admits exactly once.
	const first = await f.manager.recoverTaskReport(taskId);
	expect(first).toMatchObject({ disposition: "reconciled" });
	const second = await f.manager.recoverTaskReport(taskId);
	expect(second).toMatchObject({ disposition: "unchanged" });
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)?.body).toBe("Recovered original answer after outage");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
});

test("broker release then restart admits one late original without reopening the released lane", async () => {
	const f = await releaseFixture(false, "ordinary");
	await f.finish();
	expect(f.manager.assessTaskRelease(name)).toMatchObject({ kind: "eligible", opRef: f.task.opRef });
	expect(await f.lanes.retire(name, "operator")).toMatchObject({
		retired: true,
		closed: true,
		sessionId: f.task.sessionId,
	});
	const releasedBinding = { sessionId: "", epoch: f.task.epoch! + 1 };
	expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(releasedBinding);
	expect(f.lanes.activeLanes()).toHaveLength(0);
	// Closing and releasing capacity does not manufacture unavailable output.
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({
		disposition: "held",
		reason: "original_output_incomplete",
	});
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)).toBeUndefined();
	await f.restart();
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({
		disposition: "held",
		reason: "original_output_incomplete",
	});
	expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(releasedBinding);
	f.restoreOutput();

	const raw = new Database(join(f.directory, "gateway.db"));
	cleanup.push(async () => raw.close());
	const report = () =>
		raw.query("SELECT * FROM inbound_messages WHERE message_id = ?").get(f.db.workAttemptGet(f.task.opRef)!.reportId);
	const runtimeBefore = f.db.workAttemptGet(f.task.opRef);
	const jobBefore = f.db.laneJobJson(f.task.jobId);
	const reportBefore = report();

	const first = await f.manager.recoverTaskReport(taskId);
	expect(first).toMatchObject({ disposition: "reconciled", completeness: "complete" });
	const runtimeAfter = f.db.workAttemptGet(f.task.opRef);
	const jobAfter = f.db.laneJobJson(f.task.jobId);
	const reportAfter = report();
	const second = await f.manager.recoverTaskReport(taskId);
	expect(second).toMatchObject({ disposition: "unchanged", completeness: "complete" });
	expect(runtimeAfter).toEqual(runtimeBefore);
	expect(jobAfter).toBe(jobBefore);
	expect(reportAfter).toEqual(reportBefore);
	expect(f.db.workAttemptGet(f.task.opRef)).toEqual(runtimeAfter);
	expect(f.db.laneJobJson(f.task.jobId)).toBe(jobAfter);
	expect(report()).toEqual(reportAfter);
	expect(f.db.workTaskSourceGet(`supplement-${taskId}-original`)?.body).toBe(
		"Original result unavailable to the gateway",
	);
	expect(f.db.workTaskGet(taskId)).toMatchObject({ obligationState: "final_admitted" });
	expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(releasedBinding);
	expect(f.lanes.activeLanes()).toHaveLength(0);
	expect(f.port.closes).toEqual([{ sessionId: f.task.sessionId!, repo: f.worker }]);
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
	expect(f.port.resumes).toHaveLength(0);
});

async function until(predicate: () => boolean): Promise<void> {
	for (let i = 0; i < 300 && !predicate(); i++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

test("durable intake deduplicates transport observations, conflicts cannot dispatch, no session before bind", async () => {
	const f = await fixture();
	const first = await f.admit();
	expect(first).toMatchObject({ accepted: "durable", execution: "pending_surface", taskId });
	const duplicate = await f.manager.start(f.input, {
		...context,
		evidence: { ...context.evidence, observedAt: "2026-10-06T06:00:00Z" },
	});
	expect(duplicate).toEqual(first);
	await expect(f.manager.start({ ...f.input, text: "different assignment" }, context)).rejects.toThrow();
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sendAttempts).toHaveLength(0);
	expect(f.db.workTaskSourceGet(`assignment-${taskId}`)?.evidence.observedAt).toBe(at);
	expect(f.db.workTaskGet(taskId)?.opRef).toBe(first.opRef!);
});

test("known-null and nested callers never fall back to owner target", async () => {
	const f = await fixture({ ownerTarget: () => coordinator, allowNested: () => true });
	await expect(f.manager.start(f.input, { ...context, stableOrigin: null })).rejects.toThrow();
	f.db.putSession(workSessionKey("nested"), "worker-session");
	await expect(f.manager.start({ ...f.input, callerSessionId: "worker-session" }, context)).rejects.toThrow();
	await expect(f.manager.start(f.input)).rejects.toThrow();
	expect(f.db.workTaskGet(taskId)).toBeUndefined();
	expect(f.port.binds).toHaveLength(0);
});

test("claim grants creation once, bind preserves reserved original operation and duplicate cannot send twice", async () => {
	const f = await fixture();
	const original = await f.admit();
	expect(await f.manager.threadClaim({ taskId, claimId })).toMatchObject({ create: true });
	expect(await f.manager.threadClaim({ taskId, claimId })).toMatchObject({ create: false, disposition: "duplicate" });
	expect(await f.manager.threadClaim({ taskId, claimId: crypto.randomUUID() })).toMatchObject({ create: false });
	await f.bind();
	await f.bind();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.sends[0]?.opRef).toBe(original.opRef!);
	expect(f.db.workTaskGet(taskId)).toMatchObject({ dispatchPhase: "prepared", opRef: original.opRef!, thread });
	await expect(
		f.manager.threadBind({ taskId, claimId, outcome: { kind: "bound", origin: { ...thread, conversationId: "201" } } }),
	).rejects.toThrow();
});

test("selected thread is verified and bound without any create authorization", async () => {
	const f = await fixture();
	await f.manager.start({ ...f.input, task: { ...f.input.task, surface: { threadOrigin: thread } } }, context);
	expect(await f.manager.threadClaim({ taskId, claimId })).toMatchObject({ create: false });
	await f.manager.threadBind({ taskId, claimId, outcome: { kind: "bound", origin: thread } });
	expect(f.port.sends).toHaveLength(1);
});

test("bare start/run/resume/steer cannot bypass original task mandate", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const ordinary = { name, text: "retask", cwd: f.directory, resume: true };
	await expect(f.manager.start(ordinary)).rejects.toThrow();
	await expect(f.manager.run(ordinary, {})).rejects.toThrow();
	await expect(f.manager.steer({ name, text: "bypass" })).rejects.toThrow();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.steers).toHaveLength(0);
});

test("surface guard refuses active/unverified thread before worker creation", async () => {
	const f = await fixture({ taskSurfaceAvailable: () => false });
	await f.admit();
	await expect(f.bind()).rejects.toThrow();
	expect(f.port.binds).toHaveLength(0);
	expect(f.db.workTaskGet(taskId)?.surfacePhase).toBe("claimed");
});

test("binding and activation source roll back together on payload persistence failure", async () => {
	const f = await fixture();
	await f.admit();
	await f.manager.threadClaim({ taskId, claimId });
	const fail = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation(() => {
		throw new Error("disk failure");
	});
	await expect(f.bind()).rejects.toThrow("disk failure");
	fail.mockRestore();
	expect(f.db.workTaskGet(taskId)?.surfacePhase).toBe("claimed");
	expect(f.db.workTaskSourceGet(`activation-${taskId}`)).toBeUndefined();
	expect(f.port.sends).toHaveLength(0);
});

test("mapped concurrent duplicates are ordered, edited bodies require distinct edit identity", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const [one, same] = await Promise.all([f.manager.admitMappedEvent(event()), f.manager.admitMappedEvent(event())]);
	expect(one.controlId).toBe(same.controlId);
	expect(one.delivery).toBe("accepted");
	expect(f.port.steers).toHaveLength(1);
	await expect(f.manager.admitMappedEvent(event("event-1", { body: "changed" }))).rejects.toThrow();
	await f.manager.admitMappedEvent(event("event-1", { editId: "edit-1", body: "changed" }));
	expect(f.port.steers).toHaveLength(2);
	expect(f.db.workControlList(taskId).map((c) => c.sequence)).toEqual([1, 2]);
	expect(f.db.workControlList(taskId).every((c) => c.notificationDisposition === null)).toBe(true);
	expect(f.db.inboundPendingOldest(originKey(coordinator))).toBeUndefined();
});

for (const held of [false, true]) {
	test(`policy D quiet control dispositions retain detail and identities across recovery (persona held=${held})`, async () => {
		let count = 0;
		const f = await fixture(
			{ personaHold: () => (held ? "test_hold" : undefined) },
			{
				onSteer: () => {
					count++;
					if (count === 2) throw steerRefused();
					if (count === 3) throw new Error("receipt lost");
				},
			},
		);
		await f.admit();
		await f.bind();
		for (const id of ["accepted", "refused", "held"]) await f.manager.admitMappedEvent(event(id));
		const before = f.db.workControlList(taskId);
		expect(before.map((control) => control.phase)).toEqual(["accepted", "refused", "held"]);
		for (const control of before) {
			expect(control.notificationDisposition).toBeNull();
			expect(f.db.deliveryGet(control.notificationId)).toBeUndefined();
			const source = f.db.workTaskSourceGet(`disposition-${control.controlId}-${control.phase}`)!;
			expect(source.controlId).toBe(control.controlId);
			expect(source.body).toContain(control.phase);
			expect(source.deliveryId).not.toBeNull();
			expect(JSON.parse(f.db.deliveryGet(source.deliveryId!)!.payload_json).origin).toEqual(thread);
		}
		expect(f.db.inboundPendingOldest(originKey(coordinator))).toBeUndefined();
		await f.restart();
		for (const id of ["accepted", "refused", "held"]) await f.manager.admitMappedEvent(event(id));
		expect(f.db.workControlList(taskId)).toEqual(before);
		for (const control of before) expect(f.db.deliveryGet(control.notificationId)).toBeUndefined();
		expect(f.db.inboundPendingOldest(originKey(coordinator))).toBeUndefined();
		const view = buildWorkTaskContext(f.db, { taskId });
		expect(view.items.some((item) => item.text.includes("Steering acceptance is not task completion."))).toBe(true);
		expect(f.port.steers).toHaveLength(3);
		expect(f.port.sends).toHaveLength(1);
	});
}

test("recovered accepted steering stays quiet and retains the original held observation", async () => {
	const f = await fixture(
		{},
		{
			onSteer: () => {
				throw new Error("lost receipt");
			},
		},
	);
	await f.admit();
	await f.bind();
	await f.manager.admitMappedEvent(event());
	const control = f.db.workControlList(taskId)[0]!;
	const heldSource = f.db.workTaskSourceGet(`disposition-${control.controlId}-held`)!;
	f.port.lookup = { status: "accepted", clientRef: control.clientRef!, acceptedAt: Date.parse(at) };
	await f.restart();
	expect(f.db.workControlList(taskId)[0]).toMatchObject({
		controlId: control.controlId,
		clientRef: control.clientRef,
		phase: "accepted",
		notificationDisposition: null,
	});
	expect(f.db.inboundPendingOldest(originKey(coordinator))).toBeUndefined();
	expect(f.db.deliveryGet(control.notificationId)).toBeUndefined();
	expect(f.db.workTaskSourceGet(heldSource.sourceId)).toEqual(heldSource);
	const acceptedSource = f.db.workTaskSourceGet(`disposition-${control.controlId}-accepted`)!;
	expect(acceptedSource.controlId).toBe(control.controlId);
	expect(acceptedSource.deliveryId).not.toBeNull();
	expect(f.port.steers).toHaveLength(1);
});

test("prebinding, missing platform time and history cannot reach steering transport", async () => {
	const f = await fixture();
	await f.admit();
	await expect(f.manager.admitMappedEvent(event())).rejects.toThrow();
	await f.bind();
	await expect(f.manager.admitMappedEvent(event("missing", { platformTimestamp: null }))).rejects.toThrow();
	await expect(
		f.manager.admitMappedEvent(event("history", { platformTimestamp: "2026-10-06T04:59:59Z" })),
	).rejects.toThrow();
	expect(f.port.steers).toHaveLength(0);
});

test("local cancel retains debt without blocking later steering; reset never rebinds", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	expect(await f.manager.admitMappedEvent(event("cancel", { kind: "cancel_request" }))).toMatchObject({
		delivery: "held",
	});
	expect(await f.manager.admitMappedEvent(event("later"))).toMatchObject({ delivery: "accepted" });
	await f.manager.admitMappedEvent(event("reset", { kind: "reset_notice" }));
	expect(f.port.steers).toHaveLength(1);
	expect(f.db.workControlList(taskId)[0]).toMatchObject({
		phase: "held",
		sendingAt: null,
		receipt: null,
		reason: "local_operator_action_required",
	});
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.resumes).toHaveLength(0);
});

test("shared read-only task cannot elevate typed scope before steer", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	expect(await f.manager.admitMappedEvent(event("mutation", { scope: "code_mutating" }))).toMatchObject({
		delivery: "refused",
	});
	expect(f.port.steers).toHaveLength(0);
	expect(f.db.workTaskGet(taskId)?.request.kind).toBe("read_only");
});

test("lost steer receipt stays held across restart with no resend and blocks successors", async () => {
	const f = await fixture(
		{},
		{
			onSteer: () => {
				throw new Error("lost response");
			},
		},
	);
	await f.admit();
	await f.bind();
	const first = await f.manager.admitMappedEvent(event());
	expect(first.delivery).toBe("held");
	await f.manager.admitMappedEvent(event("second"));
	await f.restart();
	expect(f.port.steers).toHaveLength(1);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.lookups.every((read) => read.clientRef === f.port.steers[0]!.clientRef)).toBe(true);
	expect(f.db.workControlList(taskId).map((c) => c.phase)).toEqual(["held", "pending"]);
});

test("definitive steer refusal is not uncertain and permits next control", async () => {
	let calls = 0;
	const f = await fixture(
		{},
		{
			onSteer: () => {
				if (++calls === 1) throw steerRefused();
			},
		},
	);
	await f.admit();
	await f.bind();
	expect(await f.manager.admitMappedEvent(event())).toMatchObject({ delivery: "refused" });
	expect(await f.manager.admitMappedEvent(event("next"))).toMatchObject({ delivery: "accepted" });
});

test("prepared send lost before receipt is never resent on recovery", async () => {
	const f = await fixture();
	await f.admit();
	const send = spyOn(f.port, "send").mockRejectedValue(new Error("transport unknown"));
	await expect(f.bind()).rejects.toThrow();
	expect(f.db.workTaskGet(taskId)?.dispatchPhase).toBe("prepared");
	await f.restart();
	expect(send).toHaveBeenCalledTimes(1);
	expect(f.port.resumes).toHaveLength(0);
	send.mockRestore();
});

test("pending original intent after bind commit can dispatch once on recovery", async () => {
	const f = await fixture();
	await f.admit();
	const bind = spyOn(f.port, "bind").mockRejectedValue(new Error("before prepare"));
	await expect(f.bind()).rejects.toThrow();
	expect(f.db.workTaskGet(taskId)?.dispatchPhase).toBe("pending");
	bind.mockRestore();
	await f.restart();
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.sends[0]?.opRef).toBe(f.db.workTaskGet(taskId)!.opRef);
});

test("wrong current binding never steers another session", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	f.db.putSession(workSessionKey(name), "replacement-session");
	await f.manager.admitMappedEvent(event());
	expect(f.port.steers).toHaveLength(0);
});

test("source and notification transition rollback prevents unrecorded steering", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const fail = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation(() => {
		throw new Error("source write failed");
	});
	await expect(f.manager.admitMappedEvent(event())).rejects.toThrow();
	fail.mockRestore();
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.port.steers).toHaveLength(0);
});

test("complete original result retains raw UTF-8 body separately from coordinator envelope", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const text = "결과".repeat(1000);
	f.port.complete(task.opRef, text);
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "final_admitted");
	const final = f.db.workTaskSourceGet(`final-${taskId}-original`)!;
	expect(final.body).toBe(text);
	expect(final.deliveryId).not.toBeNull();
	expect(Buffer.byteLength(f.db.workAttemptGet(task.opRef)!.output.excerpt!, "utf8")).toBeLessThanOrEqual(2048);
	expect(await f.manager.recoverTaskReport(taskId)).toMatchObject({ disposition: "unchanged", execution: "none" });
	expect(f.port.sends).toHaveLength(1);
});

test("oversized original output honestly holds rather than archiving an excerpt", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	f.port.complete(f.db.workTaskGet(taskId)!.opRef, "x".repeat(16 * 1024 + 1));
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "held");
	expect(f.db.workTaskSourceGet(`final-${taskId}-original`)).toBeUndefined();
	expect(f.port.sends).toHaveLength(1);
});

test("invalid mapping holds detail without redirecting full final to coordinator", async () => {
	let available = true;
	const f = await fixture({ taskSurfaceAvailable: () => available });
	await f.admit();
	await f.bind();
	available = false;
	f.port.complete(f.db.workTaskGet(taskId)!.opRef, "private final");
	await until(() => f.db.workTaskGet(taskId)?.obligationState === "held");
	expect(f.db.workTaskGet(taskId)?.holdReason).toBe("mapped_surface_unavailable");
	expect(f.db.workTaskSourceGet(`final-${taskId}-original`)).toBeUndefined();
});

test("existing task-name history refuses assignment before reserving another operation", async () => {
	const f = await fixture();
	const identity = laneJobIdentity(name);
	const job = createLaneJobRecord({ jobId: identity.jobId, branch: "prior", worktreePath: f.directory });
	f.db.putLaneJob({ ...job, laneKey: identity.laneKey, json: JSON.stringify(job) });
	await expect(f.admit()).rejects.toThrow("history");
	expect(f.db.workTaskGet(taskId)).toBeUndefined();
	expect(f.port.binds).toHaveLength(0);
});

test("binding time is not an ingress floor: pending controls wait for the confirmed marker", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind(false);
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	expect(await f.manager.admitMappedEvent(event())).toMatchObject({ delivery: "pending", route: "work_task" });
	expect(f.port.steers).toHaveLength(0);
	await f.bind(true);
	expect(f.port.steers).toHaveLength(1);
	expect(f.db.workControlList(taskId)[0]?.phase).toBe("accepted");
});

test("post-marker edit cannot authorize an old message, and marker proof joins confirmation rollback", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind(false);
	await f.manager.admitMappedEvent(event("old", { platformTimestamp: "2026-10-06T04:59:59Z" }));
	await f.manager.admitMappedEvent(event("old", { editId: "new-edit", platformTimestamp: "2026-10-06T04:59:59Z" }));
	const marker = f.db.workTaskSourceGet(`activation-${taskId}`)!;
	expect(() =>
		f.db.withTransaction(() => {
			f.db.deliveryUpdate(marker.deliveryId!, "confirmed");
			f.manager.recordTaskActivationInTransaction({
				taskId,
				deliveryId: marker.deliveryId!,
				origin: thread,
				messageId: "marker-1",
				evidenceAt: at,
				observedAt: at,
				principalId: "discord-adapter",
			});
			throw new Error("rollback");
		}),
	).toThrow("rollback");
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
	expect(f.db.deliveryGet(marker.deliveryId!)?.state).not.toBe("confirmed");
	await f.bind(true);
	expect(f.db.workControlList(taskId).map((c) => c.phase)).toEqual(["refused", "refused"]);
	expect(f.port.steers).toHaveLength(0);
});

test("wrong marker delivery and origin cannot establish an activation floor", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind(false);
	const marker = f.db.workTaskSourceGet(`activation-${taskId}`)!;
	for (const input of [
		{ deliveryId: "wrong", origin: thread },
		{ deliveryId: marker.deliveryId!, origin: { ...thread, conversationId: "201" } },
	]) {
		expect(() =>
			f.db.withTransaction(() => {
				f.db.deliveryUpdate(marker.deliveryId!, "confirmed");
				f.manager.recordTaskActivationInTransaction({
					taskId,
					...input,
					messageId: "marker-1",
					evidenceAt: at,
					observedAt: at,
					principalId: "discord-adapter",
				});
			}),
		).toThrow();
	}
	expect(f.db.workTaskSourceGet(`activation-floor-${taskId}`)).toBeUndefined();
});

test("original steering lookup reconciles accepted, but wrong client reference never does", async () => {
	let fail = true;
	const f = await fixture(
		{},
		{
			onSteer: () => {
				if (fail) throw new Error("lost response");
			},
		},
	);
	await f.admit();
	await f.bind();
	await f.manager.admitMappedEvent(event());
	const ref = f.db.workControlList(taskId)[0]!.clientRef!;
	f.port.lookup = { status: "accepted", clientRef: "wrong", acceptedAt: Date.now() };
	await f.manager.drainTaskControls(taskId);
	expect(f.db.workControlList(taskId)[0]?.phase).toBe("held");
	f.port.lookup = { status: "accepted", clientRef: ref, acceptedAt: Date.now() };
	fail = false;
	await f.manager.drainTaskControls(taskId);
	expect(f.db.workControlList(taskId)[0]?.phase).toBe("accepted");
	expect(f.port.steers).toHaveLength(1);
	expect(f.db.workTaskGet(taskId)?.obligationState).toBe("awaiting_final");
});

test("mapped final source failure rolls back logical report and delivery together", async () => {
	const f = await fixture();
	await f.admit();
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const append = f.db.workTaskSourceAppendInTransaction.bind(f.db);
	let rejected = false;
	const fail = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation((source, delivery) => {
		if (source.sourceId === `final-${taskId}-original`) {
			rejected = true;
			throw new Error("atomic failure");
		}
		return append(source, delivery);
	});
	f.port.complete(task.opRef, "complete original");
	await until(() => rejected);
	expect(f.db.workAttemptGet(task.opRef)?.settledAt).toBeNull();
	expect(f.db.inboundPendingOldest(originKey(coordinator))).toBeUndefined();
	expect(f.db.workTaskSourceGet(`final-${taskId}-original`)).toBeUndefined();
	fail.mockRestore();
	await f.restart();
	// Recovery may need to reacquire the full body; an excerpt never becomes the final.
	expect(f.port.sends).toHaveLength(1);
});

for (const mode of ["timer", "restart", "restart-runtime-torn", "restart-runtime-malformed"] as const)
	test(`corrupt mapped lane cannot stop healthy lane settlement (${mode})`, async () => {
		const restart = mode !== "timer";
		const invalidRuntime = mode.startsWith("restart-runtime");
		const rejections: unknown[] = [];
		const rejected = (reason: unknown) => rejections.push(reason);
		process.on("unhandledRejection", rejected);
		cleanup.push(async () => {
			process.off("unhandledRejection", rejected);
		});
		const errors = spyOn(console, "error");
		cleanup.push(async () => {
			errors.mockRestore();
		});
		const f = await fixture();
		await f.admit();
		await f.bind();
		const bad = f.db.workTaskGet(taskId)!;
		const good = await f.manager.start({
			name: "healthy",
			text: "independent work",
			cwd: f.directory,
			callerSessionId: "persona-session",
		});
		if (!good.started) throw new Error("healthy lane not started");
		if (restart) await f.manager.stop();
		const record = parseLaneJobRecord(f.db.laneJobJson(bad.jobId)!);
		const raw = new Database(join(f.directory, "gateway.db"));
		cleanup.push(async () => {
			raw.close();
		});
		const taskDebt = raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId);
		if (invalidRuntime) {
			expect(record.attempts.find((attempt) => attempt.opRef === bad.opRef)?.endedAt).toBeUndefined();
			if (mode === "restart-runtime-torn")
				raw.query("UPDATE work_attempt_runtime SET version = version + 1 WHERE op_ref = ?").run(bad.opRef);
			else raw.query("UPDATE work_attempt_runtime SET record_json = '{' WHERE op_ref = ?").run(bad.opRef);
		} else {
			const closed = closeAttempt({
				record,
				opRef: bad.opRef,
				endState: "attempt_ended",
				errorCode: "host_lost",
				endedAt: new Date(Date.now() + 1000).toISOString(),
			});
			f.db.putLaneJob({ ...closed, laneKey: laneJobIdentity(name).laneKey, json: JSON.stringify(closed) });
		}
		const corruptRuntime = raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(bad.opRef);
		const assertion =
			mode === "restart-runtime-torn"
				? "runtime session and version match database columns"
				: mode === "restart-runtime-malformed"
					? "runtime record parsing"
					: "attempt.endedAt matches runtime.settledAt";
		expect(() => f.db.workAttemptGet(bad.opRef)).toThrow(assertion);
		const corruptHistory = f.db.laneJobJson(bad.jobId);
		if (restart) await f.restart();
		else await until(() => errors.mock.calls.some((call) => String(call[0]).includes("work_observer_failed")));
		f.port.complete(good.opRef, "healthy original result");
		await until(() => f.db.workAttemptGet(good.opRef)?.settledAt != null);
		const settled = f.db.workAttemptGet(good.opRef)!;
		expect(settled.terminal).toMatchObject({ kind: "broker", status: { status: "terminal_ok" } });
		expect(settled.output).toMatchObject({ disposition: "available", excerpt: "healthy original result" });
		expect(settled.decision).not.toBe("undecided");
		// Containment must not repair the torn original or fabricate a task final.
		expect(() => f.db.workTaskGet(taskId)).toThrow(assertion);
		expect(() => f.db.workAttemptGet(bad.opRef)).toThrow(assertion);
		expect(raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(bad.opRef)).toEqual(corruptRuntime);
		expect(raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId)).toEqual(taskDebt);
		expect(f.db.laneJobJson(bad.jobId)).toBe(corruptHistory);
		expect(f.db.workTaskSourceGet(`final-${taskId}-original`)).toBeUndefined();
		// Normal source/execution authority remains unavailable. The separate negative
		// observation asserts only retained admission provenance, never SDK truth.
		expect(
			raw
				.query("SELECT COUNT(*) AS count FROM work_task_sources WHERE task_id = ? AND source_id LIKE 'execution-%'")
				.get(taskId),
		).toEqual({ count: 0 });
		const negative = f.db.withTransaction(() => f.db.workTaskLinkedUnavailableSourcesInTransaction(taskId));
		expect(negative.sources).toHaveLength(1);
		expect(negative.sources[0]?.body).toContain("stored admission is not GJC truth");
		expect(negative.sources[0]?.deliveryId).not.toBeNull();
		if (restart) {
			expect(f.db.isBrokerQuarantined("work", bad.jobId)).toBe(true);
			expect(
				errors.mock.calls.filter(
					(call) =>
						call[0] === `work_recovery_invalid_attempt opRef=${JSON.stringify(bad.opRef)} assertion=${assertion}`,
				),
			).toHaveLength(1);
			expect(errors.mock.calls.filter((call) => call[0] === `work_task_recovery_held taskId=${taskId}`)).toHaveLength(
				1,
			);
			expect(errors.mock.calls.filter((call) => call[0] === `lane_report_drain_failed parent=${name}`)).toHaveLength(1);
		}
		expect(rejections).toEqual([]);
		expect(f.port.sendAttempts).toHaveLength(2);
		expect(f.port.sendAttempts.filter((send) => send.opRef === bad.opRef)).toHaveLength(1);
		expect(f.port.resumes).toHaveLength(0);
	});

for (const mode of ["timer-runtime", "restart-runtime", "restart-job"] as const)
	test(`actual malformed linked rows retain bounded negative evidence without blocking healthy work (${mode})`, async () => {
		let clock = Date.parse(at);
		const f = await fixture(
			{ now: () => clock },
			{
				onSteer: () => {
					throw new Error("lost original steer receipt");
				},
			},
		);
		await f.admit();
		await f.bind();
		const bad = f.db.workTaskGet(taskId)!;
		expect((await f.manager.admitMappedEvent(event("remote-held"))).delivery).toBe("held");
		await f.manager.admitMappedEvent(event("blocked-successor"));
		const controls = f.db.workControlList(taskId);
		expect(controls.map((control) => control.phase)).toEqual(["held", "pending"]);
		const good = await f.manager.start({
			name: "healthy-negative",
			text: "independent work",
			cwd: f.directory,
			callerSessionId: "persona-session",
		});
		if (!good.started) throw new Error("healthy lane not started");
		if (mode.startsWith("restart")) await f.manager.stop();
		const raw = new Database(join(f.directory, "gateway.db"));
		cleanup.push(async () => {
			raw.close();
		});
		const binding = f.db.getSessionRecord(workSessionKey(name));
		const taskBefore = raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId);
		// Deliberately invalid persisted bytes, not a declared terminal or host-loss event.
		if (mode === "restart-job")
			raw.query("UPDATE lane_jobs SET record_json = '{private-job-secret' WHERE job_id = ?").run(bad.jobId);
		else
			raw
				.query("UPDATE work_attempt_runtime SET record_json = '{private-runtime-secret' WHERE op_ref = ?")
				.run(bad.opRef);
		const runtimeBefore = raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(bad.opRef);
		const jobBefore = raw.query("SELECT * FROM lane_jobs WHERE job_id = ?").get(bad.jobId);
		const read = () => f.db.withTransaction(() => f.db.workTaskLinkedUnavailableSourcesInTransaction(taskId));
		if (mode.startsWith("restart")) await f.restart();
		await until(() => read().sources.length === 1);
		const source = read().sources[0]!;
		expect(source.evidence.observedAt).toBe(at);
		expect(source.deliveryId).not.toBeNull();
		expect(source.body).not.toContain("private-");
		expect(source.body).toContain("Execution, session ownership, terminal outcome and safety are unknown");
		const view = buildWorkTaskContext(f.db, { taskId }, () => new Date(clock));
		expect(view.completeness).toBe("partial");
		expect(view.omission.records).toBeNull();
		expect(view.manifest).toContainEqual(
			expect.objectContaining({
				sourceId: source.sourceId,
				revision: source.contentHash,
				evidenceAt: at,
				observedAt: at,
				completeness: "partial",
			}),
		);
		expect(view.items.find((item) => item.sourceId === source.sourceId)?.text).toContain("Current runtime coverage");
		f.port.complete(good.opRef, "healthy independent result");
		await until(() => f.db.workAttemptGet(good.opRef)?.settledAt != null);
		clock += 60_000;
		await f.restart();
		expect(read().sources).toEqual([source]);
		const repeated = buildWorkTaskContext(f.db, { taskId }, () => new Date(clock));
		expect(repeated.manifest.find((entry) => entry.sourceId === source.sourceId)?.observedAt).toBe(at);
		expect(repeated.renderedAt).not.toBe(at);
		expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(binding);
		expect(raw.query("SELECT * FROM work_tasks WHERE task_id = ?").get(taskId)).toEqual(taskBefore);
		expect(raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(bad.opRef)).toEqual(runtimeBefore);
		expect(raw.query("SELECT * FROM lane_jobs WHERE job_id = ?").get(bad.jobId)).toEqual(jobBefore);
		expect(() => f.db.workTaskGet(taskId)).toThrow();
		expect(f.db.workControlList(taskId)).toEqual(controls);
		expect(f.manager.assessTaskRelease(name).kind).toBe("hold");
		expect(f.port.closes.filter((call) => call.sessionId === bad.sessionId)).toHaveLength(0);
		expect(f.port.binds).toHaveLength(2);
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.sendAttempts.filter((send) => send.opRef === bad.opRef)).toHaveLength(1);
		expect(f.port.sends).toHaveLength(2);
		expect(f.port.resumes).toHaveLength(0);
	});

for (const kind of ["read_only", "code_mutating"] as const)
	test(`${kind} task permits dedicated original scope and rejects shared-area drift`, async () => {
		let coordinatorCwd = "";
		const f = await fixture({ coordinatorCwd: () => coordinatorCwd });
		const primary = join(f.directory, "primary");
		const worker = join(f.directory, "worker");
		await mkdir(primary);
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
		git("worktree", "add", "-b", "worker", worker);
		coordinatorCwd = primary;
		await f.manager.start({ ...f.input, cwd: worker, task: { ...f.input.task, kind } }, context);
		await f.bind();
		const executionCwd =
			kind === "code_mutating"
				? (JSON.parse(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)!.body).executionCwd as string)
				: worker;
		// Read-only tasks still dispatch on the immutable assignment cwd; a code
		// task dispatches on the distinct checkout the SDK returned natively.
		expect(f.port.sends[0]?.repo).toBe(executionCwd);
		if (kind === "code_mutating") {
			const allocationRequest = JSON.parse(f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!.body);
			expect(executionCwd).toBe(join(realpathSync(worker), ".worktrees", allocationRequest.requestKey));
			expect(allocationRequest.source.base).toBe(allocationRequest.source.head);
			expect(f.db.workTaskGet(taskId)?.request.cwd).toBe(worker);
			expect(f.port.binds[0]?.repo).toBe(worker);
			expect(f.port.binds[0]?.epoch).toBe(allocationRequest.epoch);
			expect(f.port.binds[0]?.managedWorktree).toMatchObject({
				idempotencyKey: allocationRequest.requestKey,
				lookupOnly: false,
			});
		}
		const append = f.db.workTaskSourceAppendInTransaction.bind(f.db);
		const rollback = spyOn(f.db, "workTaskSourceAppendInTransaction").mockImplementation((input, delivery) => {
			if (input.sourceId.startsWith("scope-control-")) throw new Error("scope proof failed");
			return append(input, delivery);
		});
		await expect(f.manager.admitMappedEvent(event("mutation", { scope: "code_mutating" }))).rejects.toThrow(
			"scope proof failed",
		);
		rollback.mockRestore();
		expect(f.db.workControlList(taskId)).toHaveLength(0);
		expect(f.db.workTaskSources(taskId)?.sources.some((source) => source.evidence.eventId === "mutation")).toBe(false);
		expect(f.port.steers).toHaveLength(0);
		expect(await f.manager.admitMappedEvent(event("mutation", { scope: "code_mutating" }))).toMatchObject({
			delivery: "accepted",
		});
		expect(f.db.workControlList(taskId)[0]?.request.scope).toBe("code_mutating");
		expect(f.db.workTaskGet(taskId)?.request.kind).toBe(kind);
		await f.restart();
		expect(await f.manager.admitMappedEvent(event("mutation", { scope: "code_mutating" }))).toMatchObject({
			delivery: "accepted",
		});
		expect(f.port.steers).toHaveLength(1);
		const transition = f.db.workControlTransitionInTransaction.bind(f.db);
		const race = spyOn(f.db, "workControlTransitionInTransaction").mockImplementation((id, version, input) => {
			const result = transition(id, version, input);
			if (input.phase === "sending") coordinatorCwd = worker;
			return result;
		});
		expect(await f.manager.admitMappedEvent(event("pretransport-race", { scope: "code_mutating" }))).toMatchObject({
			delivery: "held",
			reason: "pretransport_scope_changed",
		});
		race.mockRestore();
		expect(f.port.steers).toHaveLength(1);
		const held = f.db.workControlList(taskId)[1]!;
		expect(held).toMatchObject({ phase: "held", reason: "pretransport_scope_changed" });
		expect(held.sendingAt).not.toBeNull();
		coordinatorCwd = worker;
		const refusal = await f.manager.admitMappedEvent(event("scope-race", { scope: "code_mutating" }));
		expect(refusal).toMatchObject({
			acceptance: "durable",
			delivery: "refused",
			reason: kind === "read_only" ? "scope_elevation_refused" : "dedicated_worktree_required",
		});
		const refused = f.db.workControlGet(refusal.controlId)!;
		expect(refused).toMatchObject({ phase: "refused", sendingAt: null, receipt: null, sequence: held.sequence + 1 });
		expect(f.db.workTaskSourceGet(`disposition-${refused.controlId}-refused`)).toMatchObject({
			controlId: refused.controlId,
			kind: "observation",
			completeness: "complete",
		});
		expect(f.db.workTaskSourceGet(`worktree-${refused.controlId}`)).toBeUndefined();
		expect(f.db.workControlGet(held.controlId)).toEqual(held);
		coordinatorCwd = primary;
		expect(await f.manager.admitMappedEvent(event("scope-race", { scope: "code_mutating" }))).toMatchObject({
			controlId: refusal.controlId,
			acceptance: "durable",
			delivery: "refused",
		});
		expect(await f.manager.admitMappedEvent(event("valid-behind-held", { scope: "code_mutating" }))).toMatchObject({
			acceptance: "durable",
			delivery: "pending",
		});
		expect(f.db.workControlGet(held.controlId)).toEqual(held);
		expect(f.db.workTaskGet(taskId)?.request.kind).toBe(kind);
		expect(f.port.steers).toHaveLength(1);
		expect(f.port.binds).toHaveLength(1);
	});

test("unfenced mutation admission never creates a worker or authorizes thread creation", async () => {
	const f = await fixture();
	await expect(
		f.manager.start({ ...f.input, task: { ...f.input.task, kind: "code_mutating" } }, context),
	).rejects.toThrow();
	await expect(f.manager.threadClaim({ taskId, claimId })).rejects.toThrow();
	expect(f.port.binds).toHaveLength(0);
	expect(f.port.sends).toHaveLength(0);
});

for (const invalid of ["key", "target", "initial-prompt", "provenance"] as const)
	test(`invalid persisted native ${invalid} fails before claim and permits only a restored original create`, async () => {
		let coordinatorCwd = "";
		const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
		const primary = join(f.directory, "primary");
		await initRepo(primary);
		coordinatorCwd = primary;
		await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
		const authorization = f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!;
		const raw = new Database(join(f.directory, "gateway.db"));
		const replace = (body: string, principalId = authorization.evidence.principalId) => {
			raw.query("DELETE FROM work_task_sources WHERE source_id = ?").run(authorization.sourceId);
			f.db.withTransaction(() =>
				f.db.workTaskSourceAppendInTransaction({
					sourceId: authorization.sourceId,
					taskId,
					kind: "decision",
					body,
					evidence: { ...authorization.evidence, principalId },
					supersedes: null,
					completeness: "complete",
					controlId: null,
					reportId: null,
				}),
			);
		};
		try {
			const wrong = JSON.parse(authorization.body);
			if (invalid === "key") wrong.requestKey = "";
			if (invalid === "target") wrong.target.readinessTimeoutMs = -1;
			if (invalid === "initial-prompt") wrong.target.body = "Mutate before task admission.";
			replace(JSON.stringify(wrong), invalid === "provenance" ? "coordinator:forged" : undefined);
			await expect(f.bind()).rejects.toThrow("native_allocation_authorization");
			expect(f.port.binds).toHaveLength(0);
			expect(f.port.sends).toHaveLength(0);
			expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeUndefined();
			replace(authorization.body);
		} finally {
			raw.close();
		}
		await f.restart();
		expect(f.port.binds).toHaveLength(0);
		// Invalid authorization was rejected before even claiming the surface.
		// Restore the original binding, not a replacement native identity.
		await f.bind();
		expect(f.port.binds).toHaveLength(1);
		expect(f.port.binds[0]!.managedWorktree).toMatchObject({
			idempotencyKey: JSON.parse(authorization.body).requestKey,
			createTarget: JSON.parse(authorization.body).target,
			lookupOnly: false,
		});
		expect(f.port.sends).toHaveLength(1);
	});

test("known-unsent capacity refusal leaves original native create permission available", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	const admission = spyOn(f.lanes, "assertAdmission").mockImplementation(() => {
		throw new Error("capacity_full");
	});
	try {
		await expect(f.bind()).rejects.toThrow("capacity_full");
		expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeUndefined();
		expect(f.port.binds).toHaveLength(0);
	} finally {
		admission.mockRestore();
	}
	await f.restart();
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.binds[0]!.managedWorktree?.lookupOnly).toBe(false);
	expect(f.port.sends).toHaveLength(1);
});

test("source checkout replacement during native create cannot acquire task ownership", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	const foreign = join(f.directory, "foreign");
	const source = join(f.directory, "source");
	await initRepo(primary);
	await initRepo(foreign);
	runGit(primary, "worktree", "add", "--detach", source, "HEAD");
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: source, task: { ...f.input.task, kind: "code_mutating" } }, context);
	f.native.override = (input) => {
		const checkout = allocateNativeCheckout(primary, input.managedWorktree!.idempotencyKey);
		runGit(primary, "worktree", "move", source, join(f.directory, "original-source"));
		runGit(foreign, "worktree", "add", "--detach", source, "HEAD");
		return checkout;
	};
	await expect(f.bind()).rejects.toThrow("source_checkout_identity_changed");
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
	expect(f.db.getSessionRecord(workSessionKey(name))).toBeUndefined();
	expect(f.port.sends).toHaveLength(0);
});

test("missing claim with retained allocation evidence never permits another create on restart", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	const bind = f.port.bind.bind(f.port);
	const lost = spyOn(f.port, "bind").mockImplementationOnce(async (input) => {
		await bind(input);
		throw new Error("receipt_lost");
	});
	await expect(f.bind()).rejects.toThrow("native_allocation_unresolved");
	lost.mockRestore();
	const original = f.db.workTaskSourceGet(`native-allocation-result-${taskId}`);
	const raw = new Database(join(f.directory, "gateway.db"));
	try {
		raw.query("DELETE FROM work_task_sources WHERE source_id = ?").run(`native-allocation-claimed-${taskId}`);
	} finally {
		raw.close();
	}
	await f.restart();
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(0);
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toEqual(original);
	expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeUndefined();
	expect(await f.manager.status({ name, taskId })).toMatchObject({
		state: "held",
		task: { holdReason: "native_allocation_unresolved" },
	});
});

test("nested native allocation succeeds under source/.worktrees and drives runtime, baseline, ownership and control", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const request = JSON.parse(f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!.body);
	const claim = JSON.parse(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)!.body);
	const allocation = JSON.parse(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)!.body);
	const checkout = allocation.executionCwd as string;
	expect(claim).toEqual({ requestKey: request.requestKey, opRef: task.opRef });
	expect(checkout).toBe(join(realpathSync(primary), ".worktrees", request.requestKey));
	expect(checkout).not.toBe(primary);
	// A real registered linked checkout of the source repository at the retained primary base.
	expect(runGit(primary, "worktree", "list", "--porcelain")).toContain(checkout);
	expect(runGit(checkout, "rev-parse", "HEAD")).toBe(request.source.base);
	// The assignment source stays immutable while the returned path drives execution.
	expect(task.request.cwd).toBe(primary);
	expect(f.port.binds[0]?.repo).toBe(primary);
	expect(f.port.binds[0]?.epoch).toBe(request.epoch);
	expect(f.port.binds[0]?.managedWorktree).toMatchObject({
		idempotencyKey: request.requestKey,
		lookupOnly: false,
	});
	expect(request.epoch).toBe(0);
	expect(request.target).toMatchObject({ cwd: primary, worktree: { enabled: true } });
	expect(request.target.readinessTimeoutMs).toBe(SESSION_CREATE_READINESS_MS);
	expect(request.source.root).toBe(realpathSync(primary));
	expect(request.source.base).toMatch(/^[0-9a-f]{40}$/);
	expect(request.source.base).toBe(request.source.head);
	// Returned identity drives the runtime, the lane baseline and the ownership binding.
	expect(allocation.sessionId).toBe(task.sessionId);
	expect(allocation.epoch).toBe(request.epoch);
	expect(f.port.sends[0]).toMatchObject({ sessionId: task.sessionId!, repo: checkout });
	const runtime = f.db.workAttemptGet(task.opRef)!;
	expect(runtime.cwd).toBe(checkout);
	const laneJob = JSON.parse(f.db.laneJobJsonByLaneKey(laneJobIdentity(name).laneKey)!);
	expect(laneJob.lane).toMatchObject({ worktreePath: checkout, branch: request.requestKey });
	expect(laneJob.baselineSha).toBe(request.source.base);
	const authority = f.db.inspectBrokerAuthority().authority!;
	const owned = f.db.assertOwnedSession(task.sessionId!, checkout, authority);
	expect(owned.originKey).toBe(workSessionKey(name));
	expect(f.db.getSessionRecord(workSessionKey(name))).toMatchObject({ sessionId: task.sessionId!, epoch: 0 });
	// The control scope proof carries the returned execution path, not the source.
	expect(await f.manager.admitMappedEvent(event("mutation", { scope: "code_mutating" }))).toMatchObject({
		delivery: "accepted",
	});
	const control = f.db.workControlList(taskId)[0]!;
	expect(control.request.scope).toBe("code_mutating");
	expect(JSON.parse(f.db.workTaskSourceGet(`worktree-${control.controlId}`)!.body).cwd).toBe(checkout);
	// Release assessment targets the returned path.
	f.port.complete(task.opRef, "Original coding result");
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	expect(f.manager.assessTaskRelease(name)).toMatchObject({ kind: "eligible", cwd: checkout });
	// The nested bucket really is ignored in the primary — the check is not vacuous.
	const proof = admitDedicatedWorktree(checkout, primary);
	expect(proof.cwd).toBe(checkout);
	assertManagedWorktreeIgnored(proof);
	await rm(join(primary, ".gitignore"));
	expect(() => assertManagedWorktreeIgnored(admitDedicatedWorktree(checkout, primary))).toThrow(
		"managed_worktree_bucket_not_ignored",
	);
});

test("original native request, key and epoch persist before any create and are reused verbatim", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd });
	const primary = join(f.directory, "primary");
	const worker = join(f.directory, "worker");
	await initRepo(primary);
	runGit(primary, "worktree", "add", "-b", "worker", worker);
	coordinatorCwd = primary;
	const admission = await f.manager.start(
		{ ...f.input, cwd: worker, task: { ...f.input.task, kind: "code_mutating" } },
		context,
	);
	expect(admission).toMatchObject({ accepted: "durable", execution: "pending_surface" });
	// The original authorization is durable before any bind or create attempt.
	expect(f.port.binds).toHaveLength(0);
	const stored = f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!;
	expect(stored).toMatchObject({ kind: "decision", completeness: "complete" });
	const request = JSON.parse(stored.body);
	expect(request.requestKey).toBe(sessionCreateRef(f.db.instanceId, workSessionKey(name), 0, worker));
	expect(request.epoch).toBe(0);
	expect(request.target).toMatchObject({
		cwd: worker,
		worktree: { enabled: true },
		readinessTimeoutMs: SESSION_CREATE_READINESS_MS,
	});
	expect(request.source).toMatchObject({ root: realpathSync(worker), commonDir: realpathSync(join(primary, ".git")) });
	expect(request.source.base).toBe(request.source.head);
	await f.bind();
	// The single create reuses the exact persisted key, epoch and provisioning input.
	expect(f.port.binds).toHaveLength(1);
	const binding = f.port.binds[0]!;
	expect(binding.managedWorktree).toMatchObject({ idempotencyKey: request.requestKey, lookupOnly: false });
	expect(binding.epoch).toBe(request.epoch);
	expect(binding.repo).toBe(worker);
	const claim = JSON.parse(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)!.body);
	expect(claim).toEqual({ requestKey: request.requestKey, opRef: f.db.workTaskGet(taskId)!.opRef });
	const allocation = JSON.parse(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)!.body);
	expect(allocation.sessionId).toBe(f.port.sends[0]?.sessionId);
	expect(allocation.epoch).toBe(request.epoch);
	expect(allocation.executionCwd).not.toBe(worker);
});

for (const refusal of ["source", "coordinator", "foreign", "other-task"] as const)
	test(`native ${refusal} checkout return is refused before any send, send authorization or owned binding`, async () => {
		let coordinatorCwd = "";
		const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
		const primary = join(f.directory, "primary");
		const worker = join(f.directory, "worker");
		await initRepo(primary);
		runGit(primary, "worktree", "add", "-b", "worker", worker);
		coordinatorCwd = primary;
		let returned: string;
		let expected: string;
		if (refusal === "source") {
			returned = realpathSync(worker);
			expected = "checkout_is_the_source";
		} else if (refusal === "coordinator") {
			const coordinatorCheckout = join(f.directory, "coordinator");
			runGit(primary, "worktree", "add", "-b", "coordinator-work", coordinatorCheckout);
			coordinatorCwd = coordinatorCheckout;
			returned = realpathSync(coordinatorCheckout);
			expected = "native_allocation_unresolved";
		} else if (refusal === "foreign") {
			const foreign = join(f.directory, "foreign");
			await initRepo(foreign);
			const foreignCheckout = join(foreign, ".worktrees", "foreign-worker");
			runGit(foreign, "worktree", "add", foreignCheckout);
			returned = realpathSync(foreignCheckout);
			expected = "checkout_foreign_to_source_repository";
		} else {
			const worker2 = join(f.directory, "worker2");
			runGit(primary, "worktree", "add", "-b", "worker2", worker2);
			const otherTaskId = crypto.randomUUID();
			const otherCoordinator: OriginRef = {
				platform: "discord",
				kind: "channel",
				conversationId: "900",
				boundaryId: "1",
			};
			await f.manager.start(
				{
					name: `fm-${otherTaskId}`,
					text: "Independent second assignment",
					cwd: worker2,
					task: { taskId: otherTaskId, kind: "read_only" as const, surface: { parentOrigin: otherCoordinator } },
				},
				{
					stableOrigin: otherCoordinator,
					evidence: {
						principalId: "owner",
						origin: otherCoordinator,
						eventId: "assignment-2",
						editId: null,
						evidenceAt: at,
						observedAt: at,
					},
				},
			);
			// The second task's retained dedicated admission already owns worker2.
			expect(f.db.workTaskSourceGet(`worktree-admission-${otherTaskId}`)).toBeDefined();
			returned = realpathSync(worker2);
			expected = "native_allocation_unresolved";
		}
		f.native.override = () => returned;
		await f.manager.start({ ...f.input, cwd: worker, task: { ...f.input.task, kind: "code_mutating" } }, context);
		expect(f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)).toBeDefined();
		await expect(f.bind()).rejects.toThrow(`native managed allocation held: ${expected}`);
		expect(f.port.binds).toHaveLength(1);
		expect(f.port.sends).toHaveLength(0);
		expect(f.port.steers).toHaveLength(0);
		expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
		expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeDefined();
		expect(f.db.workAttemptGet(f.db.workTaskGet(taskId)!.opRef)).toBeUndefined();
		expect(f.db.workTaskGet(taskId)).toMatchObject({ dispatchPhase: "pending", surfacePhase: "bound" });
		// Refused before any durable owned binding for the work lane.
		expect(f.db.getSessionRecord(workSessionKey(name))).toBeUndefined();
	});

test("a source subdirectory of the same checkout is refused as a native return", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	const worker = join(f.directory, "worker");
	await initRepo(primary);
	runGit(primary, "worktree", "add", "-b", "worker", worker);
	const subdir = join(worker, "module");
	await mkdir(subdir);
	coordinatorCwd = primary;
	f.native.override = () => realpathSync(subdir);
	await f.manager.start({ ...f.input, cwd: worker, task: { ...f.input.task, kind: "code_mutating" } }, context);
	await expect(f.bind()).rejects.toThrow("native managed allocation held: native_allocation_unresolved");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.sends).toHaveLength(0);
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
	expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeDefined();
	expect(f.db.getSessionRecord(workSessionKey(name))).toBeUndefined();
});

test("a source diverged from the canonical primary base is refused before create and claims no permission", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	const worker = join(f.directory, "worker");
	await initRepo(primary);
	runGit(primary, "worktree", "add", "-b", "worker", worker);
	runGit(
		worker,
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"source-only commit",
	);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: worker, task: { ...f.input.task, kind: "code_mutating" } }, context);
	const authorization = JSON.parse(f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!.body);
	expect(authorization.source.head).not.toBe(authorization.source.base);
	await expect(f.bind()).rejects.toThrow("native managed allocation held: source_repository_changed_or_divergent");
	expect(f.port.binds).toHaveLength(0);
	expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeUndefined();
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
	expect(await directoryExists(join(worker, ".worktrees"))).toBe(false);
	// The known-unsent refusal consumed no create permission: the lane still reads pending.
	expect(await f.manager.status({ name, taskId })).toMatchObject({ state: "pending" });
	expect(
		await f.manager.start({ ...f.input, cwd: worker, task: { ...f.input.task, kind: "code_mutating" } }, context),
	).toMatchObject({ accepted: "durable", execution: "pending_surface" });
	expect(f.port.binds).toHaveLength(0);
});

test("a failed native create recovers lookup-only: not_found holds the task without recreating a checkout", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd });
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	f.native.failCreate = true;
	await expect(f.bind()).rejects.toThrow("native managed allocation held: native_allocation_unresolved");
	expect(f.port.binds).toHaveLength(1);
	expect(f.port.binds[0].managedWorktree).toMatchObject({ lookupOnly: false });
	expect(f.db.workTaskSourceGet(`native-allocation-claimed-${taskId}`)).toBeDefined();
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
	expect(await directoryExists(join(primary, ".worktrees"))).toBe(false);
	// A claimed pending allocation reads as held in status and on a duplicate start.
	expect(await f.manager.status({ name, taskId })).toMatchObject({
		state: "held",
		task: { holdReason: "native_allocation_unresolved" },
	});
	expect(
		await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context),
	).toMatchObject({ held: true, reason: "native_allocation_unresolved" });
	f.native.failCreate = false;
	await f.restart();
	// Recovery is lookup-only: the missing original is a not_found, never a recreate.
	expect(f.port.binds).toHaveLength(2);
	expect(f.port.binds[1].managedWorktree).toMatchObject({ lookupOnly: true });
	expect(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)).toBeUndefined();
	expect(await directoryExists(join(primary, ".worktrees"))).toBe(false);
	expect(f.port.sends).toHaveLength(0);
	expect(f.db.workTaskGet(taskId)).toMatchObject({ dispatchPhase: "pending" });
});

test("lookup-only recovery reuses the original native result after the source HEAD advances", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd });
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	const bind = f.port.bind.bind(f.port);
	let crashed = false;
	const crash = spyOn(f.port, "bind").mockImplementation(async (input) => {
		const binding = await bind(input);
		if (input.managedWorktree && !crashed) {
			crashed = true;
			throw new Error("gateway lost after the native create");
		}
		return binding;
	});
	cleanup.push(async () => {
		crash.mockRestore();
	});
	// The create itself persisted; only the gateway-side recording of it was lost.
	await expect(f.bind()).rejects.toThrow("native managed allocation held: native_allocation_unresolved");
	const resultId = `native-allocation-result-${taskId}`;
	const persisted = f.db.workTaskSourceGet(resultId)!;
	expect(persisted).toBeDefined();
	const allocation = JSON.parse(persisted.body);
	const request = JSON.parse(f.db.workTaskSourceGet(`native-allocation-request-${taskId}`)!.body);
	const checkout = allocation.executionCwd as string;
	expect(checkout).toBe(join(realpathSync(primary), ".worktrees", request.requestKey));
	expect(f.db.workTaskGet(taskId)).toMatchObject({ dispatchPhase: "pending" });
	// The source advances beyond the retained authorization base.
	runGit(
		primary,
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"source advanced",
	);
	expect(runGit(primary, "rev-parse", "HEAD")).not.toBe(request.source.base);
	await f.restart();
	expect(f.port.binds).toHaveLength(2);
	expect(f.port.binds[1].managedWorktree).toMatchObject({ lookupOnly: true });
	expect(f.db.workTaskSourceGet(resultId)!.body).toBe(persisted.body);
	expect(await readdir(join(primary, ".worktrees"))).toEqual([request.requestKey]);
	expect(f.port.sends).toHaveLength(1);
	expect(f.port.sends[0]?.repo).toBe(checkout);
	expect(f.db.workTaskGet(taskId)?.request.cwd).toBe(primary);
});

test("retirement never removes the native checkout: uncommitted files and unintegrated commits survive", async () => {
	let coordinatorCwd = "";
	const f = await fixture({ coordinatorCwd: () => coordinatorCwd }, {}, true);
	const primary = join(f.directory, "primary");
	await initRepo(primary);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: primary, task: { ...f.input.task, kind: "code_mutating" } }, context);
	await f.bind();
	const task = f.db.workTaskGet(taskId)!;
	const allocation = JSON.parse(f.db.workTaskSourceGet(`native-allocation-result-${taskId}`)!.body);
	const checkout = allocation.executionCwd as string;
	expect(allocation.sessionId).toBe(task.sessionId);
	await writeFile(join(checkout, "owner-notes.txt"), "Uncommitted owner data");
	runGit(checkout, "add", "owner-notes.txt");
	runGit(
		checkout,
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"-m",
		"unique unintegrated commit",
	);
	const unintegrated = runGit(checkout, "rev-parse", "HEAD");
	expect(unintegrated).not.toBe(runGit(primary, "rev-parse", "HEAD"));
	f.port.complete(task.opRef, "Original coding result");
	await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
	expect(f.manager.assessTaskRelease(name)).toMatchObject({ kind: "eligible", cwd: checkout });
	expect(f.manager.taskDebt(name)).toMatchObject({ safeToReleaseExecution: true, safeToCleanupWorktree: false });
	// Release itself is gated on the nested-bucket ignore invariant.
	await rm(join(primary, ".gitignore"));
	expect(f.manager.assessTaskRelease(name)).toMatchObject({
		kind: "hold",
		reason: "task_original_worktree_proof_unavailable",
	});
	await writeFile(join(primary, ".gitignore"), ".worktrees/\n");
	expect(f.manager.assessTaskRelease(name)).toMatchObject({ kind: "eligible" });
	const jobs = spyOn(f.port, "runningJobs");
	const live = spyOn(f.port, "liveness").mockImplementation(async ({ sessionId }: { sessionId: string }) => ({
		live: sessionId !== allocation.sessionId,
		disowned: false,
	}));
	expect(await f.lanes.retire(name, "operator")).toMatchObject({
		retired: true,
		closed: true,
		sessionId: allocation.sessionId,
	});
	live.mockRestore();
	expect(jobs).toHaveBeenCalledWith({ sessionId: allocation.sessionId, repo: checkout });
	jobs.mockRestore();
	expect(f.port.closes).toEqual([{ sessionId: allocation.sessionId, repo: checkout }]);
	// Retirement released capacity only: the checkout and both kinds of work survive.
	expect(await readFile(join(checkout, "owner-notes.txt"), "utf8")).toBe("Uncommitted owner data");
	expect(runGit(checkout, "rev-parse", "HEAD")).toBe(unintegrated);
	expect(runGit(primary, "worktree", "list", "--porcelain")).toContain(checkout);
	expect(gitExitCode(primary, "merge-base", "--is-ancestor", unintegrated, runGit(primary, "rev-parse", "HEAD"))).toBe(
		1,
	);
	expect(f.lanes.activeLanes().map((lane) => lane.name)).toEqual([]);
	expect(f.db.getSessionRecord(workSessionKey(name))).toEqual({ sessionId: "", epoch: task.epoch! + 1 });
});

async function releaseFixture(remote = false, scope: "dedicated" | "shared" | "ordinary" = "dedicated") {
	let coordinatorCwd = "";
	const f = await fixture(
		{ coordinatorCwd: () => coordinatorCwd },
		remote
			? {
					onSteer: () => {
						throw new Error("remote outcome unknown");
					},
				}
			: {},
		true,
	);
	const primary = join(f.directory, "primary");
	const worker = scope === "shared" ? primary : scope === "ordinary" ? f.directory : join(f.directory, "worker");
	await mkdir(primary);
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
	if (scope === "dedicated") git("worktree", "add", "-b", "worker", worker);
	coordinatorCwd = primary;
	await f.manager.start({ ...f.input, cwd: worker }, context);
	await f.bind();
	await f.manager.admitMappedEvent(event("retained-control", { kind: remote ? "steer" : "cancel_request" }));
	const task = f.db.workTaskGet(taskId)!;
	const missing = spyOn(f.port, "fetchWorkerOutput").mockResolvedValue({
		status: "unavailable",
		code: "output_unavailable",
	});
	cleanup.push(async () => {
		missing.mockRestore();
	});
	return {
		...f,
		// A restart replaces the fixture's manager; read it live through the getter.
		get manager() {
			return f.manager;
		},
		worker,
		primary,
		task,
		drift: () => {
			coordinatorCwd = worker;
		},
		finish: async () => {
			f.port.complete(task.opRef, "Original result unavailable to the gateway");
			await until(() => f.db.workAttemptGet(task.opRef)?.settledAt != null);
		},
		restoreOutput: () => missing.mockRestore(),
	};
}

for (const scope of ["dedicated", "shared", "ordinary"] as const)
	for (const route of ["normal", "force", "all-dead"] as const) {
		test(`real manager and governor release only owned capacity with retained final/local-cancel debt (${scope}, ${route})`, async () => {
			const f = await releaseFixture(false, scope);
			const sentinel = join(f.worker, "retained-owner-file");
			await writeFile(sentinel, "Owner data must survive host release");
			expect(f.db.workTaskSourceGet(`worktree-admission-${taskId}`) !== undefined).toBe(scope === "dedicated");
			const healthy = await f.manager.start({
				name: "healthy",
				text: "Independent work",
				cwd: f.primary,
				callerSessionId: "persona-session",
			});
			if (!healthy.started) throw new Error("healthy lane did not start");
			await f.finish();
			expect(f.manager.assessTaskRelease(name)).toMatchObject({
				kind: "eligible",
				taskId,
				opRef: f.task.opRef,
				sessionId: f.task.sessionId,
				epoch: f.task.epoch,
				cwd: f.worker,
			});
			expect(f.manager.taskDebt(name)).toMatchObject({
				exactTerminal: true,
				safeToReleaseExecution: true,
				unresolvedControls: 1,
				unresolvedSteers: 0,
				safeToCleanupWorktree: false,
			});
			const taskBefore = f.db.workTaskGet(taskId);
			const controlsBefore = f.db.workControlList(taskId);
			const runtimeBefore = f.db.workAttemptGet(f.task.opRef);
			const sourcesBefore = f.db.workTaskSources(taskId);
			const deliveriesBefore = sourcesBefore!.sources
				.filter((source) => source.deliveryId !== null)
				.map((source) => f.db.deliveryGet(source.deliveryId!));
			const marker = f.db.workTaskSourceGet(`activation-${taskId}`)!;
			const deliveryBefore = f.db.deliveryGet(marker.deliveryId!);
			const healthyBinding = f.db.getSessionRecord(workSessionKey("healthy"));
			const jobs = spyOn(f.port, "runningJobs");
			const live = spyOn(f.port, "liveness").mockImplementation(async ({ sessionId }) => ({
				live: sessionId !== f.task.sessionId,
				disowned: false,
			}));
			if (route === "all-dead") expect(await f.lanes.retireAllDead()).toEqual({ count: 1, names: [name] });
			else
				expect(await (route === "force" ? f.lanes.forceRetire(name) : f.lanes.retire(name, "operator"))).toMatchObject({
					retired: true,
					closed: true,
					sessionId: f.task.sessionId,
				});
			live.mockRestore();
			expect(jobs).toHaveBeenCalledWith({ sessionId: f.task.sessionId!, repo: f.worker });
			jobs.mockRestore();
			expect(f.port.closes).toEqual([{ sessionId: f.task.sessionId!, repo: f.worker }]);
			expect(await readFile(sentinel, "utf8")).toBe("Owner data must survive host release");
			expect(f.lanes.activeLanes().map((lane) => lane.name)).toEqual(["healthy"]);
			expect(f.db.getSessionRecord(workSessionKey(name))).toEqual({ sessionId: "", epoch: f.task.epoch! + 1 });
			expect(f.db.getSessionRecord(workSessionKey("healthy"))).toEqual(healthyBinding);
			expect(f.db.workTaskGet(taskId)).toEqual(taskBefore);
			expect(f.db.workControlList(taskId)).toEqual(controlsBefore);
			expect(f.db.workAttemptGet(f.task.opRef)).toEqual(runtimeBefore);
			expect(f.db.workTaskSources(taskId)).toEqual(sourcesBefore);
			expect(
				sourcesBefore!.sources
					.filter((source) => source.deliveryId !== null)
					.map((source) => f.db.deliveryGet(source.deliveryId!)),
			).toEqual(deliveriesBefore);
			expect(f.db.deliveryGet(marker.deliveryId!)).toEqual(deliveryBefore);
			expect(f.db.workTaskDiscordLocator(thread.conversationId)).toEqual({ taskId });
			expect(f.db.workTaskByThread(originKey(thread))?.taskId).toBe(taskId);
			expect(await f.manager.admitMappedEvent(event("retained-control", { kind: "cancel_request" }))).toMatchObject({
				controlId: controlsBefore[0]!.controlId,
				delivery: "held",
			});
			expect(f.db.workControlList(taskId)).toEqual(controlsBefore);
			expect(f.manager.assessTaskRelease(name).kind).toBe("hold");
			f.port.complete(healthy.opRef, "Healthy original result");
			await until(() => f.db.workAttemptGet(healthy.opRef)?.settledAt != null);
			expect(f.port.sends).toHaveLength(2);
			expect(f.port.steers).toHaveLength(0);
			expect(f.port.resumes).toHaveLength(0);
		});
	}

for (const scope of ["dedicated", "shared", "ordinary"] as const)
	for (const obstacle of [
		"remote-held",
		"uncertain-execution",
		"unsupported-jobs",
		"unreadable-jobs",
		"live-jobs",
		"false-close",
		"worktree-drift",
		"epoch-race",
		"authority-race",
		"worktree-race",
		"postclose-race",
		"release-write-failure",
	] as const) {
		if (scope !== "dedicated" && ["worktree-drift", "worktree-race"].includes(obstacle)) continue;
		test(`mapped normal, force and all-dead routes retain capacity on ${obstacle} (${scope})`, async () => {
			const f = await releaseFixture(obstacle === "remote-held", scope);
			if (obstacle !== "uncertain-execution") await f.finish();
			if (obstacle === "remote-held") {
				expect(f.manager.assessTaskRelease(name)).toMatchObject({
					kind: "hold",
					reason: "task_remote_control_unresolved",
				});
				expect(f.manager.taskDebt(name)?.safeToReleaseExecution).toBe(false);
			}
			if (obstacle === "worktree-drift") f.drift();
			if (obstacle === "unsupported-jobs") (f.port as SessionPort).runningJobs = undefined;
			if (obstacle === "unreadable-jobs") f.port.hostJobs.set(f.task.sessionId!, new Error("host cannot answer"));
			if (obstacle === "live-jobs")
				f.port.hostJobs.set(f.task.sessionId!, [{ id: "child", type: "task", label: "still running" }]);
			const bindingBefore = f.db.getSessionRecord(workSessionKey(name));
			const controlsBefore = f.db.workControlList(taskId);
			const spies: Array<{ mockRestore(): void }> = [];
			if (obstacle === "false-close")
				spies.push(
					spyOn(f.port, "close").mockImplementation(async (input): Promise<void> => {
						f.port.closes.push(input);
						// Promise<void> rejects unsupported/false closure, never resolves a fabricated false DTO.
						throw new Error("session.close ordinary host closure is unproven: closed:false");
					}),
				);
			let raced = false;
			if (["epoch-race", "authority-race", "worktree-race"].includes(obstacle)) {
				spies.push(
					spyOn(f.port, "runningJobs").mockImplementation(async () => {
						if (!raced) {
							raced = true;
							if (obstacle === "worktree-race") f.drift();
							else if (obstacle === "epoch-race") f.db.rebindEpoch(workSessionKey(name));
							else {
								const authority = f.db.inspectBrokerAuthority().authority!;
								f.db.cutoverBrokerAuthority({
									expectedAuthority: authority,
									targetAuthority: { ...authority, identity: `${authority.identity}:replacement` },
									evidence: "fixture authority replacement",
									disposition: "quarantine",
								});
							}
						}
						return [];
					}),
				);
			}
			if (obstacle === "postclose-race")
				spies.push(
					spyOn(f.port, "close").mockImplementation(async (input) => {
						f.port.closes.push(input);
						if (scope === "dedicated") f.drift();
						else reviseDebt();
					}),
				);
			function reviseDebt() {
				const task = f.db.workTaskGet(taskId)!;
				f.db.withTransaction(() =>
					f.db.workTaskObligationInTransaction(taskId, task.version, {
						identity: { opRef: task.opRef, sessionId: task.sessionId!, epoch: task.epoch! },
						state: "held",
						reason: "new debt after host closure",
						at,
					}),
				);
			}
			if (obstacle === "release-write-failure")
				spies.push(
					spyOn(f.db, "rebindEpoch").mockImplementation(() => {
						throw new Error("release transaction failed after host closure");
					}),
				);
			spies.push(spyOn(f.port, "liveness").mockResolvedValue({ live: false, disowned: true }));
			try {
				expect(await f.lanes.retire(name, "operator")).toMatchObject({ retired: false });
				expect(await f.lanes.forceRetire(name)).toMatchObject({ retired: false });
				expect(await f.lanes.retireAllDead()).toEqual({ count: 0, names: [] });
			} finally {
				for (const spy of spies.reverse()) spy.mockRestore();
			}
			if (!["epoch-race", "authority-race"].includes(obstacle))
				expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(bindingBefore);
			expect(f.db.workControlList(taskId)).toEqual(controlsBefore);
			expect(f.db.workTaskDiscordLocator(thread.conversationId)).toEqual({ taskId });
			expect(f.port.sends).toHaveLength(1);
			expect(f.port.resumes).toHaveLength(0);
			if (!["false-close", "postclose-race", "release-write-failure"].includes(obstacle))
				expect(f.port.closes).toHaveLength(0);
		});
	}

for (const boundary of ["jobs", "close"] as const)
	test(`task version is rechecked after awaited ${boundary}`, async () => {
		const f = await releaseFixture();
		await f.finish();
		const before = f.db.getSessionRecord(workSessionKey(name));
		const reviseDebt = () => {
			const current = f.db.workTaskGet(taskId)!;
			f.db.withTransaction(() => {
				expect(
					f.db.workTaskObligationInTransaction(taskId, current.version, {
						identity: { opRef: current.opRef, sessionId: current.sessionId!, epoch: current.epoch! },
						state: "held",
						reason: "new original report debt observation",
						at: new Date().toISOString(),
					}),
				).toBeDefined();
			});
		};
		const probe =
			boundary === "jobs"
				? spyOn(f.port, "runningJobs").mockImplementation(async () => {
						reviseDebt();
						return [];
					})
				: spyOn(f.port, "close").mockImplementation(async (input) => {
						f.port.closes.push(input);
						reviseDebt();
					});
		try {
			expect(await f.lanes.retire(name, "operator")).toMatchObject({
				retired: false,
				reason:
					boundary === "jobs"
						? "task_release_identity_changed"
						: "task_release_changed_after_close_closure_effect_retained",
			});
		} finally {
			probe.mockRestore();
		}
		expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(before);
		expect(f.port.closes).toHaveLength(boundary === "jobs" ? 0 : 1);
		expect(f.db.workTaskGet(taskId)?.holdReason).toBe("new original report debt observation");
		expect(f.port.sends).toHaveLength(1);
	});

for (const damage of ["op", "session", "receipt"] as const)
	test(`wrong original ${damage} proof never releases a mapped task`, async () => {
		const f = await releaseFixture();
		await f.finish();
		const runtime = f.db.workAttemptGet(f.task.opRef)!;
		const raw = new Database(join(f.directory, "gateway.db"));
		cleanup.push(async () => {
			raw.close();
		});
		const controlsBefore = raw.query("SELECT * FROM work_controls WHERE task_id = ?").all(taskId);
		const record =
			damage === "op"
				? { ...runtime, opRef: "wrong-original-operation" }
				: damage === "session"
					? { ...runtime, sessionId: "wrong-original-session" }
					: {
							...runtime,
							terminal: { ...runtime.terminal, status: { ...runtime.terminal!.status, receiptState: "missing" } },
						};
		raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(record), runtime.opRef);
		const damaged = raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(runtime.opRef);
		const before = f.db.getSessionRecord(workSessionKey(name));
		const live = spyOn(f.port, "liveness").mockResolvedValue({ live: false, disowned: true });
		try {
			expect(f.manager.assessTaskRelease(name).kind).toBe("hold");
			expect(await f.lanes.retire(name, "operator")).toMatchObject({ retired: false });
			expect(await f.lanes.forceRetire(name)).toMatchObject({ retired: false });
			expect(await f.lanes.retireAllDead()).toEqual({ count: 0, names: [] });
		} finally {
			live.mockRestore();
		}
		expect(f.db.getSessionRecord(workSessionKey(name))).toEqual(before);
		expect(raw.query("SELECT * FROM work_controls WHERE task_id = ?").all(taskId)).toEqual(controlsBefore);
		expect(raw.query("SELECT * FROM work_attempt_runtime WHERE op_ref = ?").get(runtime.opRef)).toEqual(damaged);
		expect(f.db.workTaskDiscordLocator(thread.conversationId)).toEqual({ taskId });
		expect(f.port.closes).toHaveLength(0);
		expect(f.port.sends).toHaveLength(1);
		expect(f.port.resumes).toHaveLength(0);
	});

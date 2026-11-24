import { afterAll, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const modulePath = "coding-agent/src/session/lane-model-switch.ts";
const patchPath = join(import.meta.dir, "../src/patches/gajae-code/0.21.1-lane-switch.patch");

function extractHunk(patch: string): { count: number; body: string[] } {
	const lines = patch.split("\n");
	const start = lines.findIndex(line => line === `+++ b/${modulePath}`);
	if (start < 0) throw new Error(`patch has no hunk for ${modulePath}`);
	const header = lines[start + 1] ?? "";
	const match = /^@@ -0,0 \+1,(\d+) @@$/.exec(header);
	if (!match) throw new Error(`unexpected hunk header: ${header}`);
	const body: string[] = [];
	for (const line of lines.slice(start + 2)) {
		if (!line.startsWith("+")) break;
		body.push(line.slice(1));
	}
	return { count: Number(match[1]), body };
}

const hunk = extractHunk(await readFile(patchPath, "utf8"));
const source = hunk.body.join("\n");
const directory = await mkdtemp(join(tmpdir(), "lane-model-switch-"));
const sourcePath = join(directory, "lane-model-switch.ts");
await writeFile(sourcePath, `${source}\n`);

interface Turn {
	message: { role: string };
	toolResults: any[];
	messages: any[];
}

interface LaneOptions {
	agent: any;
	enabled: boolean;
	resolveExecutor: () => Promise<{ model: any; thinkingLevel?: string } | undefined>;
	waitForPersistence: () => Promise<void>;
	commit: (model: any, thinkingLevel: string | undefined) => Promise<void>;
	currentModel: () => any;
	currentThinkingLevel: () => string | undefined;
	activeToolNames: () => readonly string[];
	notify: (message: string) => void;
	warn: (message: string) => void;
}

interface LaneModule {
	PREWALK_PLAN: string;
	PREWALK_CONTINUE: string;
	PREWALK_CHECKLIST: string;
	installLaneModelSwitch: (options: LaneOptions) => void;
}

const lane = (await import(pathToFileURL(sourcePath).href)) as LaneModule;

afterAll(async () => {
	await rm(directory, { recursive: true, force: true });
});

function planMessage(): any {
	return {
		role: "custom",
		customType: "prewalk-plan",
		content: lane.PREWALK_PLAN,
		display: false,
		attribution: "agent",
		timestamp: 1,
	};
}

function continueMessage(): any {
	return {
		role: "custom",
		customType: "prewalk-continue",
		content: lane.PREWALK_CONTINUE,
		display: false,
		attribution: "agent",
		timestamp: 2,
	};
}

function result(toolName: string, options: { isError?: boolean } = {}): any {
	return {
		role: "toolResult",
		toolName,
		toolCallId: `${toolName}-1`,
		isError: options.isError ?? false,
		timestamp: 3,
	};
}

function customTypes(steers: any[]): string[] {
	return steers.filter(message => message.role === "custom").map(message => message.customType);
}

interface HarnessOptions {
	enabled?: boolean;
	model?: any;
	target?: any;
	thinkingLevel?: string;
	currentModel?: () => any;
	currentThinkingLevel?: () => string | undefined;
	resolveExecutor?: () => Promise<{ model: any; thinkingLevel?: string } | undefined>;
	commit?: () => Promise<void>;
	persist?: () => Promise<void>;
	activeToolNames?: readonly string[];
	stateMessages?: any[];
}

function harness(options: HarnessOptions = {}) {
	const steers: any[] = [];
	const notices: string[] = [];
	const warns: string[] = [];
	const commits: Array<{ model: any; thinkingLevel: string | undefined }> = [];
	const events: string[] = [];
	const steeringQueue: any[] = [];
	const state = { messages: [...(options.stateMessages ?? [])] };
	const agent = {
		state,
		steeringQueue,
		steer(message: any) {
			steeringQueue.push(message);
			steers.push(message);
			events.push(`steer:${message.customType}`);
			return { admitted: true };
		},
		replaceMessages(messages: any[]) {
			state.messages = messages.slice();
			events.push("replaceMessages");
		},
		removeQueuedMessages(predicate: (message: any) => boolean, scope?: string) {
			if (scope !== "followUp") {
				const kept = steeringQueue.filter(message => !predicate(message));
				steeringQueue.length = 0;
				steeringQueue.push(...kept);
			}
			events.push("removeQueuedMessages");
			return { steering: 0, followUp: 0, total: 0 };
		},
		onCompletedTurn: undefined as ((turn: Turn) => Promise<void>) | undefined,
	};
	const model = options.model ?? { provider: "planner", id: "strong" };
	const target = options.target ?? { provider: "executor", id: "cheap" };
	const thinkingLevel = options.thinkingLevel ?? "high";
	const resolveExecutor = options.resolveExecutor ?? (async () => ({ model: target, thinkingLevel }));
	const commit = options.commit ?? (async () => {});
	lane.installLaneModelSwitch({
		agent,
		enabled: options.enabled ?? true,
		resolveExecutor: async () => {
			events.push("resolve");
			return await resolveExecutor();
		},
		waitForPersistence: async () => {
			events.push("persist");
			await options.persist?.();
		},
		commit: async (committedModel, committedLevel) => {
			events.push("commit");
			commits.push({ model: committedModel, thinkingLevel: committedLevel });
			await commit();
		},
		currentModel: options.currentModel ?? (() => model),
		currentThinkingLevel: options.currentThinkingLevel ?? (() => thinkingLevel),
		activeToolNames: () => options.activeToolNames ?? ["edit", "write", "apply_patch", "ast_edit", "todo_write"],
		notify: message => notices.push(message),
		warn: message => warns.push(message),
	});
	const start = (toolResults: any[], liveMessages: any[] = []): Promise<void> => {
		if (!agent.onCompletedTurn) return Promise.reject(new Error("the lane installed no completed-turn hook"));
		return agent.onCompletedTurn({ message: { role: "assistant" }, toolResults, messages: liveMessages });
	};
	const end = async (toolResults: any[], liveMessages: any[] = []): Promise<void> => {
		await start(toolResults, liveMessages);
	};
	return { agent, steers, notices, warns, commits, events, steeringQueue, state, target, model, start, end };
}

test("the lane-model-switch hunk header count matches its added lines", () => {
	expect(hunk.count).toBe(hunk.body.length);
	expect(hunk.body[0]?.startsWith("import type {")).toBe(true);
	expect(source).toContain("export const PREWALK_PLAN = ");
	expect(source).toContain("export const PREWALK_CONTINUE = ");
	expect(source).toContain("export const PREWALK_CHECKLIST = ");
});

test("installing the lane never hooks or blocks tool execution", () => {
	const laneRun = harness();
	expect((laneRun.agent as Record<string, unknown>).beforeToolCall).toBeUndefined();
	expect(source).not.toContain("beforeToolCall");
	expect(source).not.toContain("block: true");
	expect(source).not.toContain("EDIT_BLOCK_REASON");
	expect(source).not.toContain("followUp(");
	expect(source).not.toContain("planning phase is complete");
});

test("the plan nudge is steered once and hidden", async () => {
	const laneRun = harness();
	await laneRun.end([result("read")]);
	expect(laneRun.steers).toHaveLength(1);
	const [plan] = laneRun.steers;
	expect(plan.role).toBe("custom");
	expect(plan.customType).toBe("prewalk-plan");
	expect(plan.display).toBe(false);
	expect(plan.attribution).toBe("agent");
	expect(plan.content).toContain("STOP: In NEXT reply, before further exploration, write complete plan.");
	expect(plan.content).toContain("use todo tool to capture 5–9 items");
	expect(plan.content).toContain("Checkpoint, not final answer");
	expect(plan.content).not.toContain("Do not call");

	await laneRun.end([result("read")]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan"]);
});

test("a continue nudge follows tool results during planning", async () => {
	const laneRun = harness();
	await laneRun.end([result("read")]);
	await laneRun.end([result("read")]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan"]);

	await laneRun.end([]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan", "prewalk-continue"]);
	const continued = laneRun.steers[1];
	expect(continued.content).toBe("Continue task now; do not end turn here.");
	expect(continued.display).toBe(false);
	expect(continued.role).toBe("custom");

	await laneRun.end([]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan", "prewalk-continue"]);
});

test("a text-only planning reply gets the plan nudge, then a continue nudge", async () => {
	const laneRun = harness();
	await laneRun.end([]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan"]);

	await laneRun.end([]);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan", "prewalk-continue"]);
	expect(laneRun.commits).toHaveLength(0);
});

test("an edit before todo_write never switches the model", async () => {
	const laneRun = harness();
	await laneRun.end([result("edit")]);
	expect(laneRun.commits).toHaveLength(0);
	expect(customTypes(laneRun.steers)).toEqual(["prewalk-plan"]);
	expect(laneRun.events).not.toContain("persist");
});

test("one switch after the first edit batch once todo exists, awaited before the next model call", async () => {
	let releaseCommit!: () => void;
	const commitGate = new Promise<void>(resolve => {
		releaseCommit = resolve;
	});
	let commitStarted!: () => void;
	const started = new Promise<void>(resolve => {
		commitStarted = resolve;
	});
	const laneRun = harness({
		commit: async () => {
			commitStarted();
			await commitGate;
		},
	});

	await laneRun.end([result("read")]);
	await laneRun.end([result("todo_write")]);
	const pending = laneRun.start([result("edit")]);
	await started;
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");
	releaseCommit();
	await pending;

	expect(laneRun.commits).toEqual([{ model: laneRun.target, thinkingLevel: "high" }]);
	expect(customTypes(laneRun.steers)).toContain("prewalk-checklist");
	expect(laneRun.events.indexOf("persist")).toBeLessThan(laneRun.events.indexOf("commit"));
	expect(laneRun.events.indexOf("commit")).toBeLessThan(laneRun.events.indexOf("steer:prewalk-checklist"));
	expect(laneRun.warns).toEqual([]);

	await laneRun.end([result("edit")]);
	expect(laneRun.commits).toHaveLength(1);
	expect(customTypes(laneRun.steers).filter(type => type === "prewalk-checklist")).toHaveLength(1);
});

test("the checklist carries the upstream verification text and is steered once", async () => {
	const laneRun = harness();
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("write")]);
	const checklists = laneRun.steers.filter(message => message.customType === "prewalk-checklist");
	expect(checklists).toHaveLength(1);
	expect(checklists[0]?.content).toContain("Before task complete, verify:");
	expect(checklists[0]?.content).toContain("Do not claim task complete until all three checks done.");
	expect(checklists[0]?.display).toBe(false);

	for (const toolName of ["write", "edit", "apply_patch", "ast_edit"]) await laneRun.end([result(toolName)]);
	expect(customTypes(laneRun.steers).filter(type => type === "prewalk-checklist")).toHaveLength(1);
	expect(laneRun.commits).toHaveLength(1);
});

test("bash never triggers the switch", async () => {
	const laneRun = harness();
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("bash")]);
	expect(laneRun.commits).toHaveLength(0);
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");
});

test("a failed edit never triggers the switch", async () => {
	const laneRun = harness();
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("edit", { isError: true })]);
	expect(laneRun.commits).toHaveLength(0);
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");
});

test("the switch scrubs the plan nudge from context, state, and the steering queue", async () => {
	const stateMessages = [planMessage(), continueMessage(), { role: "user", content: "task", timestamp: 0 }];
	const laneRun = harness({ stateMessages });
	const liveMessages = [continueMessage(), planMessage()];
	laneRun.steeringQueue.push(planMessage());

	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("edit")], liveMessages);

	expect(liveMessages.map(message => message.customType)).toEqual(["prewalk-continue"]);
	expect(laneRun.state.messages.map(message => message.customType)).toEqual(["prewalk-continue", undefined]);
	expect(laneRun.steeringQueue.filter(message => message.customType === "prewalk-plan")).toHaveLength(0);
	expect(laneRun.steeringQueue.filter(message => message.customType === "prewalk-checklist")).toHaveLength(1);
	expect(laneRun.commits).toHaveLength(1);
});

test("a same-model switch is skipped with a notice and never retried", async () => {
	const model = { provider: "executor", id: "cheap" };
	const laneRun = harness({
		model,
		target: { provider: "executor", id: "cheap" },
		currentModel: () => model,
	});
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("edit")]);
	expect(laneRun.commits).toHaveLength(0);
	const skips = laneRun.notices.filter(message => message.includes("already matches the active model"));
	expect(skips).toHaveLength(1);
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");

	await laneRun.end([result("edit")]);
	expect(laneRun.commits).toHaveLength(0);
	expect(laneRun.notices.filter(message => message.includes("already matches the active model"))).toHaveLength(1);
});

test("a failing commit warns once, keeps the model, and never retries", async () => {
	const laneRun = harness({
		commit: async () => {
			throw new Error("no api key");
		},
	});
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("edit")]);
	expect(laneRun.warns).toHaveLength(1);
	expect(laneRun.warns[0]?.startsWith("lane_model_switch failed; keeping the current model:")).toBe(true);
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");

	await laneRun.end([result("edit")]);
	expect(laneRun.commits).toHaveLength(1);
	expect(laneRun.warns).toHaveLength(1);
});

test("a missing executor warns and commits nothing", async () => {
	const laneRun = harness({ resolveExecutor: async () => undefined });
	await laneRun.end([result("todo_write")]);
	await laneRun.end([result("edit")]);
	expect(laneRun.warns).toEqual([
		"lane_model_switch executor assignment is missing; keeping the current model",
	]);
	expect(laneRun.commits).toHaveLength(0);
	expect(customTypes(laneRun.steers)).not.toContain("prewalk-checklist");
});

test("a session that does not enable the lane installs nothing (GAJAEWAY_LANE_SWITCH unset)", () => {
	// The session passes enabled=false when GAJAEWAY_LANE_SWITCH is unset, for a
	// child session, or for a reopened session that already has messages.
	const laneRun = harness({ enabled: false });
	expect(laneRun.agent.onCompletedTurn).toBeUndefined();
	expect(laneRun.steers).toHaveLength(0);
	expect(laneRun.notices).toHaveLength(0);
	expect(laneRun.warns).toHaveLength(0);
});

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
const directory = await mkdtemp(join(tmpdir(), "lane-model-switch-"));
const sourcePath = join(directory, "lane-model-switch.ts");
await writeFile(sourcePath, `${hunk.body.join("\n")}\n`);

interface LaneModule {
	LANE_PLAN: string;
	LANE_CHECKLIST: string;
	installLaneModelSwitch: (options: {
		agent: unknown;
		enabled: boolean;
		resolveExecutor: () => Promise<{ model: unknown; thinkingLevel?: unknown } | undefined>;
		commit: (model: unknown, thinkingLevel: unknown) => Promise<void>;
		warn: (message: string) => void;
	}) => void;
}

const lane = (await import(pathToFileURL(sourcePath).href)) as LaneModule;

afterAll(async () => {
	await rm(directory, { recursive: true, force: true });
});

type ToolContext = { toolCall: { id: string; name: string } };
type Listener = (event: any) => void;
type ToolHook = (context: ToolContext, signal?: AbortSignal) => any;

function toolContext(name: string): ToolContext {
	return { toolCall: { id: `${name}-1`, name } };
}

function planMessages(): any[] {
	return [{ role: "user", content: [{ type: "text", text: lane.LANE_PLAN }], timestamp: 1 }];
}

function assistantEnd(content: any[]): any {
	return { type: "message_end", message: { role: "assistant", content, timestamp: Date.now() } };
}

function todoWriteEnd(isError = false): any {
	return { type: "tool_execution_end", toolCallId: "todo-1", toolName: "todo_write", result: {}, isError };
}

function harness(options: {
	enabled?: boolean;
	resolve?: () => Promise<{ model: unknown; thinkingLevel?: unknown } | undefined>;
	commit?: (model: unknown, thinkingLevel: unknown) => Promise<void>;
} = {}) {
	const listeners: Listener[] = [];
	const followUps: any[] = [];
	const passedThrough: string[] = [];
	const sentinel = { block: false, reason: "previous hook" };
	const previousHook: ToolHook = context => {
		passedThrough.push(context.toolCall.name);
		return sentinel;
	};
	const agent = {
		subscribe(fn: Listener) {
			listeners.push(fn);
			return () => {};
		},
		followUp(message: any) {
			followUps.push(message);
		},
		beforeModelCall: undefined as ((messages: any[]) => Promise<any> | any) | undefined,
		beforeToolCall: previousHook as ToolHook | undefined,
	};
	const model = { provider: "p", id: "cheap" };
	const thinkingLevel = "high";
	let commitCalls = 0;
	const commits: Array<{ model: unknown; thinkingLevel: unknown }> = [];
	const warns: string[] = [];
	lane.installLaneModelSwitch({
		agent,
		enabled: options.enabled ?? true,
		resolveExecutor: options.resolve ?? (async () => ({ model, thinkingLevel })),
		commit: async (committedModel, committedLevel) => {
			commitCalls += 1;
			commits.push({ model: committedModel, thinkingLevel: committedLevel });
			await options.commit?.(committedModel, committedLevel);
		},
		warn: message => warns.push(message),
	});
	return {
		agent,
		listeners,
		followUps,
		passedThrough,
		sentinel,
		previousHook,
		model,
		thinkingLevel,
		warns,
		commits,
		commitCalls: () => commitCalls,
	};
}

test("the lane-model-switch hunk header count matches its added lines", () => {
	expect(hunk.count).toBe(hunk.body.length);
	expect(hunk.body[0]).toBe('import type { Agent, AgentMessage, ThinkingLevel } from "@gajae-code/agent-core";');
	expect(hunk.body.join("\n")).toContain("export const LANE_PLAN = ");
	expect(hunk.body.join("\n")).toContain("export const LANE_CHECKLIST = ");
});

test("edits are blocked before planning ends and other tools reach the previous hook", async () => {
	const laneRun = harness();
	for (const name of ["edit", "write", "apply_patch", "ast_edit"]) {
		const result = await laneRun.agent.beforeToolCall?.(toolContext(name));
		expect(result?.block).toBe(true);
		expect(String(result?.reason).toLowerCase()).toContain("planning");
	}
	expect(laneRun.passedThrough).toEqual([]);
	const passed = await laneRun.agent.beforeToolCall?.(toolContext("read"));
	expect(passed).toBe(laneRun.sentinel);
	expect(laneRun.passedThrough).toEqual(["read"]);
});

test("a successful todo_write ends planning and the next model call commits the executor", async () => {
	const laneRun = harness();
	const first = await laneRun.agent.beforeModelCall?.([]);
	expect(first).toHaveLength(1);
	expect(first?.[0]?.role).toBe("user");
	expect(first?.[0]?.content[0]?.text).toBe(lane.LANE_PLAN);

	for (const listener of laneRun.listeners) listener(todoWriteEnd());
	expect((await laneRun.agent.beforeToolCall?.(toolContext("edit")))?.block).toBe(true);

	const messages = planMessages();
	const switched = await laneRun.agent.beforeModelCall?.(messages);
	expect(laneRun.commits).toEqual([{ model: laneRun.model, thinkingLevel: laneRun.thinkingLevel }]);
	expect(laneRun.warns).toEqual([]);
	expect(switched).toHaveLength(1);
	expect(switched?.[0]?.content[0]?.text).toBe(lane.LANE_CHECKLIST);
	expect(messages[0].content[0].text).toBe("Planning phase is complete.");
	expect(await laneRun.agent.beforeToolCall?.(toolContext("edit"))).toBe(laneRun.sentinel);

	expect(await laneRun.agent.beforeModelCall?.(messages)).toBeUndefined();
	expect(laneRun.commitCalls()).toBe(1);
});

test("a reply with no tool calls sends Continue and the next model call commits", async () => {
	const laneRun = harness();
	await laneRun.agent.beforeModelCall?.([]);
	for (const listener of laneRun.listeners) listener(assistantEnd([{ type: "text", text: "the plan" }]));
	expect(laneRun.followUps).toHaveLength(1);
	expect(laneRun.followUps[0]?.content[0]?.text).toBe("Continue.");

	const switched = await laneRun.agent.beforeModelCall?.(planMessages());
	expect(laneRun.commitCalls()).toBe(1);
	expect(switched?.[0]?.content[0]?.text).toBe(lane.LANE_CHECKLIST);
});

test("any reply with no tool calls ends planning and switches exactly once", async () => {
	const laneRun = harness();
	await laneRun.agent.beforeModelCall?.([]);
	for (const listener of laneRun.listeners)
		listener(assistantEnd([{ type: "toolCall", id: "call-1", name: "read", arguments: {} }]));
	expect(laneRun.followUps).toHaveLength(0);
	for (const listener of laneRun.listeners)
		listener(assistantEnd([{ type: "toolCall", id: "call-2", name: "read", arguments: {} }]));
	expect(laneRun.followUps).toHaveLength(0);
	expect((await laneRun.agent.beforeToolCall?.(toolContext("edit")))?.block).toBe(true);

	for (const listener of laneRun.listeners) listener(assistantEnd([{ type: "text", text: "later plan" }]));
	expect(laneRun.followUps).toHaveLength(1);
	expect(laneRun.followUps[0]?.content[0]?.text).toBe("Continue.");

	const switched = await laneRun.agent.beforeModelCall?.(planMessages());
	expect(switched?.[0]?.content[0]?.text).toBe(lane.LANE_CHECKLIST);
	expect(laneRun.commitCalls()).toBe(1);
	expect(await laneRun.agent.beforeModelCall?.(planMessages())).toBeUndefined();
	expect(laneRun.commitCalls()).toBe(1);
	expect(await laneRun.agent.beforeToolCall?.(toolContext("edit"))).toBe(laneRun.sentinel);
});

test("a failed todo_write does not end planning", async () => {
	const laneRun = harness();
	await laneRun.agent.beforeModelCall?.([]);
	for (const listener of laneRun.listeners) listener(todoWriteEnd(true));
	expect((await laneRun.agent.beforeToolCall?.(toolContext("write")))?.block).toBe(true);
	expect(await laneRun.agent.beforeModelCall?.(planMessages())).toBeUndefined();
	expect(laneRun.commitCalls()).toBe(0);
});

test("a failing commit warns once, keeps the model, and never retries", async () => {
	const laneRun = harness({
		commit: async () => {
			throw new Error("no api key");
		},
	});
	await laneRun.agent.beforeModelCall?.([]);
	for (const listener of laneRun.listeners) listener(todoWriteEnd());
	expect(await laneRun.agent.beforeModelCall?.(planMessages())).toBeUndefined();
	expect(laneRun.warns).toHaveLength(1);
	expect(laneRun.warns[0]?.startsWith("lane_model_switch failed; keeping the current model:")).toBe(true);
	expect(laneRun.commitCalls()).toBe(1);
	expect(await laneRun.agent.beforeToolCall?.(toolContext("edit"))).toBe(laneRun.sentinel);
	expect(await laneRun.agent.beforeModelCall?.(planMessages())).toBeUndefined();
	expect(laneRun.commitCalls()).toBe(1);
});

test("a missing executor warns, commits nothing, and leaves edits unblocked", async () => {
	const laneRun = harness({ resolve: async () => undefined });
	await laneRun.agent.beforeModelCall?.([]);
	for (const listener of laneRun.listeners) listener(todoWriteEnd());
	expect(await laneRun.agent.beforeModelCall?.(planMessages())).toBeUndefined();
	expect(laneRun.warns).toEqual([
		"lane_model_switch executor assignment is missing; keeping the current model",
	]);
	expect(laneRun.commitCalls()).toBe(0);
	expect(await laneRun.agent.beforeToolCall?.(toolContext("edit"))).toBe(laneRun.sentinel);
});

test("a disabled lane installs nothing", () => {
	const laneRun = harness({ enabled: false });
	expect(laneRun.listeners).toHaveLength(0);
	expect(laneRun.agent.beforeModelCall).toBeUndefined();
	expect(laneRun.agent.beforeToolCall).toBe(laneRun.previousHook);
	expect(laneRun.followUps).toHaveLength(0);
	expect(laneRun.warns).toHaveLength(0);
});

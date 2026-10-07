import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, WORK_TASK_CONTEXT_MAX_BYTES } from "@gajae-gateway/protocol";
import { buildWorkTaskContext } from "../src/server/work-task-context";
import { GatewayDatabase, type WorkTaskSourceInput } from "../src/store/db";

const taskId = "bd2f2494-2584-4d13-b7b6-c6ac24a1087f";
const thread = { platform: "discord" as const, kind: "thread" as const,
	conversationId: "1556589606403842128", parentId: "1511673764574793798", boundaryId: "1510336487894286436" };
const time = "2026-10-06T00:00:00.000Z";
const handles: Array<{ close(): void }> = [];
const directories: string[] = [];
afterEach(async () => {
	for (const handle of handles.splice(0)) handle.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});
async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "task-context-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const db = await GatewayDatabase.open(path);
	handles.push(db);
	db.assertBrokerAuthority({ canonicalAgentDir: join(directory, "agent"), identity: `gjc:${join(directory, "agent")}` }, { initializeEmpty: true });
	const create = (id = taskId) => db.workTaskCreate({ taskId: id, opRef: `original-${id}`,
		request: { text: "Inspect the immutable original assignment.", kind: "read_only", cwd: directory,
			coordinator: LOOPBACK_ORIGIN, surface: { thread }, evidence: { principalId: "local-owner",
				origin: LOOPBACK_ORIGIN, eventId: `assignment:${id}`, editId: null, evidenceAt: time, observedAt: time } } }).record;
	const source = (id: string, body: string, target = taskId): WorkTaskSourceInput => ({
		sourceId: id, taskId: target, kind: "decision", body,
		evidence: { principalId: "owner", origin: thread, eventId: "1556589606403842199",
			editId: null, evidenceAt: time, observedAt: time },
		supersedes: null, completeness: "complete", controlId: null, reportId: null,
	});
	return { db, path, create, source };
}

test("uncached context retains immutable brief, current facts, exact source links and stable freshness", async () => {
	const f = await fixture(); f.create();
	f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source("decision-one", "Investigate only; no edit authorization.")));
	const first = buildWorkTaskContext(f.db, { taskId }, () => new Date(time));
	expect(first.items.some((item) => item.text.includes("immutable original assignment"))).toBe(true);
	expect(first.items.some((item) => item.text.includes("https://discord.com/channels/1510336487894286436/1556589606403842128/1556589606403842199"))).toBe(true);
	expect(first.items.find((item) => item.sourceId.startsWith("state-"))?.text).toContain("not task success");
	const repeat = buildWorkTaskContext(f.db, { taskId, continuation: first.continuation }, () => new Date("2026-10-06T00:01:00.000Z"));
	expect(repeat.snapshot).toBe("same");
	expect(repeat.snapshotId).toBe(first.snapshotId);
	expect(repeat.renderedAt).not.toBe(first.renderedAt);
	f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source("decision-two", "Changed explicit decision.")));
	expect(buildWorkTaskContext(f.db, { taskId, continuation: first.continuation }).snapshot).toBe("new");
	expect(f.db.workTaskSourceGet("decision-one")?.body).toBe("Investigate only; no edit authorization.");
});

test("newest decisions beyond fifty records remain visible without fabricating an archive", async () => {
	const f = await fixture(); f.create();
	for (let i = 0; i < 55; i++) f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source(`decision-${i}`, `Decision ${i}.`)));
	const result = buildWorkTaskContext(f.db, { taskId });
	expect(result.items.some((item) => item.sourceId === "decision-54")).toBe(true);
	expect(result.items.some((item) => item.sourceId === `assignment-${taskId}`)).toBe(true);
	expect(result.manifest.length).toBeLessThanOrEqual(50);
	expect(result.completeness).toBe("partial");
	expect(result.omission.records).toBeGreaterThan(0);
	expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
});

test("bounded checkpoint and exception selection retains evidence time, qualification and honest omissions", async () => {
	const f = await fixture(); f.create();
	for (let index = 0; index < 55; index++) {
		const source = { ...f.source(`execution-fixture-${index}`,
			`Original checkpoint/error observation ${index}; execution coverage incomplete, not task success.`),
			kind: "observation" as const, completeness: "incomplete" as const };
		f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(source));
	}
	const later = "2026-10-06T01:00:00.000Z";
	const selected = buildWorkTaskContext(f.db, { taskId }, () => new Date(later));
	const entry = selected.manifest.find((source) => source.sourceId === "execution-fixture-54")!;
	expect(entry).toMatchObject({ evidenceAt: time, observedAt: time, completeness: "partial" });
	expect(selected.items.find((source) => source.sourceId === entry.sourceId)?.text)
		.toContain("Selection/render time is not new evidence");
	expect(selected.renderedAt).toBe(later);
	expect(selected.completeness).toBe("partial");
	expect(selected.omission.records).toBeGreaterThan(0);
	expect(selected.manifest.length).toBeLessThanOrEqual(50);
	expect(Buffer.byteLength(JSON.stringify(selected))).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
	const repeat = buildWorkTaskContext(f.db, { taskId, continuation: selected.continuation });
	expect(repeat.snapshot).toBe("same");
	expect(f.db.deliveryRows()).toHaveLength(0);
});

test("escaped Unicode and many large records obey the entire serialized packet byte bound", async () => {
	const f = await fixture(); f.create();
	for (let i = 0; i < 8; i++) f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source(`large-${i}`, '"\\😀'.repeat(800))));
	const result = buildWorkTaskContext(f.db, { taskId });
	expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
	expect(result.completeness).toBe("partial");
	expect(result.omission.overflow).toBe(true);
	expect(result.items.some((item) => item.text.includes("�"))).toBe(false);
});

test("twenty-first held task outranks healthy tombstones and corruption does not hide siblings", async () => {
	const f = await fixture();
	const ids = Array.from({ length: 21 }, (_, i) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`);
	for (const id of ids) f.create(id);
	const held = f.db.workTaskGet(ids[20]!)!;
	f.db.withTransaction(() => f.db.workTaskSurfaceInTransaction(held.taskId, held.version, { phase: "held", reason: "operator needed", at: time }));
	const raw = new Database(f.path); handles.push(raw);
	raw.query("UPDATE work_tasks SET record_json = '{}' WHERE task_id = ?").run(ids[0]!);
	const result = buildWorkTaskContext(f.db, {});
	expect(result.items[0]?.taskId).toBe(ids[20]);
	expect(result.items[0]?.text).toContain("operator needed");
	expect(result.manifest.some((entry) => entry.taskId === ids[0] && entry.completeness === "unavailable")).toBe(true);
	expect(result.manifest.some((entry) => entry.taskId === ids[1] && entry.completeness === "complete")).toBe(true);
	expect(result.omission.tasks).toBe(1);
	expect(result.omission.records).toBeNull();
	expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
});

test("topic changes snapshot and does not turn retrieval into instructions or mutations", async () => {
	const f = await fixture(); f.create();
	f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source("decision", "Investigate retrieval before editing.")));
	const before = f.db.workTaskGet(taskId);
	const first = buildWorkTaskContext(f.db, { taskId, topic: "retrieval" });
	const different = buildWorkTaskContext(f.db, { taskId, topic: "missing-topic", continuation: first.continuation });
	expect(different.snapshot).toBe("new");
	expect(different.items.some((item) => item.sourceId === "decision")).toBe(false);
	expect(different.items.some((item) => item.sourceId === `assignment-${taskId}`)).toBe(true);
	expect(f.db.workTaskGet(taskId)).toEqual(before);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
});

test("unknown explicit task and malformed continuation request fail without creating work", async () => {
	const f = await fixture();
	expect(() => buildWorkTaskContext(f.db, { taskId })).toThrow("unknown task");
	expect(() => buildWorkTaskContext(f.db, { continuation: "" })).toThrow();
	expect(f.db.workTaskKeys().keys).toHaveLength(0);
});

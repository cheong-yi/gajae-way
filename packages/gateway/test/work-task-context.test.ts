import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, PROFILE_VERSION, WORK_TASK_CONTEXT_MAX_BYTES } from "@gajae-gateway/protocol";
import { buildWorkTaskContext, buildWorkTaskSourceRead } from "../src/server/work-task-context";
import { GatewayDatabase, type WorkTaskSourceInput } from "../src/store/db";

const taskId = "bd2f2494-2584-4d13-b7b6-c6ac24a1087f";
const thread = {
	platform: "discord" as const,
	kind: "thread" as const,
	conversationId: "1556589606403842128",
	parentId: "1511673764574793798",
	boundaryId: "1510336487894286436",
};
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
	db.assertBrokerAuthority(
		{ canonicalAgentDir: join(directory, "agent"), identity: `gjc:${join(directory, "agent")}` },
		{ initializeEmpty: true },
	);
	const create = (id = taskId) =>
		db.workTaskCreate({
			taskId: id,
			opRef: `original-${id}`,
			request: {
				text: "Inspect the immutable original assignment.",
				kind: "read_only",
				cwd: directory,
				coordinator: LOOPBACK_ORIGIN,
				surface: { thread },
				evidence: {
					principalId: "local-owner",
					origin: LOOPBACK_ORIGIN,
					eventId: `assignment:${id}`,
					editId: null,
					evidenceAt: time,
					observedAt: time,
				},
			},
		}).record;
	const source = (id: string, body: string, target = taskId): WorkTaskSourceInput => ({
		sourceId: id,
		taskId: target,
		kind: "decision",
		body,
		evidence: {
			principalId: "owner",
			origin: thread,
			eventId: "1556589606403842199",
			editId: null,
			evidenceAt: time,
			observedAt: time,
		},
		supersedes: null,
		completeness: "complete",
		controlId: null,
		reportId: null,
	});
	return { db, path, create, source };
}

test("source read reassembles legal 16KiB originals within the real escaped UTF-8 response envelope", async () => {
	const f = await fixture();
	f.create();
	for (const unit of ["a", '"\\\n\t', "é", "한", "🦞"]) {
		const repeated = unit.repeat(Math.floor(16384 / Buffer.byteLength(unit)));
		const body = repeated + "x".repeat(16384 - Buffer.byteLength(repeated));
		const candidate = f.source(`original-${Buffer.from(unit).toString("hex")}`.padEnd(512, "x"), body);
		const source = f.db.withTransaction(() =>
			f.db.workTaskSourceAppendInTransaction({
				...candidate,
				evidence: {
					...candidate.evidence,
					principalId: '"'.repeat(512),
					eventId: '"'.repeat(1024),
					editId: '"'.repeat(512),
				},
			}),
		);
		const input = { mode: "source" as const, taskId, sourceId: source.sourceId, contentHash: source.contentHash };
		let cursor: string | undefined;
		let rebuilt = "";
		let offset = 0;
		const requestId = '"'.repeat(1024);
		do {
			const page = buildWorkTaskSourceRead(f.db, { ...input, ...(cursor ? { cursor } : {}) }, requestId);
			expect(
				Buffer.byteLength(JSON.stringify({ v: PROFILE_VERSION, type: "response", id: requestId, result: page }) + "\n"),
			).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
			expect(page.startByte).toBe(offset);
			expect(page.totalBytes).toBe(16384);
			expect(page.completeness).toBe("complete");
			expect(page.evidence).toEqual(source.evidence);
			expect(page.body).not.toContain("\ufffd");
			expect(page.endByte - page.startByte).toBe(Buffer.byteLength(page.body));
			expect(page.endByte).toBeGreaterThan(page.startByte);
			rebuilt += page.body;
			offset = page.endByte;
			cursor = page.nextCursor;
			expect(page.eof).toBe(cursor === undefined);
		} while (cursor);
		expect(rebuilt).toBe(body);
		expect(offset).toBe(16384);
	}
	expect(f.db.deliveryRows()).toHaveLength(0);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
});

test("source cursor pins task/source/hash and UTF-8 boundary across database restart", async () => {
	const f = await fixture();
	f.create();
	const source = f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction(f.source("original-long", "한".repeat(5461) + "x")),
	);
	const input = { mode: "source" as const, taskId, sourceId: source.sourceId, contentHash: source.contentHash };
	const first = buildWorkTaskSourceRead(f.db, input);
	expect(first.eof).toBe(false);
	const reopened = await GatewayDatabase.open(f.path);
	handles.push(reopened);
	const second = buildWorkTaskSourceRead(reopened, { ...input, cursor: first.nextCursor });
	expect(first.body + second.body).toBe(source.body);
	expect(second.eof).toBe(true);
	expect(() => buildWorkTaskSourceRead(reopened, { ...input, contentHash: "0".repeat(64) })).toThrow();
	expect(() =>
		buildWorkTaskSourceRead(reopened, { ...input, taskId: "bd2f2494-2584-4d13-b7b6-c6ac24a10870" }),
	).toThrow();
	const other = reopened.withTransaction(() =>
		reopened.workTaskSourceAppendInTransaction(f.source("other-original", "Different retained evidence")),
	);
	expect(() =>
		buildWorkTaskSourceRead(reopened, {
			...input,
			sourceId: other.sourceId,
			contentHash: other.contentHash,
			cursor: first.nextCursor,
		}),
	).toThrow();
	expect(() => buildWorkTaskSourceRead(reopened, { ...input, cursor: "not-a-cursor" })).toThrow();
	const splitCursor = Buffer.from(JSON.stringify([taskId, source.sourceId, source.contentHash, 1])).toString(
		"base64url",
	);
	expect(() => buildWorkTaskSourceRead(reopened, { ...input, cursor: splitCursor })).toThrow();
	expect(() => buildWorkTaskSourceRead(reopened, input, "x".repeat(16384))).toThrow("envelope");
});

test("uncached context retains immutable brief, current facts, exact source links and stable freshness", async () => {
	const f = await fixture();
	f.create();
	f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction(f.source("decision-one", "Investigate only; no edit authorization.")),
	);
	const first = buildWorkTaskContext(f.db, { taskId }, () => new Date(time));
	expect(first.items.some((item) => item.text.includes("immutable original assignment"))).toBe(true);
	expect(
		first.items.some((item) =>
			item.text.includes("https://discord.com/channels/1510336487894286436/1556589606403842128/1556589606403842199"),
		),
	).toBe(true);
	expect(first.items.find((item) => item.sourceId.startsWith("state-"))?.text).toContain("not task success");
	const repeat = buildWorkTaskContext(
		f.db,
		{ taskId, continuation: first.continuation },
		() => new Date("2026-10-06T00:01:00.000Z"),
	);
	expect(repeat.snapshot).toBe("same");
	expect(repeat.snapshotId).toBe(first.snapshotId);
	expect(repeat.renderedAt).not.toBe(first.renderedAt);
	f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction(f.source("decision-two", "Changed explicit decision.")),
	);
	expect(buildWorkTaskContext(f.db, { taskId, continuation: first.continuation }).snapshot).toBe("new");
	expect(f.db.workTaskSourceGet("decision-one")?.body).toBe("Investigate only; no edit authorization.");
});

test("selected immutable replacements identify their predecessor without changing source freshness or authority", async () => {
	const f = await fixture();
	f.create();
	const original = f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction(f.source("decision-old", "Select red.")),
	);
	const replacement = f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction({
			...f.source("decision-new", "Select blue instead."),
			supersedes: original.sourceId,
		}),
	);
	const task = f.db.workTaskGet(taskId);
	const result = buildWorkTaskContext(f.db, { taskId }, () => new Date("2026-10-07T00:00:00.000Z"));
	expect(result.items.some((item) => item.sourceId === original.sourceId)).toBe(true);
	expect(result.items.find((item) => item.sourceId === replacement.sourceId)?.text).toStartWith(
		"Supersedes source decision-old; predecessor selected in this view.\n",
	);
	expect(result.manifest.find((entry) => entry.sourceId === replacement.sourceId)).toMatchObject({
		revision: replacement.contentHash,
		evidenceAt: time,
		observedAt: time,
	});
	expect(f.db.workTaskSourceGet(original.sourceId)).toEqual(original);
	expect(f.db.workTaskSourceGet(replacement.sourceId)).toEqual(replacement);
	expect(f.db.workTaskGet(taskId)).toEqual(task);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
});

for (const omission of ["topic", "record-bound"] as const)
	test(`replacement identifies its omitted predecessor after ${omission} selection`, async () => {
		const f = await fixture();
		f.create();
		const original = f.db.withTransaction(() =>
			f.db.workTaskSourceAppendInTransaction(f.source("decision-old", "Select red.")),
		);
		if (omission === "record-bound")
			for (let index = 0; index < 52; index++)
				f.db.withTransaction(() =>
					f.db.workTaskSourceAppendInTransaction(f.source(`discussion-${index}`, `Discussion ${index}.`)),
				);
		const replacement = f.db.withTransaction(() =>
			f.db.workTaskSourceAppendInTransaction({
				...f.source("decision-new", "Select blue instead."),
				supersedes: original.sourceId,
			}),
		);
		const result = buildWorkTaskContext(f.db, { taskId, ...(omission === "topic" ? { topic: "blue" } : {}) });
		expect(result.items.some((item) => item.sourceId === original.sourceId)).toBe(false);
		expect(result.manifest.some((entry) => entry.sourceId === original.sourceId)).toBe(false);
		expect(result.items.find((item) => item.sourceId === replacement.sourceId)?.text).toStartWith(
			"Supersedes source decision-old; predecessor omitted from this view.\n",
		);
		expect(f.db.workTaskSourceGet(original.sourceId)).toEqual(original);
		expect(result.manifest.find((entry) => entry.sourceId === replacement.sourceId)?.revision).toBe(
			replacement.contentHash,
		);
		expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
		expect(f.db.workControlList(taskId)).toHaveLength(0);
	});

test("replacement provenance survives byte pressure and qualifies only actually selected predecessors", async () => {
	const f = await fixture();
	f.create();
	for (let index = 0; index < 24; index++)
		f.db.withTransaction(() =>
			f.db.workTaskSourceAppendInTransaction({
				...f.source(`decision-${index}`, `Decision ${index}: ${'"\\😀'.repeat(150)}`),
				supersedes: index === 0 ? null : `decision-${index - 1}`,
			}),
		);
	const result = buildWorkTaskContext(f.db, { taskId });
	const selected = new Set(result.items.map((item) => item.sourceId));
	let selectedPredecessors = 0;
	let omittedPredecessors = 0;
	for (const item of result.items) {
		const source = f.db.workTaskSourceGet(item.sourceId);
		if (!source?.supersedes) continue;
		const included = selected.has(source.supersedes);
		if (included) selectedPredecessors++;
		else omittedPredecessors++;
		expect(item.text).toStartWith(
			`Supersedes source ${source.supersedes}; predecessor ${included ? "selected in" : "omitted from"} this view.\n`,
		);
		expect(item.revision).toBe(source.contentHash);
	}
	expect(selectedPredecessors).toBeGreaterThan(0);
	expect(omittedPredecessors).toBeGreaterThan(0);
	expect(result.completeness).toBe("partial");
	expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
	expect(result.items.some((item) => item.text.includes("�"))).toBe(false);
	expect(f.db.workControlList(taskId)).toHaveLength(0);
	expect(f.db.deliveryRows()).toHaveLength(0);
});

test("newest decisions beyond fifty records remain visible without fabricating an archive", async () => {
	const f = await fixture();
	f.create();
	for (let i = 0; i < 55; i++)
		f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source(`decision-${i}`, `Decision ${i}.`)));
	const result = buildWorkTaskContext(f.db, { taskId });
	expect(result.items.some((item) => item.sourceId === "decision-54")).toBe(true);
	expect(result.items.some((item) => item.sourceId === `assignment-${taskId}`)).toBe(true);
	expect(result.manifest.length).toBeLessThanOrEqual(50);
	expect(result.completeness).toBe("partial");
	expect(result.omission.records).toBeGreaterThan(0);
	expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(WORK_TASK_CONTEXT_MAX_BYTES);
});

test("bounded checkpoint and exception selection retains evidence time, qualification and honest omissions", async () => {
	const f = await fixture();
	f.create();
	for (let index = 0; index < 55; index++) {
		const source = {
			...f.source(
				`execution-fixture-${index}`,
				`Original checkpoint/error observation ${index}; execution coverage incomplete, not task success.`,
			),
			kind: "observation" as const,
			completeness: "incomplete" as const,
		};
		f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(source));
	}
	const later = "2026-10-06T01:00:00.000Z";
	const selected = buildWorkTaskContext(f.db, { taskId }, () => new Date(later));
	const entry = selected.manifest.find((source) => source.sourceId === "execution-fixture-54")!;
	expect(entry).toMatchObject({ evidenceAt: time, observedAt: time, completeness: "partial" });
	expect(selected.items.find((source) => source.sourceId === entry.sourceId)?.text).toContain(
		"Selection/render time is not new evidence",
	);
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
	const f = await fixture();
	f.create();
	for (let i = 0; i < 8; i++)
		f.db.withTransaction(() => f.db.workTaskSourceAppendInTransaction(f.source(`large-${i}`, '"\\😀'.repeat(800))));
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
	f.db.withTransaction(() =>
		f.db.workTaskSurfaceInTransaction(held.taskId, held.version, {
			phase: "held",
			reason: "operator needed",
			at: time,
		}),
	);
	const raw = new Database(f.path);
	handles.push(raw);
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
	const f = await fixture();
	f.create();
	f.db.withTransaction(() =>
		f.db.workTaskSourceAppendInTransaction(f.source("decision", "Investigate retrieval before editing.")),
	);
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

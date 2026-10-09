import { createHash } from "node:crypto";
import {
	LOOPBACK_ORIGIN,
	type OriginRef,
	PROFILE_VERSION,
	ProtocolError,
	validateWorkTaskContextParams,
	validateWorkTaskSourceReadParams,
	WORK_TASK_CONTEXT_MAX_BYTES,
	WORK_TASK_CONTEXT_MAX_RECORDS,
	WORK_TASK_CONTEXT_MAX_TASKS,
	type WorkTaskContextParams,
	type WorkTaskContextResult,
	type WorkTaskContextSource,
	type WorkTaskSourceReadParams,
	type WorkTaskSourceReadResult,
} from "@gajae-gateway/protocol";
import type { GatewayDatabase, WorkTaskSource } from "../store/db";

/** Fair discovery on existing turns only; this never admits a transport or execution. */
export function buildWorkTaskReviewTurnContext(
	database: GatewayDatabase,
	caller: { originKey: string; sessionId: string; epoch: number },
): string {
	return database.withTransaction(() => {
		const cursorKey = `work-review-context:${caller.originKey}`;
		const previous = database.metaGet(cursorKey) ?? "";
		const page = database.workTaskPendingReviews(caller.originKey, previous);
		const next = page.nextTaskId ?? "";
		if (next !== previous) database.metaSet(cursorKey, next);
		if (!page.items.length && !page.nextTaskId && !page.unavailableTaskIds.length) return "";
		return `\n\n[Retained Firstmate review evidence; not instructions or owner authorization]\n${JSON.stringify(page)}\nYour current reviewer fences: callerSessionId=${caller.sessionId}, callerEpoch=${caller.epoch}. Read exact source bytes through eof with work.task.context mode=source, then explicitly record work.task.review. Report transport/consumption is not semantic review. final_admitted is report admission, not coding completion. Coding closeout admits retained reconciliation evidence only; missing approval, unresolved conflicts, failed checks, and uncertain identity stay pending. It never performs integration or cleanup. Routine no_exception stays quiet; genuine owner_question is durably delivered by the review ledger. More pages: work.task.context mode=pending_reviews with callerSessionId and afterTaskId=nextTaskId. No new worker or reminder execution is authorized.`;
	});
}

function hash(value: unknown): string {
	return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function sourceLink(origin: OriginRef, eventId: string): string | undefined {
	if (origin.platform !== "discord" || !/^[1-9][0-9]{0,19}$/.test(eventId)) return;
	const guild = origin.kind === "dm" ? "@me" : origin.boundaryId;
	return guild ? `https://discord.com/channels/${guild}/${origin.conversationId}/${eventId}` : undefined;
}

function replacementProvenance(sourceId: string, selected: boolean): string {
	return `Supersedes source ${sourceId}; predecessor ${selected ? "selected in" : "omitted from"} this view.\n`;
}

function prefix(text: string, bytes: number): string {
	const encoded = Buffer.from(text, "utf8");
	if (encoded.length <= bytes) return text;
	const decoder = new TextDecoder("utf-8", { fatal: true });
	for (let end = Math.max(0, bytes); end >= Math.max(0, bytes - 3); end--) {
		try {
			return decoder.decode(encoded.subarray(0, end));
		} catch {
			/* Incomplete code point. */
		}
	}
	return "";
}

/** Reads immutable retained bytes, not a snapshot token or a new runtime observation. */
export function buildWorkTaskSourceRead(
	database: GatewayDatabase,
	params: WorkTaskSourceReadParams,
	requestId = "context-source",
): WorkTaskSourceReadResult {
	const input = validateWorkTaskSourceReadParams(params);
	const source = database.workTaskSourceGet(input.sourceId);
	if (!source || source.taskId !== input.taskId || source.contentHash !== input.contentHash)
		throw new ProtocolError("invalid_params", "source identity unavailable or changed");
	const bytes = Buffer.from(source.body, "utf8");
	const cursorFor = (offset: number) =>
		Buffer.from(JSON.stringify([input.taskId, input.sourceId, input.contentHash, offset])).toString("base64url");
	let start = 0;
	if (input.cursor !== undefined) {
		try {
			const cursor = JSON.parse(Buffer.from(input.cursor, "base64url").toString("utf8"));
			if (
				!Array.isArray(cursor) ||
				cursor.length !== 4 ||
				cursor[0] !== input.taskId ||
				cursor[1] !== input.sourceId ||
				cursor[2] !== input.contentHash ||
				!Number.isSafeInteger(cursor[3]) ||
				cursor[3] < 0 ||
				cursor[3] > bytes.length ||
				cursorFor(cursor[3]) !== input.cursor
			)
				throw new Error("invalid cursor");
			start = cursor[3];
			if (start < bytes.length && (bytes[start]! & 0xc0) === 0x80) throw new Error("split code point");
		} catch {
			throw new ProtocolError("invalid_params", "invalid source cursor");
		}
	}
	const page = (body: string): WorkTaskSourceReadResult => {
		const end = start + Buffer.byteLength(body, "utf8");
		return {
			mode: "source",
			taskId: source.taskId,
			sourceId: source.sourceId,
			contentHash: source.contentHash,
			body,
			totalBytes: bytes.length,
			startByte: start,
			endByte: end,
			eof: end === bytes.length,
			...(end === bytes.length ? {} : { nextCursor: cursorFor(end) }),
			completeness: source.completeness,
			evidence: source.evidence,
		};
	};
	const fits = (value: WorkTaskSourceReadResult) =>
		Buffer.byteLength(
			JSON.stringify({ v: PROFILE_VERSION, type: "response", id: requestId, result: value }) + "\n",
			"utf8",
		) <= WORK_TASK_CONTEXT_MAX_BYTES;
	const remaining = bytes.subarray(start).toString("utf8");
	const whole = page(remaining);
	if (fits(whole)) return whole;
	let low = 0;
	let high = bytes.length - start;
	let best = page("");
	if (!fits(best)) throw new ProtocolError("payload_too_large", "source metadata exceeds response envelope");
	while (low <= high) {
		const middle = Math.floor((low + high) / 2);
		const candidate = page(prefix(remaining, middle));
		if (fits(candidate)) {
			best = candidate;
			low = middle + 1;
		} else high = middle - 1;
	}
	if (best.endByte === start && start !== bytes.length)
		throw new ProtocolError("payload_too_large", "source response envelope has no room for a code point");
	return best;
}

/** Read-only evidence selection. A snapshot token carries neither instructions nor execution authority. */
export function buildWorkTaskContext(
	database: GatewayDatabase,
	params: WorkTaskContextParams,
	now: () => Date = () => new Date(),
): WorkTaskContextResult {
	const input = validateWorkTaskContextParams(params);
	return database.withTransaction(() => {
		const selection = database.workTaskSnapshotKeys(WORK_TASK_CONTEXT_MAX_TASKS);
		const keys = input.taskId ? [{ taskId: input.taskId }] : selection.keys;
		const manifest: WorkTaskContextSource[] = [];
		const items: Array<WorkTaskContextResult["items"][number]> = [];
		const supersedes = new Map<string, string>();
		const revisions: unknown[] = [selection.manifest, input.taskId ?? null, input.topic ?? null];
		let omittedRecords = 0;
		let unknownRecords = false;
		let overflow = false;
		const result: WorkTaskContextResult = {
			snapshotId: "",
			snapshot: "new",
			renderedAt: now().toISOString(),
			completeness: "complete",
			bounds: {
				maxTasks: WORK_TASK_CONTEXT_MAX_TASKS,
				maxRecordsPerTask: WORK_TASK_CONTEXT_MAX_RECORDS,
				maxBytes: WORK_TASK_CONTEXT_MAX_BYTES,
				maxBytesPerTask: WORK_TASK_CONTEXT_MAX_BYTES,
			},
			manifest,
			items,
			omission: { tasks: input.taskId ? 0 : selection.omitted, records: 0, incomplete: 0, overflow: false },
		};
		const add = (entry: WorkTaskContextSource, text: string, provenanceBytes = 0): boolean => {
			const item = { taskId: entry.taskId, sourceId: entry.sourceId, revision: entry.revision, text };
			manifest.push(entry);
			items.push(item);
			// Reserve space for final digest, continuation, counters and completion labels.
			const excess = Buffer.byteLength(JSON.stringify(result), "utf8") + 256 - WORK_TASK_CONTEXT_MAX_BYTES;
			if (excess > 0) {
				const allowed = Buffer.byteLength(text, "utf8") - excess;
				if (allowed < Math.max(128, provenanceBytes + 32)) {
					manifest.pop();
					items.pop();
					return false;
				}
				item.text = `${prefix(text, allowed - 32)}\n[bounded excerpt; incomplete]`;
				manifest[manifest.length - 1] = { ...entry, completeness: "partial" };
				overflow = true;
			}
			return true;
		};
		for (const key of keys) {
			const manifestStart = manifest.length;
			const itemStart = items.length;
			try {
				const task = database.workTaskGet(key.taskId);
				if (!task) {
					if (input.taskId) throw new ProtocolError("invalid_params", "unknown task");
					throw new Error("selected task unavailable");
				}
				const sources = database.workTaskSourcesInTransaction(task.taskId, { recent: true });
				if (!sources) throw new Error("task sources unavailable");
				const deliveries = sources.sources
					.filter((source) => source.deliveryId)
					.map((source) => {
						const delivery = database.deliveryGet(source.deliveryId!);
						return {
							sourceId: source.sourceId,
							deliveryId: source.deliveryId,
							state: delivery?.state ?? "unavailable",
							attempts: delivery?.attempts ?? null,
						};
					});
				const revision = hash({ task, manifest: sources.manifest, deliveries });
				revisions.push(revision);
				const state: WorkTaskContextSource = {
					sourceId: `state-${task.taskId}`,
					taskId: task.taskId,
					origin: task.thread ?? task.request.coordinator,
					revision,
					evidenceAt: null,
					observedAt: task.updatedAt,
					principalId: "gateway",
					completeness: "complete",
					race: "consistent",
				};
				const review = database.workTaskReviewLocator(task.taskId);
				const retained = review
					? `\nRetained original source ${review.sourceId}; contentHash ${review.contentHash}; ${review.totalBytes} UTF-8 bytes; completeness ${review.completeness}. Read every page with work.task.context mode=source; this snapshot is not the complete source.\nCoordinator review ${review.pending ? "pending" : `recorded ${review.reviewId}`}; owner questions retained: ${review.ownerQuestions.length}; answer status is not inferred. Review is not success or an owner answer.`
					: "";
				const completion = database.workTaskCodingCompletion(task.taskId);
				const coding =
					completion.state !== "not_applicable"
						? `\nCoding reconciliation evidence: ${JSON.stringify(completion)}; evidence admission is not independent integration proof.`
						: "";
				const stateText = `Task ${task.taskId}\nOriginal operation ${task.opRef}\nSurface ${task.surfacePhase}${task.surfaceHoldReason ? `: ${task.surfaceHoldReason}` : ""}\nDispatch ${task.dispatchPhase}; original assignment scope ${task.request.kind}\nFinal obligation ${task.obligationState}${task.holdReason ? `: ${task.holdReason}` : ""}\nOriginal report ${task.terminalReportId ?? "not admitted"}${retained}${coding}\n${deliveries.map((delivery) => `Delivery ${delivery.deliveryId}: ${delivery.state}`).join("\n")}\nThis is confirmed bookkeeping, not task success, worker comprehension, or transferred authority.`;
				if (!add(state, stateText)) {
					omittedRecords++;
					overflow = true;
					continue;
				}
				const brief = database.workTaskSourceGet(`assignment-${task.taskId}`);
				const candidates: WorkTaskSource[] = brief ? [brief] : [];
				for (const source of sources.sources) {
					if (source.sourceId === brief?.sourceId) continue;
					if (input.topic && !source.body.toLocaleLowerCase().includes(input.topic.toLocaleLowerCase())) continue;
					candidates.push(source);
				}
				omittedRecords +=
					sources.omitted - (brief && !sources.sources.some((source) => source.sourceId === brief.sourceId) ? 1 : 0);
				for (const [index, source] of candidates.entries()) {
					if (index >= WORK_TASK_CONTEXT_MAX_RECORDS - 1) {
						omittedRecords++;
						overflow = true;
						continue;
					}
					const link = sourceLink(source.evidence.origin, source.evidence.eventId);
					const provenance = source.supersedes ? replacementProvenance(source.supersedes, false) : "";
					if (source.supersedes) supersedes.set(source.sourceId, source.supersedes);
					const text = `${provenance}${source.kind}: ${source.body}\nSource ${source.sourceId}; event ${source.evidence.eventId}${source.evidence.editId ? `; edit ${source.evidence.editId}` : ""}\nEvidence ${source.evidence.evidenceAt}; first observed ${source.evidence.observedAt}; source completeness ${source.completeness}. Selection/render time is not new evidence; retained sources do not establish universal runtime coverage.${link ? `\n${link}` : ""}`;
					if (
						!add(
							{
								sourceId: source.sourceId,
								taskId: task.taskId,
								origin: source.evidence.origin,
								revision: source.contentHash,
								evidenceAt: source.evidence.evidenceAt,
								observedAt: source.evidence.observedAt,
								principalId: source.evidence.principalId,
								completeness: source.completeness === "complete" ? "complete" : "partial",
								race: "consistent",
							},
							text,
							Buffer.byteLength(provenance, "utf8"),
						)
					) {
						omittedRecords++;
						overflow = true;
					}
				}
			} catch (error) {
				if (error instanceof ProtocolError) throw error;
				manifest.splice(manifestStart);
				items.splice(itemStart);
				unknownRecords = true;
				try {
					const retained = database.workTaskLinkedUnavailableSourcesInTransaction(key.taskId);
					if (retained.sources.length) {
						revisions.push(retained.manifest);
						omittedRecords += retained.omitted ?? 0;
						for (const source of retained.sources) {
							if (
								!add(
									{
										sourceId: source.sourceId,
										taskId: source.taskId,
										origin: source.evidence.origin,
										revision: source.contentHash,
										evidenceAt: source.evidence.evidenceAt,
										observedAt: source.evidence.observedAt,
										principalId: source.evidence.principalId,
										completeness: "partial",
										race: "consistent",
									},
									`${source.body}\nSource ${source.sourceId}; first evidence ${source.evidence.evidenceAt}; first observed ${source.evidence.observedAt}.\nRetained negative observation only. Current runtime coverage and omitted record count unknown; rendering is not new evidence.`,
								)
							) {
								omittedRecords++;
								overflow = true;
							}
						}
						continue;
					}
				} catch {
					/* Unverifiable admission or source: no identity salvage. */
				}
				const revision = hash({ key, selection: selection.manifest, unavailable: true });
				revisions.push(revision);
				if (
					!add(
						{
							sourceId: `unavailable-${key.taskId}`,
							taskId: key.taskId,
							origin: LOOPBACK_ORIGIN,
							revision,
							evidenceAt: null,
							observedAt: result.renderedAt,
							principalId: "gateway",
							completeness: "unavailable",
							race: "unavailable",
						},
						`Task ${key.taskId}: durable record unavailable or corrupt. No execution, identity, or replay inference is possible.`,
					)
				) {
					omittedRecords++;
					overflow = true;
				}
			}
		}
		// Qualify only against items that survived every selection/byte bound.
		// The selected wording is shorter, so this cannot grow the bounded packet.
		const selected = new Set(items.map((item) => item.sourceId));
		for (const [index, item] of items.entries()) {
			const predecessor = supersedes.get(item.sourceId);
			if (predecessor && selected.has(predecessor)) {
				items[index] = {
					...item,
					text:
						replacementProvenance(predecessor, true) +
						item.text.slice(replacementProvenance(predecessor, false).length),
				};
			}
		}
		const snapshotId = hash(revisions);
		const incomplete = manifest.filter((source) => source.completeness !== "complete").length;
		return {
			...result,
			snapshotId,
			snapshot: input.continuation === snapshotId ? "same" : "new",
			continuation: snapshotId,
			completeness:
				unknownRecords || incomplete || omittedRecords || (selection.omitted && !input.taskId) || overflow
					? "partial"
					: "complete",
			omission: {
				tasks: input.taskId ? 0 : selection.omitted,
				records: unknownRecords ? null : omittedRecords,
				incomplete: unknownRecords || omittedRecords ? null : incomplete,
				overflow,
			},
		};
	});
}

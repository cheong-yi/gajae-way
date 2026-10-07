import { describe, expect, test } from "bun:test";
import {
	CHAT_PLATFORMS,
	containsSilenceToken,
	decodeFrame,
	describeChatPlatforms,
	encodeFrame,
	FrameDecoder,
	isChatPlatform,
	isSilenceToken,
	isSilentOutput,
	LOOPBACK_ORIGIN,
	MAX_FRAME_BYTES,
	monitorSessionOrigin,
	negotiate,
	originKey,
	PROFILE_VERSION,
	ProtocolError,
	parseOriginKey,
	validateOriginRef,
	validateWorkTaskSpec,
	validateWorkStartParams,
	validateWorkSteerParams,
	validateWorkStatusParams,
	validateWorkThreadClaimParams,
	validateWorkThreadBindParams,
	validateWorkTaskContextParams,
	validateWorkTaskRecoverParams,
	validateWorkTaskDispositionParams,
	validateWorkTaskDispositionBasis,
	validateWorkTaskDispositionBasisParams,
	validateWorkTaskDispositionBasisResult,
	validateWorkTaskDispositionRecord,
	validateWorkTaskDispositionResult,
	type WorkTaskDispositionParams,
	type WorkTaskDispositionBasis,
	type WorkTaskDispositionBasisResult,
	type WorkTaskDispositionNegative,
	type WorkTaskDispositionRecord,
	type WorkTaskDispositionResult,
	WORK_TASK_CONTEXT_MAX_BYTES,
	WORK_TASK_TEXT_MAX_BYTES,
	WORK_TASK_EVENT_ID_MAX_BYTES,
	type ChatEditResult,
	type ChatSendResult,
	type ChatMessagePayload,
	type OriginRef,
	type WorkTaskSpec,
	type WorkTaskControlProjection,
	type WorkThreadBindParams,
	type VerbCatalogV01,
	DELIVERY_CONFIRM_MAX_BYTES,
	DELIVERY_RECEIPT_MAX_MESSAGE_IDS,
	validateDeliveryConfirmParams,
	validateWorkJobsParams,
	validateWorkTaskMessageMetadata,
	validateWorkTaskOriginSource,
} from "../src/index";

describe("administrative work.task.disposition contract", () => {
	const input = {
		taskId: "bd2f2494-2584-4d13-b7b6-c6ac24a1087f", jobId: "work/fm-original",
		expectedOpRef: "gw-original", sessionId: null, epoch: null, cwd: "/work",
		requestHash: "a".repeat(64), target: { kind: "report", reportId: null },
		eventId: "local:disposition:1", expectedTaskVersion: 2, outcome: "unresolved",
		reason: "Original evidence is unavailable.",
		evidence: { availability: "unavailable", detail: "No remote receipt can be recovered.", evidenceAt: null },
	} satisfies WorkTaskDispositionParams;
	test("unknown original report/session stays explicitly null, without invented proof", () => {
		expect(validateWorkTaskDispositionParams(input)).toEqual(input);
		expect(validateWorkTaskDispositionParams({ ...input, callerSessionId: "routing-only" }).callerSessionId)
			.toBe("routing-only");
	});
	test("standalone basis strictly qualifies negative evidence without granting execution", () => {
		const qualification: WorkTaskDispositionNegative = { kind: "validation_unavailable", scope: "read_only", sourceId: `unavailability-${"b".repeat(64)}`,
			corruptionFingerprint: "b".repeat(64), firstObservedAt: "2026-10-06T00:00:00.000Z" };
		const basis: WorkTaskDispositionBasis = {
			taskId: input.taskId, jobId: input.jobId, expectedOpRef: input.expectedOpRef,
			sessionId: input.sessionId, epoch: input.epoch, cwd: input.cwd, requestHash: input.requestHash,
			expectedTaskVersion: input.expectedTaskVersion, controls: [], controlsCompleteness: "complete",
			report: input.target,
		};
		const result: WorkTaskDispositionBasisResult = { execution: "none", kind: "validation_unavailable", basis, qualification };
		expect(validateWorkTaskDispositionBasisParams({ taskId: input.taskId })).toEqual({ taskId: input.taskId });
		expect(() => validateWorkTaskDispositionBasisParams({ taskId: input.taskId, principalId: "owner" })).toThrow();
		expect(validateWorkTaskDispositionBasisResult(result)).toEqual(result);
		expect(() => validateWorkTaskDispositionBasisResult({ ...result, execution: "started" })).toThrow();
		expect(() => validateWorkTaskDispositionBasisResult({ ...result,
			qualification: { ...qualification, corruptionFingerprint: "c".repeat(64) } })).toThrow();
		expect(() => validateWorkTaskDispositionBasisResult({ ...result, kind: "original" })).toThrow();
		expect(validateWorkTaskDispositionBasisResult({ execution: "none", kind: "unavailable", basis: null }))
			.toEqual({ execution: "none", kind: "unavailable", basis: null });
		const record: WorkTaskDispositionRecord = {
			request: { ...input, validationUnavailable: qualification }, principalId: "local-owner",
			origin: LOOPBACK_ORIGIN, recordedAt: qualification.firstObservedAt, taskVersion: 3,
			retained: { obligationState: "awaiting_final", reportId: null, holdReason: null, controlPhase: null },
		};
		expect(validateWorkTaskDispositionRecord(record)).toEqual(record);
		expect(() => validateWorkTaskDispositionRecord({ ...record, request: input })).toThrow();
		expect(() => validateWorkTaskDispositionRecord({ ...record,
			retained: { ...record.retained, obligationState: "final_admitted" } })).toThrow();
		expect(() => validateWorkTaskDispositionParams({ ...input,
			validationUnavailable: { ...qualification, sdkValidated: true } })).toThrow();
	});
	test("status basis is strict snapshot data with bounded qualified targets, not operator authority", () => {
		const { target, eventId, outcome, reason, evidence, ...fences } = input;
		const control: WorkTaskDispositionBasis["controls"][number] =
			{ kind: "control", controlId: "control-original", eventId: "original-event", clientRef: null };
		const basis: WorkTaskDispositionBasis = { ...fences, controls: [control], controlsCompleteness: "partial", report: target };
		expect(validateWorkTaskDispositionBasis(basis)).toEqual(basis);
		for (const invalid of [
			{ ...basis, reason }, { ...basis, outcome }, { ...basis, evidence }, { ...basis, eventId },
			{ ...basis, permissions: ["disposition"] }, { ...basis, expectedTaskVersion: -1 },
			{ ...basis, sessionId: "original" }, { ...basis, epoch: 0 }, { ...basis, cwd: "/work/../other" },
			{ ...basis, requestHash: "not-a-hash" }, { ...basis, controlsCompleteness: "unknown" },
			{ ...basis, controls: Array.from({ length: 21 }, (_, n) => ({ ...control, controlId: `control-${n}` })) },
			{ ...basis, controls: [control, control] }, { ...basis, controls: [{ ...control, receipt: "accepted" }] },
			{ ...basis, controls: [{ ...control, clientRef: undefined }] },
			{ ...basis, report: { kind: "report" } }, { ...basis, report: { ...target, accepted: true } },
		]) expect(() => validateWorkTaskDispositionBasis(invalid)).toThrow();
	});
	test("rejects synthetic authority, fabricated outcomes, unknown fields and unbounded evidence", () => {
		for (const invalid of [
			{ ...input, principalId: "owner" }, { ...input, origin: LOOPBACK_ORIGIN },
			{ ...input, authenticated: true }, { ...input, execution: "cancelled" },
			{ ...input, outcome: "accepted" }, { ...input, reason: " " },
			{ ...input, reason: "é".repeat(1025) }, { ...input, eventId: "x".repeat(1025) },
			{ ...input, expectedTaskVersion: -1 }, { ...input, expectedTaskVersion: 1.5 },
			{ ...input, sessionId: "original", epoch: null }, { ...input, cwd: "/work/../other" },
			{ ...input, requestHash: "A".repeat(64) }, { ...input, target: { kind: "report" } },
			{ ...input, target: { kind: "report", reportId: null, controlId: "invented" } },
			{ ...input, evidence: { ...input.evidence, availability: "complete" } },
			{ ...input, evidence: { ...input.evidence, detail: "x".repeat(4097) } },
			{ ...input, evidence: { ...input.evidence, remoteProof: "authenticated" } },
			{ ...input, evidence: { ...input.evidence, evidenceAt: "yesterday" } },
		]) expect(() => validateWorkTaskDispositionParams(invalid)).toThrow();
	});
	test("strict results carry administrative identity and historical retained uncertainty only", () => {
		const record: WorkTaskDispositionResult["record"] = {
			request: input, principalId: "local-ipc:owner", origin: LOOPBACK_ORIGIN,
			recordedAt: "2026-10-06T00:01:00.000Z", taskVersion: 3,
			retained: { obligationState: "held", reportId: null, holdReason: "original_unknown", controlPhase: null },
		};
		const result: WorkTaskDispositionResult = { execution: "none", disposition: "recorded", dispositionId: `disposition-${"b".repeat(64)}`,
			sourceId: `disposition-${"b".repeat(64)}`, deliveryId: null, record };
		expect(validateWorkTaskDispositionResult(result)).toEqual(result);
		for (const invalid of [
			{ ...result, execution: "released" }, { ...result, capacityReleased: true },
			{ ...result, disposition: "accepted" }, { ...result, sourceId: "other" },
			{ ...result, record: { ...record, taskVersion: 2 } },
			{ ...result, record: { ...record, retained: { ...record.retained, obligationState: "final_admitted" } } },
			{ ...result, record: { ...record, retained: { ...record.retained, receipt: "accepted" } } },
		]) expect(() => validateWorkTaskDispositionResult(invalid)).toThrow();
		expect(() => validateWorkTaskDispositionRecord({ ...record, ownerCredential: true })).toThrow();
	});
});

describe("negotiation", () => {
	test("picks highest mutual version", () => {
		const result = negotiate({ supportedVersions: [PROFILE_VERSION] });
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.negotiated.profileVersion).toBe(PROFILE_VERSION);
	});

	test("rejects disjoint versions with typed code", () => {
		const result = negotiate({ supportedVersions: ["9.9"] });
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.code).toBe("incompatible_profile_version");
	});

	test("serves only profile 1.1 without falling back to 1.0", () => {
		const current = negotiate({ supportedVersions: ["1.0", "1.1"] });
		expect(current.ok).toBe(true);
		if (current.ok) expect(current.negotiated.profileVersion).toBe("1.1");
		const obsolete = negotiate({ supportedVersions: ["1.0"] });
		expect(obsolete.ok).toBe(false);
		if (!obsolete.ok) {
			expect(obsolete.code).toBe("incompatible_profile_version");
			expect(obsolete.supportedVersions).toEqual(["1.1"]);
		}
	});

	test("rejects missing required capability", () => {
		const result = negotiate({
			supportedVersions: [PROFILE_VERSION],
			requiredCapabilities: ["timetravel"],
		});
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.code).toBe("missing_required_capability");
	});

	test("unknown optional fields are ignored", () => {
		const hello = {
			supportedVersions: [PROFILE_VERSION],
			futureOptionalField: { anything: true },
		} as never;
		expect(negotiate(hello).ok).toBe(true);
	});
});

describe("frames", () => {
	test("round-trips a request frame", () => {
		const line = encodeFrame({
			v: PROFILE_VERSION,
			type: "request",
			id: "r1",
			verb: "gateway.status",
		});
		const frame = decodeFrame(line.trim());
		expect(frame.type).toBe("request");
	});

	test("rejects non-JSON with malformed_frame", () => {
		expect(() => decodeFrame("not json")).toThrow(ProtocolError);
		try {
			decodeFrame("not json");
		} catch (error) {
			expect((error as ProtocolError).code).toBe("malformed_frame");
		}
	});

	test("rejects unknown frame type typed", () => {
		try {
			decodeFrame(JSON.stringify({ v: PROFILE_VERSION, type: "teleport" }));
			expect.unreachable();
		} catch (error) {
			expect((error as ProtocolError).code).toBe("unsupported_frame_type");
		}
	});

	test("rejects oversized frames", () => {
		const big = "x".repeat(MAX_FRAME_BYTES + 1);
		expect(() => decodeFrame(big)).toThrow(ProtocolError);
	});

	test("decoder splits chunked NDJSON and buffers partials", () => {
		const decoder = new FrameDecoder();
		const line = encodeFrame({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload: {} });
		const half = Math.floor(line.length / 2);
		expect(decoder.feed(line.slice(0, half))).toHaveLength(0);
		const frames = decoder.feed(line.slice(half));
		expect(frames).toHaveLength(1);
		expect(frames[0]?.type).toBe("event");
	});
});

describe("Firstmate wire validation", () => {
	const taskId = "12345678-1234-4123-8123-123456789abc";
	const otherId = "12345678-1234-4123-8123-123456789abd";
	const claimId = "12345678-1234-4123-8123-123456789abe";
	const parentOrigin = { platform: "discord", kind: "channel", conversationId: "222", boundaryId: "333" } satisfies OriginRef;
	const threadOrigin = { ...parentOrigin, kind: "thread", conversationId: "111", parentId: "222" } satisfies OriginRef;
	const spec = { taskId, kind: "read_only", surface: { threadOrigin } } satisfies WorkTaskSpec;
	const steer = { name: `fm-${taskId}`, text: "Inspect the evidence", taskId, eventId: `cli:${claimId}`, expectedOpRef: "gw-fm-original" };

	test("parses either surface without copying assignment into task metadata", () => {
		expect(validateWorkTaskSpec(spec)).toEqual(spec);
		const parent = { ...spec, kind: "code_mutating", surface: { parentOrigin, title: "Review" }, context: "Selected evidence" } satisfies WorkTaskSpec;
		expect(validateWorkTaskSpec(parent)).toEqual(parent);
		expect(validateWorkTaskSpec({ ...spec, surface: { parentOrigin, title: "é".repeat(100) } }).surface.title).toBe("é".repeat(100));
		expect(validateWorkStartParams({ name: `fm-${taskId}`, text: "Assignment", task: spec })).toEqual({
			name: `fm-${taskId}`, text: "Assignment", task: spec,
		});
	});

	test("titles reject ASCII controls and preserve printable boundary characters", () => {
		for (const character of ["\0", "\t", "\n", "\u001f", "\u007f"]) {
			const title = `before${character}after`;
			expect(() => validateWorkTaskSpec({ ...spec, surface: { parentOrigin, title } })).toThrow(ProtocolError);
		}
		for (const title of ["before after", " Review ", "~", "검토 café 😀"]) {
			expect(validateWorkTaskSpec({ ...spec, surface: { parentOrigin, title } }).surface.title).toBe(title);
		}
	});

	test("rejects malformed unknown inputs and ambiguous task alternatives", () => {
		for (const input of [null, [], 1, "task", {}, { ...spec, taskId: "not-a-uuid" },
			{ ...spec, kind: "mutating" }, { ...spec, text: "second assignment" },
			{ ...spec, surface: {} }, { ...spec, surface: { threadOrigin, parentOrigin } },
			{ ...spec, surface: { threadOrigin, title: "ambiguous" } },
			{ ...spec, surface: { parentOrigin, title: "line\nbreak" } },
			{ ...spec, surface: { parentOrigin, title: "x".repeat(101) } }]) {
			expect(() => validateWorkTaskSpec(input)).toThrow(ProtocolError);
		}
	});

	test("requires Discord guild structure, not a DM or plausible origin segment", () => {
		for (const origin of [
			{ ...threadOrigin, platform: "slack" },
			{ ...threadOrigin, kind: "dm", peerId: "444" },
			{ ...threadOrigin, boundaryId: undefined },
			{ ...threadOrigin, boundaryId: "guild-333" },
			{ ...threadOrigin, parentId: undefined },
			{ ...threadOrigin, parentId: "111" },
			{ ...threadOrigin, conversationId: 111 },
			{ ...threadOrigin, conversationId: "18446744073709551616" },
			{ ...threadOrigin, conversationId: "0" },
			{ ...threadOrigin, peerId: "444" },
		]) {
			expect(() => validateWorkTaskSpec({ ...spec, surface: { threadOrigin: origin } })).toThrow(ProtocolError);
		}
		expect(() => validateWorkTaskSpec({ ...spec, surface: { parentOrigin: { ...parentOrigin, parentId: "555" } } })).toThrow(ProtocolError);
	});

	test("enforces UTF-8 bounds without silently truncating context or text", () => {
		const context = "é".repeat(WORK_TASK_CONTEXT_MAX_BYTES / 2);
		expect(validateWorkTaskSpec({ ...spec, context }).context).toBe(context);
		expect(() => validateWorkTaskSpec({ ...spec, context: `${context}é` })).toThrow(ProtocolError);
		expect(() => validateWorkTaskSpec({ ...spec, context: "x".repeat(WORK_TASK_CONTEXT_MAX_BYTES + 1) })).toThrow(ProtocolError);
		const text = "é".repeat(WORK_TASK_TEXT_MAX_BYTES / 2);
		expect(validateWorkSteerParams({ ...steer, text }).text).toBe(text);
		expect(() => validateWorkSteerParams({ ...steer, text: `${text}é` })).toThrow(ProtocolError);
	});

	test("task admission rejects conflicting identity and resume but preserves unbound use", () => {
		expect(() => validateWorkStartParams({ name: `fm-${otherId}`, text: "Assignment", task: spec })).toThrow(ProtocolError);
		expect(() => validateWorkStartParams({ name: `fm-${taskId}`, text: "Assignment", task: spec, resume: true })).toThrow(ProtocolError);
		expect(validateWorkStartParams({ name: "ordinary", text: "Assignment", resume: true, model: { preset: "fast" } })).toEqual({
			name: "ordinary", text: "Assignment", resume: true, model: { preset: "fast" },
		});
	});

	test("steering identity is all-or-nothing with typed scope and stable event ID", () => {
		expect(validateWorkSteerParams(steer)).toEqual(steer);
		expect(validateWorkSteerParams({ ...steer, eventId: claimId, kind: "code_mutating" }).kind).toBe("code_mutating");
		expect(validateWorkSteerParams({ name: "ordinary", text: "Continue" })).toEqual({ name: "ordinary", text: "Continue" });
		for (const input of [
			{ name: steer.name, text: steer.text, taskId },
			{ name: steer.name, text: steer.text, eventId: steer.eventId },
			{ name: steer.name, text: steer.text, expectedOpRef: steer.expectedOpRef },
			{ name: steer.name, text: steer.text, kind: "code_mutating" },
			{ ...steer, taskId: otherId }, { ...steer, name: "other" },
			{ ...steer, eventId: "unstable" }, { ...steer, eventId: "discord:" },
			{ ...steer, eventId: `cli:${"x".repeat(WORK_TASK_EVENT_ID_MAX_BYTES)}` },
			{ ...steer, eventId: "cli:line\nbreak" }, { ...steer, expectedOpRef: "GW-OTHER" },
			{ ...steer, kind: "unknown" }, { ...steer, text: "" },
		]) expect(() => validateWorkSteerParams(input)).toThrow(ProtocolError);
	});

	test("status fences task and operation without treating prompt status as steer proof", () => {
		expect(validateWorkStatusParams({ name: steer.name, taskId, expectedOpRef: steer.expectedOpRef }).taskId).toBe(taskId);
		expect(() => validateWorkStatusParams({ name: "other", taskId })).toThrow(ProtocolError);
		expect(() => validateWorkStatusParams({ name: steer.name, expectedOpRef: steer.expectedOpRef })).toThrow(ProtocolError);
		expect(() => validateWorkSteerParams({ ...steer, status: "accepted", clientRef: claimId })).toThrow(ProtocolError);
	});

	test("steering preserves a caller routing hint without enabling task mode or supplying task identity", () => {
		const ordinary = { name: "ordinary", text: "Continue", callerSessionId: otherId };
		expect(validateWorkSteerParams(ordinary)).toEqual(ordinary);
		expect(validateWorkSteerParams({ ...steer, callerSessionId: otherId })).toEqual({ ...steer, callerSessionId: otherId });
		expect(() => validateWorkSteerParams({ ...ordinary, taskId })).toThrow(ProtocolError);
		expect(() => validateWorkSteerParams({ ...steer, callerSessionId: otherId, taskId: otherId })).toThrow(ProtocolError);
	});

	test("steering requires canonical caller UUIDs in both bound and unbound requests", () => {
		for (const base of [steer, { name: "ordinary", text: "Continue" }]) {
			for (const callerSessionId of [
				undefined, null, 123, {}, "", "not-a-uuid", otherId.toUpperCase(),
				` ${otherId}`, `${otherId}\n`, otherId.replace("-4123-", "-0123-"),
				otherId.replace("-8123-", "-7123-"),
			]) {
				expect(() => validateWorkSteerParams({ ...base, callerSessionId })).toThrow(ProtocolError);
			}
		}
	});

	test("caller routing hints cannot smuggle authority or synthetic Discord source fields", () => {
		for (const base of [steer, { name: "ordinary", text: "Continue" }]) {
			for (const extra of [
				{ principalId: "local-ipc:owner" }, { authorized: true },
				{ source: { origin: threadOrigin } }, { origin: threadOrigin },
				{ callerSessionID: otherId }, { unknownField: true },
			]) {
				expect(() => validateWorkSteerParams({ ...base, callerSessionId: otherId, ...extra })).toThrow(ProtocolError);
			}
		}
	});

	test("work.jobs represents corrupt task holds without fabricated operation identities", () => {
		const result: VerbCatalogV01["work.jobs"]["result"] = {
			jobs: [], tasks: [], taskErrors: [{ taskId, reason: "invalid attempt" }],
		};
		const frame = decodeFrame(encodeFrame({ v: PROFILE_VERSION, type: "response", id: "jobs", result }).trim());
		expect(frame.type).toBe("response");
		if (frame.type !== "response") throw new Error("expected response");
		expect(frame.result).toEqual({
			jobs: [], tasks: [], taskErrors: [{ taskId, reason: "invalid attempt" }],
		});
	});

	test("claim and bind preserve IDs and reject mixed outcomes or recreation options", () => {
		expect(validateWorkThreadClaimParams({ taskId, claimId })).toEqual({ taskId, claimId });
		const bound = { taskId, claimId, outcome: { kind: "bound", origin: threadOrigin } } satisfies WorkThreadBindParams;
		const held = { taskId, claimId, outcome: { kind: "held", reason: "Creation outcome unknown" } } satisfies WorkThreadBindParams;
		expect(validateWorkThreadBindParams(bound)).toEqual(bound);
		expect(validateWorkThreadBindParams(held)).toEqual(held);
		expect(() => validateWorkThreadClaimParams({ taskId, claimId, retry: true })).toThrow(ProtocolError);
		expect(() => validateWorkThreadClaimParams({ taskId, claimId: "unstable" })).toThrow(ProtocolError);
		for (const outcome of [
			{ kind: "bound", origin: threadOrigin, reason: "also held" },
			{ kind: "held", reason: "held", origin: threadOrigin },
			{ kind: "bound", origin: parentOrigin }, { kind: "held", reason: "" },
			{ kind: "held", reason: "é".repeat(513) }, { kind: "retry" },
		]) expect(() => validateWorkThreadBindParams({ taskId, claimId, outcome })).toThrow(ProtocolError);
	});

	test("context and recovery cannot smuggle replacement execution or unbounded selection", () => {
		expect(validateWorkTaskContextParams({ taskId, topic: "decision", continuation: "snapshot:1" })).toEqual({
			taskId, topic: "decision", continuation: "snapshot:1",
		});
		expect(validateWorkTaskRecoverParams({ taskId })).toEqual({ taskId });
		for (const input of [{ taskId, opRef: "replacement" }, { taskId, resume: true }, { taskId: "invalid" }]) {
			expect(() => validateWorkTaskRecoverParams(input)).toThrow(ProtocolError);
		}
		for (const input of [new Date(), { taskId, limit: 5000 }, { topic: "é".repeat(129) }, { continuation: "x".repeat(1025) }, { taskId: null }]) {
			expect(() => validateWorkTaskContextParams(input)).toThrow(ProtocolError);
		}
	});
});

describe("Firstmate mapped delivery seams", () => {
	const taskId = "12345678-1234-4123-8123-123456789abc";
	const otherId = "12345678-1234-4123-8123-123456789abd";
	const origin = {
		platform: "discord", kind: "thread", conversationId: "111", parentId: "222", boundaryId: "333",
	} as const;
	const metadata = { taskId, opRef: "gw-fm-original", sourceId: "source:activation:1", mappedOnly: true } as const;
	const receipt = { deliveryId: "delivery-original", platformReceipt: { origin, messageIds: ["444", "555"] } };

	test("task-key cursor accepts omitted params and rejects other pagination or execution fields", () => {
		const params: VerbCatalogV01["work.jobs"]["params"] = { afterTaskId: taskId };
		expect(validateWorkJobsParams(params)).toEqual(params);
		expect(validateWorkJobsParams(undefined)).toBeUndefined();
		expect(validateWorkJobsParams({})).toEqual({});
		for (const value of [null, [], "cursor", { afterTaskId: undefined }, { afterTaskId: otherId.toUpperCase() },
			{ afterTaskId: "12345678-1234-0123-8123-123456789abc" }, { afterTaskId: "not-a-uuid" },
			{ afterTaskId: taskId, cursor: "page" }, { limit: 21 }, { resume: true }]) {
			expect(() => validateWorkJobsParams(value)).toThrow(ProtocolError);
		}
	});

	test("mapped tags survive Unicode payload framing with immutable correlation", () => {
		const workTask = validateWorkTaskMessageMetadata({ ...metadata, bindingRevision: "a".repeat(64) }, {
			origin, taskId, opRef: metadata.opRef, sourceId: metadata.sourceId,
		});
		const payload: ChatMessagePayload = {
			turnId: "original-turn", deliveryId: "delivery-original", origin, role: "assistant",
			text: "검토 결과: café", final: true, workTask,
		};
		const frame = decodeFrame(encodeFrame({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload }).trim());
		expect(frame.type).toBe("event");
		if (frame.type === "event") expect(frame.payload).toEqual(payload);
		expect(validateWorkTaskMessageMetadata(metadata, { origin })).toEqual(metadata);
	});

	test("mapped source IDs reject ASCII controls and preserve printable boundary characters", () => {
		for (const character of ["\0", "\t", "\n", "\u001f", "\u007f"]) {
			const sourceId = `before${character}after`;
			expect(() => validateWorkTaskMessageMetadata({ ...metadata, sourceId }, { origin })).toThrow(ProtocolError);
		}
		for (const sourceId of ["before after", " source ", "~", "검토 café 😀"]) {
			expect(validateWorkTaskMessageMetadata({ ...metadata, sourceId }, {
				origin, taskId, opRef: metadata.opRef, sourceId,
			}).sourceId).toBe(sourceId);
		}
	});

	test("mapped metadata rejects runtime-row fields, malformed identities and mismatched correlation", () => {
		for (const value of [null, [], { ...metadata, taskId: "invalid" }, { ...metadata, mappedOnly: false },
			{ ...metadata, mappedOnly: "true" }, { ...metadata, opRef: "GW bad" },
			{ ...metadata, sourceId: "" }, { ...metadata, sourceId: "source\ninjection" },
			{ ...metadata, sourceId: "é".repeat(513) }, { ...metadata, version: 1 },
			{ ...metadata, sessionId: otherId }, { ...metadata, bindingRevision: "task-version-2" },
			{ ...metadata, bindingRevision: "A".repeat(64) }, { ...metadata, bindingRevision: undefined }]) {
			expect(() => validateWorkTaskMessageMetadata(value, { origin })).toThrow(ProtocolError);
		}
		for (const mismatch of [{ taskId: otherId }, { opRef: "gw-other" }, { sourceId: "source:other" }]) {
			expect(() => validateWorkTaskMessageMetadata(metadata, { origin, ...mismatch })).toThrow(ProtocolError);
		}
		expect(validateWorkTaskMessageMetadata({ ...metadata, sourceId: "é".repeat(512) }, { origin }).sourceId).toHaveLength(512);
	});

	test("tags and receipts reject non-mapped or malformed Discord guild origins", () => {
		for (const invalid of [
			{ ...origin, platform: "slack" }, { ...origin, kind: "dm" }, { ...origin, kind: "channel" },
			{ ...origin, parentId: undefined }, { ...origin, boundaryId: undefined },
			{ ...origin, parentId: origin.conversationId }, { ...origin, boundaryId: origin.conversationId },
			{ ...origin, peerId: "444" }, { ...origin, conversationId: "01" },
			{ ...origin, conversationId: "18446744073709551616" }, { ...origin, extra: true },
		]) {
			expect(() => validateDeliveryConfirmParams({ ...receipt, platformReceipt: { ...receipt.platformReceipt, origin: invalid } })).toThrow(ProtocolError);
			// The validator's context is typed but still checks untrusted platform structure.
			expect(() => validateWorkTaskMessageMetadata(metadata, { origin: invalid as typeof origin })).toThrow(ProtocolError);
		}
	});

	test("whole-payload receipt preserves chunk order and ordinary confirmations remain receipt-free", () => {
		expect(validateDeliveryConfirmParams(receipt)).toEqual(receipt);
		expect(validateDeliveryConfirmParams({ deliveryId: receipt.deliveryId })).toEqual({ deliveryId: receipt.deliveryId });
		const messageIds = ["18446744073709551615", "1"];
		expect(validateDeliveryConfirmParams({ ...receipt, platformReceipt: { origin, messageIds } }).platformReceipt?.messageIds).toEqual(messageIds);
		const maximum = Array.from({ length: DELIVERY_RECEIPT_MAX_MESSAGE_IDS }, (_, index) => String(index + 1));
		expect(validateDeliveryConfirmParams({ ...receipt, platformReceipt: { origin, messageIds: maximum } }).platformReceipt?.messageIds).toEqual(maximum);
	});

	test("receipt IDs reject duplicates, overflow, noncanonical forms and incomplete arrays without clipping", () => {
		for (const messageIds of [[], ["444", "444"], ["0"], ["01"], ["-1"], ["1.0"], ["1e3"], [" 1"], [1],
			["18446744073709551616"], ["999999999999999999999"], ["é"], [undefined], new Array(1),
			Array.from({ length: DELIVERY_RECEIPT_MAX_MESSAGE_IDS + 1 }, (_, index) => String(index + 1))]) {
			expect(() => validateDeliveryConfirmParams({ ...receipt, platformReceipt: { origin, messageIds } })).toThrow(ProtocolError);
		}
	});

	test("confirmation rejects alternate receipts, unknown keys, fake floor time and metadata byte overflow", () => {
		for (const value of [null, [], {}, { ...receipt, deliveryId: "" }, { ...receipt, platformReceipt: null },
			{ ...receipt, platformReceipt: undefined }, { ...receipt, receipt: receipt.platformReceipt },
			{ ...receipt, settled: true }, { ...receipt, platformReceipt: { ...receipt.platformReceipt, platformCreatedAt: 1 } },
			{ ...receipt, platformReceipt: { ...receipt.platformReceipt, messageId: "444" } },
			{ ...receipt, platformReceipt: { messageIds: ["444"] } },
			{ ...receipt, platformReceipt: { origin, messageIds: "444" } }]) {
			expect(() => validateDeliveryConfirmParams(value)).toThrow(ProtocolError);
		}
		const oversized = { ...receipt, deliveryId: "é".repeat(DELIVERY_CONFIRM_MAX_BYTES / 2) };
		expect(() => validateDeliveryConfirmParams(oversized)).toThrow(/UTF-8 byte bound/);
	});

	test("original creation evidence is separate from edit time and never accepts caller floor claims", () => {
		const evidence = { platformCreatedAt: 1791266400000, recovered: true };
		expect(validateWorkTaskOriginSource(evidence, origin)).toEqual(evidence);
		for (const value of [{}, { platformCreatedAt: "1791266400000" }, { platformCreatedAt: -1 },
			{ platformCreatedAt: 1.5 }, { platformCreatedAt: Number.NaN }, { platformCreatedAt: Number.POSITIVE_INFINITY },
			{ platformCreatedAt: 8640000000000001 }, { ...evidence, recovered: "true" },
			{ ...evidence, activationFloor: 1 }, { ...evidence, eventId: "invented" }]) {
			expect(() => validateWorkTaskOriginSource(value, origin)).toThrow(ProtocolError);
		}
		expect(() => validateWorkTaskOriginSource(evidence, { ...origin, platform: "slack" })).toThrow(ProtocolError);
	});

	test("mapped send and edit result fixtures discriminate before persona turn handling", () => {
		const mapped: WorkTaskControlProjection = {
			route: "work_task", acceptance: "durable", taskId, controlId: otherId,
			opRef: metadata.opRef, delivery: "pending",
		} satisfies ChatSendResult;
		const edited: WorkTaskControlProjection = { ...mapped, delivery: "held", reason: "Original evidence unavailable" };
		const persona: ChatSendResult = { turnId: "persona-turn", engaged: true };
		for (const result of [mapped, edited, persona] as readonly (ChatSendResult | ChatEditResult)[]) {
			const frame = decodeFrame(encodeFrame({ v: PROFILE_VERSION, type: "response", id: "r1", result }).trim());
			expect(frame.type).toBe("response");
			if (frame.type === "response") expect(frame.result).toEqual(result);
			if (result.route === "work_task") {
				expect(result.acceptance).toBe("durable");
				expect(result).not.toHaveProperty("turnId");
				expect(result).not.toHaveProperty("engaged");
			} else {
				expect(result.turnId).toBe("persona-turn");
				expect(result.engaged).toBe(true);
			}
		}
	});
});

describe("origin normalization", () => {
	test("originKey is deterministic and validated", () => {
		const key = originKey({
			platform: "discord",
			kind: "thread",
			conversationId: "111",
			parentId: "222",
			boundaryId: "guild-333",
		});
		expect(key).toBe("discord/thread/111/parent=222");
		expect(parseOriginKey(key)).toEqual({
			platform: "discord",
			kind: "thread",
			conversationId: "111",
			parentId: "222",
		});
	});

	test("retains valid policy boundaries only on group origins without changing identity", () => {
		for (const ref of [
			{ platform: "discord", kind: "channel", conversationId: "c1", boundaryId: "guild-1" },
			{ platform: "discord", kind: "thread", conversationId: "t1", parentId: "c1", boundaryId: "guild-1" },
			{ platform: "telegram", kind: "topic", conversationId: "t1", parentId: "c1", boundaryId: "chat-1" },
		] as const) {
			const validated = validateOriginRef(ref);
			expect(validated).toBe(ref);
			expect(validated.boundaryId).toBe(ref.boundaryId);
			expect(originKey(ref)).toBe(originKey({ ...ref, boundaryId: undefined }));
		}
	});

	test("rejects invalid and non-group boundary metadata", () => {
		const channel = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
		for (const boundaryId of ["", "bad/id", "x".repeat(257)]) {
			expect(() => validateOriginRef({ ...channel, boundaryId })).toThrow();
		}
		expect(() =>
			validateOriginRef({
				platform: "discord",
				kind: "dm",
				conversationId: "d1",
				peerId: "u1",
				boundaryId: "g1",
			} as never),
		).toThrow(/boundaryId/);
		expect(() => validateOriginRef({ ...channel, boundaryId: null } as never)).toThrow(/boundaryId/);
	});

	test("dm requires peerId; channel forbids it", () => {
		expect(() => validateOriginRef({ platform: "discord", kind: "dm", conversationId: "c1" })).toThrow();
		expect(() =>
			validateOriginRef({
				platform: "discord",
				kind: "channel",
				conversationId: "c1",
				peerId: "p1",
			}),
		).toThrow();
	});

	test("monitor session origins are scoped by monitor id and round-trip", () => {
		const key = originKey(monitorSessionOrigin("m-1", "backlog.watch"));
		expect(key).toBe("monitor/eventtype/backlog.watch/parent=m-1");
		expect(key).not.toBe(originKey(monitorSessionOrigin("m-2", "backlog.watch")));
		expect(parseOriginKey(key)).toEqual(monitorSessionOrigin("m-1", "backlog.watch"));
		expect(() => originKey(monitorSessionOrigin("bad/id", "backlog.watch"))).toThrow();
	});

	test("thread requires parentId", () => {
		expect(() => validateOriginRef({ platform: "telegram", kind: "topic", conversationId: "c1" })).toThrow();
	});

	test("slack supports dm, channel and thread, and its thread ids round-trip through originKey", () => {
		expect(originKey({ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" })).toBe(
			"slack/dm/D1/peer=U1",
		);
		expect(originKey({ platform: "slack", kind: "channel", conversationId: "C1" })).toBe("slack/channel/C1");
		// A Slack thread is identified by its channel:ts pair; the colon and dot are
		// legal segment characters, so the key parses back to the same origin.
		const thread = {
			platform: "slack",
			kind: "thread",
			conversationId: "C123:1726543210.123456",
			parentId: "C123",
		} as const;
		const key = originKey(thread);
		expect(key).toBe("slack/thread/C123:1726543210.123456/parent=C123");
		expect(parseOriginKey(key)).toEqual(thread);
	});

	test("slack rejects the telegram-only topic kind", () => {
		expect(() =>
			validateOriginRef({ platform: "slack", kind: "topic", conversationId: "C1:1.2", parentId: "C1" }),
		).toThrow(/topic/);
		expect(() =>
			validateOriginRef({ platform: "telegram", kind: "topic", conversationId: "t1", parentId: "-100" }),
		).not.toThrow();
	});

	test("chat platforms are exactly the ones an adapter can deliver to", () => {
		expect(CHAT_PLATFORMS).toEqual(["discord", "telegram", "slack"]);
		for (const platform of CHAT_PLATFORMS) expect(isChatPlatform(platform)).toBe(true);
		expect(isChatPlatform("loopback")).toBe(false);
		expect(isChatPlatform("monitor")).toBe(false);
		expect(describeChatPlatforms()).toBe("discord, telegram or slack");
	});

	test("loopback origin is valid and stable", () => {
		expect(originKey(LOOPBACK_ORIGIN)).toBe("loopback/loopback/loopback");
	});

	test("two distinct origins never share a key", () => {
		const a = originKey({
			platform: "discord",
			kind: "dm",
			conversationId: "c1",
			peerId: "alice",
		});
		const b = originKey({
			platform: "discord",
			kind: "dm",
			conversationId: "c1",
			peerId: "bob",
		});
		expect(a).not.toBe(b);
	});

	test("rejects segment injection attempts", () => {
		expect(() =>
			validateOriginRef({
				platform: "discord",
				kind: "channel",
				conversationId: "c1/parent=evil",
			}),
		).toThrow();
	});
});

describe("silence tokens", () => {
	for (const text of [
		"preamble [SILENT]",
		"preamble\n[SILENT]\npostscript",
		"preamble [silent]",
		"preamble\n[silent]\npostscript",
	]) {
		test(`recognizes embedded marker ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(true);
			expect(isSilenceToken(text)).toBe(false);
		});
	}

	test("recognizes an original-body marker beyond a 2 KiB excerpt", () => {
		const text = `${"x".repeat(2049)}\n[SILENT]`;
		expect(containsSilenceToken(text)).toBe(true);
		expect(containsSilenceToken(text.slice(0, 2048))).toBe(false);
	});

	for (const text of ["ordinary text", "preamble SILENT", "preamble [NO_REPLY]", "preamble [ SILENT ]"]) {
		test(`does not broaden embedded grammar for ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(false);
		});
	}

	for (const text of [
		"- **답장 표시**: 👀 리액션, `[SILENT]`(답하지 않기) 같은 표시를 해석해요.",
		"quoted ``[SILENT]`` with a double-backtick span",
		"flow\n```\nadapter ⇄ [SILENT] ⇄ session\n```\nend",
		"inline `[silent]` lowercase",
	]) {
		test(`a marker inside markdown code is quoted, not a directive: ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(false);
			expect(isSilentOutput(text)).toBe(false);
		});
	}

	for (const text of [
		"explains `[SILENT]` in code, then opts out.\n\n[SILENT]",
		"```\ncode\n```\n[SILENT]",
		"unclosed ```\n[SILENT]",
		"stray ` backtick [SILENT]",
	]) {
		test(`a marker outside markdown code still silences: ${JSON.stringify(text)}`, () => {
			expect(containsSilenceToken(text)).toBe(true);
		});
	}

	test(`embedded [Silent] is not recognized (case-sensitive)`, () => {
		expect(containsSilenceToken("preamble [Silent]")).toBe(false);
	});

	for (const text of ["SILENT", "[SILENT]", "silent", "NO_REPLY", "NO REPLY", "[NO_REPLY]", "[NO REPLY]"]) {
		test(`preserves exact-body alias ${text}`, () => {
			expect(isSilenceToken(text)).toBe(true);
			expect(isSilenceToken(`  ${text}\n`)).toBe(true);
		});
	}
});

describe("isSilentOutput", () => {
	test("exact-match tokens are silent", () => {
		expect(isSilentOutput("[SILENT]")).toBe(true);
		expect(isSilentOutput("SILENT")).toBe(true);
		expect(isSilentOutput("silent")).toBe(true);
		expect(isSilentOutput("NO_REPLY")).toBe(true);
		expect(isSilentOutput("NO REPLY")).toBe(true);
		expect(isSilentOutput("[NO_REPLY]")).toBe(true);
		expect(isSilentOutput("[NO REPLY]")).toBe(true);
		expect(isSilentOutput("  [SILENT]\n")).toBe(true);
		expect(isSilentOutput("  silent  ")).toBe(true);
	});

	test("embedded [SILENT] or [silent] markers anywhere silence (issue #338: propagate.ts must use this)", () => {
		// Leading markers
		expect(isSilentOutput("[SILENT] This is a status update")).toBe(true);
		expect(isSilentOutput("[SILENT]\nMultiline status")).toBe(true);
		expect(isSilentOutput("[silent] lowercase marker with text")).toBe(true);
		// Trailing markers
		expect(isSilentOutput("Nothing to report. [SILENT]")).toBe(true);
		expect(isSilentOutput("Finished processing. [silent]")).toBe(true);
		// Mid-text markers
		expect(isSilentOutput("Please see [SILENT] in docs")).toBe(true);
		expect(isSilentOutput("This bug is about [SILENT] marker support")).toBe(true);
	});

	test("non-silent text is not silent", () => {
		expect(isSilentOutput("ordinary text")).toBe(false);
		expect(isSilentOutput("hello world")).toBe(false);
		expect(isSilentOutput("")).toBe(false);
		expect(isSilentOutput("This is a real response")).toBe(false);
		// Case-sensitive embedded markers: [Silent], [silent] only, not mixed case
		expect(isSilentOutput("preamble [Silent]")).toBe(false);
	});
});

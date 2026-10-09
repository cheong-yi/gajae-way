import { expect, test } from "bun:test";
import {
	type WorkTaskCloseoutEvidence,
	type WorkTaskReviewParams,
	validateWorkTaskCloseoutEvidence,
	validateWorkTaskCloseoutSerializationStatement,
	validateWorkTaskCloseoutVerificationStatement,
	validateWorkTaskOwnerApproval,
	validateWorkTaskReviewParams,
} from "@gajae-gateway/protocol";
import {
	type WorkTaskCloseoutControlView,
	type WorkTaskCloseoutInput,
	type WorkTaskCloseoutReviewView,
	type WorkTaskCloseoutSourceView,
	type WorkTaskCloseoutTaskView,
	deriveWorkTaskCodingCompletion,
} from "../src/orchestrator/work-task-closeout";

const TASK = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
const REVIEW_ID = "9c2a5f10-1f3b-4a5e-8c7d-1a2b3c4d5e6f";
const OP_REF = "gw-work-fm-closeout";
const REPORT = "report-7";
const REVIEWED = `assignment-${TASK}`;
const SERIAL_CONTROL = "control-1";
const APPROVAL_CONTROL = "control-2";
const SERIAL_SOURCE = `instruction-${SERIAL_CONTROL}`;
const APPROVAL_SOURCE = `instruction-${APPROVAL_CONTROL}`;
const VERIFY_SOURCE = "checkpoint-verify-1";
// Symbols for hashes already validated by the store; durable hashing is tested
// through GatewayDatabase in work-task-store.test.ts.
const HASH_REVIEWED = "a".repeat(64);
const HASH_APPROVAL = "b".repeat(64);
const HASH_SERIAL = "c".repeat(64);
const HASH_VERIFY = "d".repeat(64);
const HASH_OTHER = "9".repeat(64);
/** Integration target base. */
const BASE = "1".repeat(40);
/** Worker base — deliberately different from the integration target base. */
const WORKER_BASE = "5".repeat(40);
/** Worker result commit inside the reviewed diff. */
const WORKER_COMMIT = "2".repeat(40);
/** Integrated commit after merge/cherry-pick; deliberately different from WORKER_COMMIT. */
const INTEGRATED_COMMIT = "4".repeat(40);
const DIFF = "e".repeat(64);
const COMMON_DIR = "/srv/gajaeway/.worktrees/fm-worker";
const OWNER_CHANNEL = "discord/channel/111";
const OWNER_THREAD = "discord/thread/222";
const MAPPED_HUMAN = "discord-user-owner";
const TARGET = { ref: "refs/heads/main", baseCommit: BASE, resultCommit: INTEGRATED_COMMIT };
const WORKER_RESULT = { baseCommit: WORKER_BASE, commit: WORKER_COMMIT, diffHash: DIFF };

const approvalRaw = {
	kind: "work_task_closeout_approval",
	taskId: TASK,
	opRef: OP_REF,
	action: "integrate",
	repository: { commonDir: COMMON_DIR },
	target: TARGET,
	result: WORKER_RESULT,
	reviewed: { sourceId: REVIEWED, contentHash: HASH_REVIEWED, reportId: REPORT },
	verification: { sourceId: VERIFY_SOURCE, contentHash: HASH_VERIFY },
	serialization: { sourceId: SERIAL_SOURCE, contentHash: HASH_SERIAL },
	answers: [],
};
const approvalBody = JSON.stringify(validateWorkTaskOwnerApproval(approvalRaw));

const verificationRaw = {
	kind: "work_task_closeout_verification",
	taskId: TASK,
	opRef: OP_REF,
	repository: { commonDir: COMMON_DIR },
	target: TARGET,
	result: WORKER_RESULT,
	resultingCommit: INTEGRATED_COMMIT,
	checks: [
		{ name: "typecheck", outcome: "pass" },
		{ name: "unit", outcome: "pass" },
	],
	conflicts: "none",
};
const verificationBody = JSON.stringify(validateWorkTaskCloseoutVerificationStatement(verificationRaw));

const serializationRaw = {
	kind: "work_task_closeout_serialization",
	taskId: TASK,
	opRef: OP_REF,
	repository: { commonDir: COMMON_DIR },
	target: TARGET,
	integrationOwner: MAPPED_HUMAN,
	order: { baseCommit: BASE, resultCommit: INTEGRATED_COMMIT },
};
const serializationBody = JSON.stringify(validateWorkTaskCloseoutSerializationStatement(serializationRaw));

const evidenceRaw = {
	repository: { commonDir: COMMON_DIR },
	result: WORKER_RESULT,
	target: TARGET,
	serialization: { sourceId: SERIAL_SOURCE, contentHash: HASH_SERIAL, controlId: SERIAL_CONTROL, sequence: 4 },
	verification: {
		sourceId: VERIFY_SOURCE,
		contentHash: HASH_VERIFY,
		resultingCommit: INTEGRATED_COMMIT,
		checks: verificationRaw.checks,
		conflicts: "none",
	},
	ownerApproval: { sourceId: APPROVAL_SOURCE, contentHash: HASH_APPROVAL },
	action: "integrate",
};

const evidence: WorkTaskCloseoutEvidence = validateWorkTaskCloseoutEvidence(evidenceRaw);

function closeout(patch: Record<string, unknown>): WorkTaskCloseoutEvidence {
	return validateWorkTaskCloseoutEvidence({ ...evidenceRaw, ...patch });
}

const task: WorkTaskCloseoutTaskView = {
	taskId: TASK,
	opRef: OP_REF,
	kind: "code_mutating",
	obligation: "final_admitted",
	terminalReportId: REPORT,
	terminalSettled: true,
	repositoryCommonDir: COMMON_DIR,
	ownerOriginKeys: [OWNER_CHANNEL, OWNER_THREAD],
	authenticatedHumanPrincipals: [MAPPED_HUMAN],
	ownerQuestions: [],
	administrativeUnresolved: false,
};

const review: WorkTaskCloseoutReviewView = {
	taskId: TASK,
	expectedOpRef: OP_REF,
	reportId: REPORT,
	sourceId: REVIEWED,
	contentHash: HASH_REVIEWED,
	fullRead: true,
	disposition: "no_exception",
	closeout: evidence,
};

const sources: readonly WorkTaskCloseoutSourceView[] = [
	{
		sourceId: REVIEWED,
		taskId: TASK,
		editId: null,
		contentHash: HASH_REVIEWED,
		kind: "assignment",
		body: "original assignment text",
		completeness: "complete",
		controlId: null,
		principalId: MAPPED_HUMAN,
		originKey: OWNER_THREAD,
		eventId: "evt-1",
	},
	{
		sourceId: APPROVAL_SOURCE,
		taskId: TASK,
		editId: null,
		contentHash: HASH_APPROVAL,
		kind: "instruction",
		body: approvalBody,
		completeness: "complete",
		controlId: APPROVAL_CONTROL,
		principalId: MAPPED_HUMAN,
		originKey: OWNER_CHANNEL,
		eventId: "evt-2",
	},
	{
		sourceId: SERIAL_SOURCE,
		taskId: TASK,
		editId: null,
		contentHash: HASH_SERIAL,
		kind: "instruction",
		body: serializationBody,
		completeness: "complete",
		controlId: SERIAL_CONTROL,
		principalId: MAPPED_HUMAN,
		originKey: OWNER_CHANNEL,
		eventId: "evt-3",
	},
	{
		sourceId: VERIFY_SOURCE,
		taskId: TASK,
		editId: null,
		contentHash: HASH_VERIFY,
		kind: "decision",
		body: verificationBody,
		completeness: "complete",
		controlId: null,
		principalId: "coordinator:caller",
		originKey: OWNER_THREAD,
		eventId: "evt-4",
	},
];

/** Approval control (control-2) and serialization control (control-1) are distinct records. */
const serialControl: WorkTaskCloseoutControlView = {
	controlId: SERIAL_CONTROL,
	taskId: TASK,
	editId: null,
	opRef: OP_REF,
	sequence: 4,
	principalId: MAPPED_HUMAN,
	body: serializationBody,
	originKey: OWNER_CHANNEL,
	eventId: "evt-3",
};
const approvalControl: WorkTaskCloseoutControlView = {
	controlId: APPROVAL_CONTROL,
	taskId: TASK,
	editId: null,
	opRef: OP_REF,
	sequence: 7,
	principalId: MAPPED_HUMAN,
	body: approvalBody,
	originKey: OWNER_CHANNEL,
	eventId: "evt-2",
};
const controls: readonly WorkTaskCloseoutControlView[] = [serialControl, approvalControl];

function fixture(patch: Partial<WorkTaskCloseoutInput> = {}): WorkTaskCloseoutInput {
	return { task, review, sources, controls, ...patch };
}

function reasons(patch: Partial<WorkTaskCloseoutInput> = {}): readonly string[] {
	return deriveWorkTaskCodingCompletion(fixture(patch)).pendingReasons;
}

function withSource(sourceId: string, patch: Partial<WorkTaskCloseoutSourceView>): WorkTaskCloseoutSourceView[] {
	return sources.map((source) => (source.sourceId === sourceId ? { ...source, ...patch } : source));
}

function withControl(controlId: string, patch: Partial<WorkTaskCloseoutControlView>): WorkTaskCloseoutControlView[] {
	return controls.map((control) => (control.controlId === controlId ? { ...control, ...patch } : control));
}

/**
 * Retained instruction bodies must stay byte-identical to their control record,
 * so statement-level tests update both sides and still exercise the parser.
 */
function withApprovalBody(body: string): Partial<WorkTaskCloseoutInput> {
	return {
		sources: withSource(APPROVAL_SOURCE, { body }),
		controls: withControl(APPROVAL_CONTROL, { body }),
	};
}

test("representative closeout with distinct worker and integrated identities is admitted", () => {
	expect(WORKER_COMMIT).not.toBe(INTEGRATED_COMMIT);
	expect(WORKER_RESULT.baseCommit).not.toBe(TARGET.baseCommit);
	expect(deriveWorkTaskCodingCompletion(fixture())).toEqual({ state: "evidence_admitted", pendingReasons: [] });
});

test("read-only and noncode tasks stay independent of git integration", () => {
	expect(deriveWorkTaskCodingCompletion(fixture({ review: null, sources: [], controls: [] }))).toEqual({
		state: "pending",
		pendingReasons: ["evidence_missing"],
	});
	expect(
		deriveWorkTaskCodingCompletion(
			fixture({ task: { ...task, kind: "read_only" }, review: null, sources: [], controls: [] }),
		),
	).toEqual({ state: "not_applicable", pendingReasons: [] });
});

test("review, result and repository identity mismatches stay pending", () => {
	expect(reasons({ review: { ...review, expectedOpRef: "gw-work-other" } })).toContain("mismatch");
	expect(reasons({ review: { ...review, reportId: "report-other" } })).toContain("mismatch");
	expect(
		reasons({
			review: {
				...review,
				closeout: closeout({
					verification: { ...evidenceRaw.verification, resultingCommit: "3".repeat(40) },
				}),
			},
		}),
	).toContain("mismatch");
	// The worker base is an independent identity: substituting the target base must not admit.
	expect(
		reasons({ review: { ...review, closeout: closeout({ result: { ...WORKER_RESULT, baseCommit: BASE } }) } }),
	).toContain("mismatch");
	expect(reasons({ task: { ...task, repositoryCommonDir: "/srv/other" } })).toContain("repository_mismatch");
	expect(reasons({ task: { ...task, repositoryCommonDir: null } })).toContain("repository_mismatch");
});

test("verification claims bound to the worker commit instead of the integrated result stay pending", () => {
	const workerBoundEvidence = closeout({
		verification: { ...evidenceRaw.verification, resultingCommit: WORKER_COMMIT },
	});
	const workerBoundStatement = JSON.stringify(
		validateWorkTaskCloseoutVerificationStatement({
			...verificationRaw,
			resultingCommit: WORKER_COMMIT,
		}),
	);
	expect(
		reasons({
			review: { ...review, closeout: workerBoundEvidence },
			sources: withSource(VERIFY_SOURCE, { body: workerBoundStatement }),
		}),
	).toContain("mismatch");
});

test("arbitrary complete source bodies cannot certify verification or serialization", () => {
	const fakeVerification = JSON.stringify({ status: "all green", checks: ["lint", "unit", "e2e"] });
	expect(reasons({ sources: withSource(VERIFY_SOURCE, { body: fakeVerification }) })).toContain(
		"verification_unstructured",
	);
	expect(reasons({ sources: withSource(VERIFY_SOURCE, { body: "checks passed, no conflicts" }) })).toContain(
		"verification_unstructured",
	);
	const fakeSerialization = JSON.stringify({ sequence: 4, owner: "anyone" });
	expect(reasons({ sources: withSource(SERIAL_SOURCE, { body: fakeSerialization }) })).toContain(
		"serialization_unstructured",
	);
	expect(reasons({ sources: withSource(SERIAL_SOURCE, { body: "control-1 went first" }) })).toContain(
		"serialization_unstructured",
	);
});

test("verification and serialization statements must match the evidence exactly", () => {
	const driftedChecks = JSON.stringify(
		validateWorkTaskCloseoutVerificationStatement({
			...verificationRaw,
			checks: [{ name: "typecheck", outcome: "pass" }],
		}),
	);
	expect(reasons({ sources: withSource(VERIFY_SOURCE, { body: driftedChecks }) })).toContain("mismatch");
	const driftedBase = JSON.stringify(
		validateWorkTaskCloseoutVerificationStatement({
			...verificationRaw,
			result: { ...WORKER_RESULT, baseCommit: BASE },
		}),
	);
	expect(reasons({ sources: withSource(VERIFY_SOURCE, { body: driftedBase }) })).toContain("mismatch");
	const driftedOrder = JSON.stringify(
		validateWorkTaskCloseoutSerializationStatement({
			...serializationRaw,
			order: { ...serializationRaw.order, baseCommit: WORKER_BASE },
		}),
	);
	expect(reasons({ sources: withSource(SERIAL_SOURCE, { body: driftedOrder }) })).toContain("serialization_mismatch");
});

test("approval provenance accepts only mapped humans or the explicit local owner", () => {
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { editId: "different-edit" }) })).toContain(
		"approval_provenance",
	);
	const persona = "local-ipc:persona:coordinator-session";
	expect(
		reasons({
			task: { ...task, authenticatedHumanPrincipals: [MAPPED_HUMAN, persona] },
			sources: withSource(APPROVAL_SOURCE, { principalId: persona }),
			controls: withControl(APPROVAL_CONTROL, { principalId: persona }),
		}),
	).toContain("approval_provenance");
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { principalId: "local-ipc:persona" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { principalId: "coordinator:caller" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { principalId: "discord-user-stranger" }) })).toContain(
		"approval_provenance",
	);
	expect(
		reasons({
			sources: withSource(APPROVAL_SOURCE, { principalId: "local-ipc:owner" }),
			controls: withControl(APPROVAL_CONTROL, { principalId: "local-ipc:owner" }),
		}),
	).toEqual([]);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { originKey: "discord/thread/999" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { kind: "decision" }) })).toContain("approval_provenance");
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { principalId: "gateway", eventId: "" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { controlId: "control-ghost" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { contentHash: HASH_OTHER }) })).toContain("approval_mismatch");
	expect(reasons({ controls: controls.filter((control) => control.controlId !== APPROVAL_CONTROL) })).toContain(
		"approval_provenance",
	);
});

test("approval source and control must agree on exact body, principal, origin and event", () => {
	// Real body divergence: the authenticated control record still carries the original statement.
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { body: `${approvalBody} ` }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { principalId: "discord-user-other" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { originKey: "discord/thread/444" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { eventId: "evt-other" }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { body: `${approvalBody} ` }) })).toContain(
		"approval_provenance",
	);
	expect(reasons({ controls: withControl(APPROVAL_CONTROL, { opRef: "gw-work-other" }) })).toContain(
		"approval_provenance",
	);
});

test("authenticated malformed approval statements still fail at the parser", () => {
	expect(reasons(withApprovalBody("please merge this, looks good"))).toContain("approval_unstructured");
	expect(reasons(withApprovalBody(JSON.stringify({ approved: true, sha: WORKER_COMMIT })))).toContain(
		"approval_unstructured",
	);
});

test("owner approval statements must match evidence exactly", () => {
	const driftedTarget = JSON.stringify(
		validateWorkTaskOwnerApproval({ ...approvalRaw, target: { ...TARGET, ref: "refs/heads/release" } }),
	);
	expect(reasons(withApprovalBody(driftedTarget))).toContain("approval_mismatch");
	const driftedReviewed = JSON.stringify(
		validateWorkTaskOwnerApproval({ ...approvalRaw, reviewed: { ...approvalRaw.reviewed, reportId: "report-8" } }),
	);
	expect(reasons(withApprovalBody(driftedReviewed))).toContain("approval_mismatch");
	const driftedResult = JSON.stringify(
		validateWorkTaskOwnerApproval({
			...approvalRaw,
			result: { ...WORKER_RESULT, commit: INTEGRATED_COMMIT },
		}),
	);
	expect(reasons(withApprovalBody(driftedResult))).toContain("approval_mismatch");
	const driftedBase = JSON.stringify(
		validateWorkTaskOwnerApproval({ ...approvalRaw, result: { ...WORKER_RESULT, baseCommit: BASE } }),
	);
	expect(reasons(withApprovalBody(driftedBase))).toContain("approval_mismatch");
});

test("owner acceptance of substituted verification or serialization evidence is rejected", () => {
	const substitutedVerification = JSON.stringify(
		validateWorkTaskOwnerApproval({
			...approvalRaw,
			verification: { sourceId: VERIFY_SOURCE, contentHash: HASH_OTHER },
		}),
	);
	expect(reasons(withApprovalBody(substitutedVerification))).toContain("approval_mismatch");
	const substitutedSerialization = JSON.stringify(
		validateWorkTaskOwnerApproval({
			...approvalRaw,
			serialization: { sourceId: SERIAL_SOURCE, contentHash: HASH_OTHER },
		}),
	);
	expect(reasons(withApprovalBody(substitutedSerialization))).toContain("approval_mismatch");
	const substitutedSource = JSON.stringify(
		validateWorkTaskOwnerApproval({
			...approvalRaw,
			verification: { sourceId: "checkpoint-verify-2", contentHash: HASH_VERIFY },
		}),
	);
	expect(reasons(withApprovalBody(substitutedSource))).toContain("approval_mismatch");
});

test("newer owner decisions supersede old approval even with incomplete retained evidence", () => {
	const newerControl = { ...approvalControl, controlId: "control-3", sequence: 8, eventId: "evt-5" };
	for (const completeness of ["complete", "incomplete"] as const) {
		const newerSource = {
			...sources[1]!,
			sourceId: "instruction-control-3",
			controlId: newerControl.controlId,
			eventId: newerControl.eventId,
			contentHash: HASH_OTHER,
			completeness,
		};
		expect(
			reasons({
				sources: [...sources, newerSource],
				controls: [...controls, newerControl],
			}),
		).toContain("approval_mismatch");
		// Retaining an older control later does not reverse the actual owner-control order.
		expect(
			deriveWorkTaskCodingCompletion(
				fixture({
					sources: [...sources, newerSource],
					controls: [...controls, { ...newerControl, sequence: 6 }],
				}),
			).state,
		).toBe("evidence_admitted");
	}
});

test("incomplete evidence stays pending", () => {
	expect(reasons({ review: null })).toEqual(["evidence_missing"]);
	expect(reasons({ review: { ...review, fullRead: false } })).toContain("evidence_incomplete");
	expect(reasons({ sources: sources.filter((source) => source.sourceId !== REVIEWED) })).toContain(
		"evidence_incomplete",
	);
	expect(reasons({ sources: withSource(REVIEWED, { completeness: "incomplete" }) })).toContain("evidence_incomplete");
	expect(reasons({ sources: withSource(APPROVAL_SOURCE, { completeness: "incomplete" }) })).toContain(
		"evidence_incomplete",
	);
	expect(reasons({ sources: sources.filter((source) => source.sourceId !== VERIFY_SOURCE) })).toContain(
		"verification_incomplete",
	);
	expect(reasons({ sources: sources.filter((source) => source.sourceId !== APPROVAL_SOURCE) })).toContain(
		"approval_missing",
	);
	const noTarget = { ...evidenceRaw } as Record<string, unknown>;
	delete noTarget.target;
	expect(reasons({ review: { ...review, closeout: noTarget as unknown as WorkTaskCloseoutEvidence } })).toContain(
		"target_missing",
	);
	const emptyChecks = {
		...evidenceRaw,
		verification: { ...evidenceRaw.verification, checks: [] },
	} as unknown as WorkTaskCloseoutEvidence;
	expect(reasons({ review: { ...review, closeout: emptyChecks } })).toContain("verification_incomplete");
});

test("conflicts and failed checks stay pending", () => {
	const conflictedEvidence = closeout({
		verification: { ...evidenceRaw.verification, conflicts: "detected" },
	});
	const conflictedStatement = JSON.stringify(
		validateWorkTaskCloseoutVerificationStatement({ ...verificationRaw, conflicts: "detected" }),
	);
	expect(
		reasons({
			review: { ...review, closeout: conflictedEvidence },
			sources: withSource(VERIFY_SOURCE, { body: conflictedStatement }),
		}),
	).toContain("conflicts");
	const failedEvidence = closeout({
		verification: { ...evidenceRaw.verification, checks: [{ name: "typecheck", outcome: "fail" }] },
	});
	const failedStatement = JSON.stringify(
		validateWorkTaskCloseoutVerificationStatement({
			...verificationRaw,
			checks: [{ name: "typecheck", outcome: "fail" }],
		}),
	);
	expect(
		reasons({
			review: { ...review, closeout: failedEvidence },
			sources: withSource(VERIFY_SOURCE, { body: failedStatement }),
		}),
	).toContain("failed_checks");
	const emptyChecks = {
		...evidenceRaw,
		verification: { ...evidenceRaw.verification, checks: [] },
	} as unknown as WorkTaskCloseoutEvidence;
	expect(reasons({ review: { ...review, closeout: emptyChecks } })).toContain("verification_incomplete");
});

test("uncertain terminal identity and unresolved administration stay pending", () => {
	expect(reasons({ task: { ...task, terminalSettled: false } })).toContain("terminal_uncertain");
	expect(reasons({ task: { ...task, administrativeUnresolved: true } })).toContain("administrative_unresolved");
	expect(reasons({ task: { ...task, obligation: "held" } })).toEqual(
		expect.arrayContaining(["report_not_admitted", "administrative_unresolved"]),
	);
	expect(reasons({ task: { ...task, obligation: "awaiting_final" } })).toContain("report_not_admitted");
});

test("serialization ownership and order must match retained records and statements", () => {
	expect(reasons({ controls: [] })).toContain("serialization_mismatch");
	expect(reasons({ controls: withControl(SERIAL_CONTROL, { sequence: 5 }) })).toContain("serialization_mismatch");
	expect(reasons({ controls: withControl(SERIAL_CONTROL, { opRef: "gw-work-other" }) })).toContain(
		"serialization_mismatch",
	);
	expect(reasons({ sources: withSource(SERIAL_SOURCE, { controlId: null }) })).toContain("serialization_mismatch");
	expect(reasons({ sources: sources.filter((source) => source.sourceId !== SERIAL_SOURCE) })).toContain(
		"serialization_mismatch",
	);
	const missingOwner = JSON.stringify(
		validateWorkTaskCloseoutSerializationStatement({ ...serializationRaw, integrationOwner: "someone" }),
	);
	expect(
		reasons({
			sources: withSource(SERIAL_SOURCE, { body: missingOwner }),
			controls: withControl(SERIAL_CONTROL, { body: missingOwner }),
		}),
	).toContain("serialization_mismatch");
	expect(reasons({ sources: withSource(SERIAL_SOURCE, { kind: "decision" }) })).toContain("serialization_mismatch");
});

test("open owner questions keep completion pending", () => {
	expect(reasons({ review: { ...review, disposition: "owner_question" } })).toContain("owner_question");
});

test("only exact authenticated owner answers resolve retained closeout questions", () => {
	const question = { reviewId: REVIEW_ID, sourceId: `review-${REVIEW_ID}`, contentHash: HASH_OTHER };
	const asked = { ...task, ownerQuestions: [question] };
	expect(reasons({ task: asked })).toContain("owner_question");
	const answered = (contentHash: string, resolution: "resolved" | "unresolved" = "resolved") =>
		JSON.stringify(
			validateWorkTaskOwnerApproval({
				...approvalRaw,
				answers: [
					{
						...question,
						contentHash,
						answer: resolution === "resolved" ? "Use the reviewed target and checks." : "I haven't decided.",
						resolution,
					},
				],
			}),
		);
	expect(reasons({ task: asked, ...withApprovalBody(answered(HASH_OTHER)) })).toEqual([]);
	expect(reasons({ task: asked, ...withApprovalBody(answered(HASH_OTHER, "unresolved")) })).toContain("owner_question");
	expect(() =>
		validateWorkTaskOwnerApproval({ ...approvalRaw, answers: [{ ...question, answer: "I haven't decided." }] }),
	).toThrow("explicit owner resolution required");
	expect(reasons({ task: asked, ...withApprovalBody(answered(HASH_REVIEWED)) })).toContain("owner_question");
	expect(reasons({ task: asked, ...withApprovalBody(answered(HASH_REVIEWED)) })).toContain("approval_mismatch");
	expect(reasons(withApprovalBody(answered(HASH_OTHER)))).toContain("approval_mismatch");
});

test("protocol review and statement validation stay strict", () => {
	const params: WorkTaskReviewParams = {
		taskId: TASK,
		expectedOpRef: OP_REF,
		reportId: REPORT,
		sourceId: REVIEWED,
		contentHash: HASH_REVIEWED,
		reviewId: REVIEW_ID,
		expectedReviewId: null,
		callerSessionId: "coordinator-session",
		callerEpoch: 3,
		fullRead: true,
		disposition: "no_exception",
		rationale: "exact original read",
	};
	expect(validateWorkTaskReviewParams({ ...params })).toEqual(params);
	expect(validateWorkTaskReviewParams({ ...params, closeout: evidence })).toEqual({ ...params, closeout: evidence });
	expect(() => validateWorkTaskReviewParams({ ...params, closeout: { ...evidenceRaw, surprise: true } })).toThrow();
	expect(() =>
		validateWorkTaskReviewParams({
			...params,
			closeout: { ...evidenceRaw, result: { commit: WORKER_COMMIT, diffHash: "xyz" } },
		}),
	).toThrow();
	expect(() =>
		validateWorkTaskCloseoutEvidence({
			...evidenceRaw,
			result: { commit: WORKER_COMMIT, diffHash: DIFF },
		}),
	).toThrow();
	expect(() => validateWorkTaskCloseoutEvidence({ ...evidenceRaw, action: "merge" })).toThrow();
	expect(() => validateWorkTaskCloseoutEvidence({ ...evidenceRaw, target: { ...TARGET, ref: "main" } })).toThrow();
	expect(() =>
		validateWorkTaskCloseoutEvidence({ ...evidenceRaw, verification: { ...evidenceRaw.verification, checks: [] } }),
	).toThrow();
	expect(() =>
		validateWorkTaskOwnerApproval({ kind: "work_task_closeout_approval", taskId: TASK, opRef: OP_REF }),
	).toThrow();
	expect(() =>
		validateWorkTaskOwnerApproval({ ...approvalRaw, result: { commit: WORKER_COMMIT, diffHash: DIFF } }),
	).toThrow();
	const { verification: _verification, ...withoutVerification } = approvalRaw;
	expect(() => validateWorkTaskOwnerApproval(withoutVerification)).toThrow();
	const { serialization: _serialization, ...withoutSerialization } = approvalRaw;
	expect(() => validateWorkTaskOwnerApproval(withoutSerialization)).toThrow();
	expect(() => validateWorkTaskCloseoutVerificationStatement({ ...verificationRaw, extra: true })).toThrow();
	expect(() => validateWorkTaskCloseoutVerificationStatement({ kind: "work_task_closeout_verification" })).toThrow();
	expect(() =>
		validateWorkTaskCloseoutSerializationStatement({ kind: "work_task_closeout_serialization", taskId: TASK }),
	).toThrow();
});

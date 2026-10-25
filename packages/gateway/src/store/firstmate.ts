import { execFileSync } from "node:child_process";
import { realpathSync } from "node:fs";
import { isAbsolute } from "node:path";
import type { FirstmateReleaseReceipt, FirstmateResultIdentity } from "@gajae-gateway/protocol";
import { type OriginRef, originKey, ProtocolError, validateOriginRef } from "@gajae-gateway/protocol";

export type {
	FirstmateApproval,
	FirstmateOutcomeApproval,
	FirstmateReleaseIntent,
	FirstmateReleaseReceipt,
} from "@gajae-gateway/protocol";

/** Trusted policy input, not public work.start authority or another task lifecycle. */
export interface FirstmateAssignment {
	readonly taskId: string;
	readonly name: string;
	readonly ownerId: string;
	readonly messageId: string;
	readonly thread: OriginRef;
	readonly mandate: {
		readonly id: string;
		readonly text: string;
		readonly checkRetries: boolean;
		readonly commit: boolean;
		readonly push: boolean;
	};
	readonly worktree: {
		readonly path: string;
		readonly commonDir: string;
		/** Linked Git directory identifies registration; HEAD may advance. */
		readonly gitDir: string;
		readonly branch: string;
	};
}

export type FirstmateCause =
	| { readonly kind: "assignment" }
	| {
			readonly kind: "correction";
			readonly priorOpRef: string;
			readonly ownerId: string;
			readonly messageId: string;
			readonly mandateId: string;
	  }
	| {
			readonly kind: "checks";
			readonly priorOpRef: string;
			readonly prNumber: number;
			readonly headSha: string;
			readonly failingChecks: readonly string[];
			readonly retryOrdinal: number;
	  };

export interface FirstmatePrepare {
	readonly assignment: FirstmateAssignment;
	readonly cause: FirstmateCause;
	readonly text: string;
}

export interface FirstmateAttempt extends FirstmateResultIdentity {
	readonly cause: FirstmateCause;
	readonly text: string;
	readonly preparedAt: string;
}

/** Narrow read contract for the existing FirstmateStore owner in groups/work.ts. */
export interface FirstmateCloseoutReaders {
	/** All immutable preparations for exactly this task, ordered by ascending ordinal. */
	attemptsForTask(taskId: string): readonly FirstmateAttempt[];
	/**
	 * Completed receipt only, matched by all three fields against its request identity.
	 * Covers earlier corrected attempts in the same released session; a pending intent
	 * never supplies coverage. No live binding check or native read belongs in this reader.
	 */
	completedReleaseForSession(taskId: string, sessionId: string, epoch: number): FirstmateReleaseReceipt | undefined;
}

export function firstmateAssert(condition: unknown, reason: string): asserts condition {
	if (!condition) {
		throw new ProtocolError("invalid_params", "FirstMate admission refused", {
			reasonCode: "firstmate_admission_refused",
			reason,
		});
	}
}

export function identityText(value: unknown): value is string {
	if (typeof value !== "string" || value.length === 0) return false;
	for (let index = 0; index < value.length; index++) {
		const code = value.charCodeAt(index);
		if (code <= 0x1f || code === 0x7f) return false;
	}
	return true;
}

/** Stable field order compares immutable policy, not caller property insertion order. */
export function assignmentJson(value: FirstmateAssignment): string {
	firstmateAssert(value && typeof value === "object", "assignment_missing");
	firstmateAssert(
		identityText(value.taskId) && identityText(value.ownerId) && identityText(value.messageId),
		"identity_missing",
	);
	firstmateAssert(
		typeof value.name === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u.test(value.name),
		"lane_invalid",
	);
	validateOriginRef(value.thread);
	firstmateAssert(value.thread.platform === "discord" && value.thread.kind === "thread", "thread_required");
	const { mandate, worktree } = value;
	firstmateAssert(
		mandate && identityText(mandate.id) && typeof mandate.text === "string" && mandate.text.length > 0,
		"mandate_missing",
	);
	firstmateAssert(
		[mandate.checkRetries, mandate.commit, mandate.push].every((flag) => typeof flag === "boolean"),
		"authority_missing",
	);
	firstmateAssert(
		worktree &&
			[worktree.path, worktree.commonDir, worktree.gitDir].every((path) => identityText(path) && isAbsolute(path)),
		"worktree_missing",
	);
	firstmateAssert(identityText(worktree.branch), "branch_missing");
	return JSON.stringify({
		taskId: value.taskId,
		name: value.name,
		ownerId: value.ownerId,
		messageId: value.messageId,
		thread: {
			platform: value.thread.platform,
			kind: value.thread.kind,
			conversationId: value.thread.conversationId,
			parentId: value.thread.parentId,
			...(value.thread.threadVisibility === undefined ? {} : { threadVisibility: value.thread.threadVisibility }),
		},
		mandate: {
			id: mandate.id,
			text: mandate.text,
			checkRetries: mandate.checkRetries,
			commit: mandate.commit,
			push: mandate.push,
		},
		worktree: {
			path: worktree.path,
			commonDir: worktree.commonDir,
			gitDir: worktree.gitDir,
			branch: worktree.branch,
		},
	});
}

export function assertCause(cause: FirstmateCause, assignment?: FirstmateAssignment): void {
	firstmateAssert(cause && typeof cause === "object", "cause_missing");
	if (cause.kind === "assignment") return;
	if (cause.kind === "correction") {
		firstmateAssert(
			[cause.priorOpRef, cause.ownerId, cause.messageId, cause.mandateId].every(identityText),
			"correction_cause_invalid",
		);
		if (assignment) {
			firstmateAssert(
				cause.ownerId === assignment.ownerId &&
					cause.mandateId === assignment.mandate.id &&
					cause.messageId !== assignment.messageId,
				"correction_authority_mismatch",
			);
		}
		return;
	}
	firstmateAssert(cause.kind === "checks", "cause_invalid");
	firstmateAssert(
		identityText(cause.priorOpRef) &&
			identityText(cause.headSha) &&
			Number.isSafeInteger(cause.prNumber) &&
			cause.prNumber > 0 &&
			Number.isSafeInteger(cause.retryOrdinal) &&
			cause.retryOrdinal > 0 &&
			Array.isArray(cause.failingChecks) &&
			cause.failingChecks.length > 0 &&
			cause.failingChecks.every(identityText),
		"retry_cause_invalid",
	);
}

/** Read-only admission checks, not a sandbox or a cross-process path-swap guarantee. */
export function assertFirstmateWorktree(assignment: FirstmateAssignment): void {
	assignmentJson(assignment);
	const expected = assignment.worktree;
	try {
		for (const path of [expected.path, expected.commonDir, expected.gitDir]) {
			firstmateAssert(realpathSync(path) === path, "noncanonical_path");
		}
		const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_")));
		const git = (...args: string[]) =>
			execFileSync("git", ["-C", expected.path, ...args], {
				encoding: "utf8",
				timeout: 5_000,
				stdio: ["ignore", "pipe", "pipe"],
				env: { ...env, GIT_OPTIONAL_LOCKS: "0" },
			}).replace(/\n$/u, "");
		firstmateAssert(realpathSync(git("rev-parse", "--show-toplevel")) === expected.path, "worktree_root_changed");
		firstmateAssert(
			realpathSync(git("rev-parse", "--path-format=absolute", "--git-common-dir")) === expected.commonDir,
			"common_dir_changed",
		);
		firstmateAssert(realpathSync(git("rev-parse", "--absolute-git-dir")) === expected.gitDir, "registration_changed");
		firstmateAssert(expected.gitDir !== expected.commonDir, "dedicated_linked_worktree_required");
		firstmateAssert(git("symbolic-ref", "--short", "HEAD") === expected.branch, "branch_changed");
		const entries = git("worktree", "list", "--porcelain", "-z")
			.split("\0\0")
			.map((entry) => entry.split("\0"));
		const registered = entries.filter((entry) => entry[0] === `worktree ${expected.path}`);
		firstmateAssert(
			registered.length === 1 && registered[0]!.includes(`branch refs/heads/${expected.branch}`),
			"registration_missing",
		);
		firstmateAssert(
			entries.filter((entry) => entry.includes(`branch refs/heads/${expected.branch}`)).length === 1,
			"branch_not_dedicated",
		);
	} catch (error) {
		if (error instanceof ProtocolError) throw error;
		firstmateAssert(false, "worktree_unavailable");
	}
}

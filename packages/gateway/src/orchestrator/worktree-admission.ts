import { readFileSync, realpathSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";

export interface DedicatedWorktreeProof {
	readonly requestedCwd: string;
	readonly requestedCoordinator: string;
	readonly cwd: string;
	readonly coordinator: string;
	readonly primary: string;
	readonly commonDir: string;
	readonly gitDir: string;
}

/**
 * Git admission only: no provisioning, ownership transfer or filesystem sandbox.
 * Placement is not a fence: a distinct registered linked checkout may nest under
 * the primary or the coordinator area (GJC default `<source>/.worktrees/<name>`).
 */
export function admitDedicatedWorktree(cwd: string, coordinatorCwd: string): DedicatedWorktreeProof {
	try {
		if (!isAbsolute(cwd) || !isAbsolute(coordinatorCwd)) throw new Error();
		const target = realpathSync(cwd);
		const coordinator = realpathSync(coordinatorCwd);
		const git = (dir: string, ...args: string[]): string => {
			const result = Bun.spawnSync(["git", "-C", dir, ...args], {
				stdout: "pipe",
				stderr: "pipe",
				// Repository-selection environment cannot override the inspected directory.
				env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_"))),
			});
			if (result.exitCode !== 0) throw new Error();
			return result.stdout.toString().trim();
		};
		if (realpathSync(git(target, "rev-parse", "--show-toplevel")) !== target) throw new Error();
		const commonDir = realpathSync(resolve(target, git(target, "rev-parse", "--git-common-dir")));
		const gitDir = realpathSync(resolve(target, git(target, "rev-parse", "--git-dir")));
		if (
			commonDir === gitDir ||
			dirname(dirname(gitDir)) !== commonDir ||
			dirname(gitDir) !== resolve(commonDir, "worktrees")
		)
			throw new Error();
		const records = git(target, "worktree", "list", "--porcelain", "-z").split("\0\0").filter(Boolean);
		// Unrelated registrations may be missing or prunable; only the selected record's integrity gates this target.
		const registrations = records.map((record) => {
			const fields = record.split("\0");
			const field = fields.find((item) => item.startsWith("worktree "));
			if (!field) throw new Error();
			return {
				root: registeredRoot(field.slice(9)),
				prunable: fields.some((item) => item.startsWith("prunable")),
			};
		});
		// Exactly one intact registration must name the canonical selected root.
		const selected = registrations.filter((registration) => registration.root === target);
		if (selected.length !== 1 || selected[0].prunable) throw new Error();
		const primary = registrations[0]?.root;
		// The primary checkout itself never admits as worker.
		if (!primary || target === primary) throw new Error();
		// Coordinator checkout identity: a Git coordinator is refused only when its
		// canonical root IS this worker — root, subdirectory and symlink forms all
		// resolve there — while a distinct nested checkout keeps its own identity.
		// A coordinator outside every work tree falls back to realpath containment.
		let coordinatorRoot: string | undefined;
		try {
			coordinatorRoot = realpathSync(git(coordinator, "rev-parse", "--show-toplevel"));
		} catch {
			coordinatorRoot = undefined;
		}
		if (coordinatorRoot === undefined ? contains(target, coordinator) : coordinatorRoot === target) throw new Error();
		// The linked administrative directory must point back to this exact worktree.
		if (realpathSync(readFileSync(resolve(gitDir, "gitdir"), "utf8").trim()) !== realpathSync(resolve(target, ".git")))
			throw new Error();
		return {
			requestedCwd: cwd,
			requestedCoordinator: coordinatorCwd,
			cwd: target,
			coordinator,
			primary,
			commonDir,
			gitDir,
		};
	} catch {
		throw new Error("dedicated_worktree_required");
	}
}

/** True when child resolves to parent or sits beneath it. */
function contains(parent: string, child: string): boolean {
	const path = relative(parent, child);
	return path === "" || (!path.startsWith(`..${sep}`) && path !== ".." && !isAbsolute(path));
}

/** Unresolvable registration paths (vanished directories) cannot name the selected root. */
function registeredRoot(registered: string): string | undefined {
	try {
		return realpathSync(registered);
	} catch {
		return undefined;
	}
}

/** Re-read Git registration and realpaths immediately before transport; fail closed on drift. */
export function revalidateDedicatedWorktree(proof: DedicatedWorktreeProof): void {
	const current = admitDedicatedWorktree(proof.requestedCwd, proof.requestedCoordinator);
	if (JSON.stringify(current) !== JSON.stringify(proof)) throw new Error("dedicated_worktree_changed");
}

/** Revalidate the native nested-bucket ignore invariant without prescribing its placement. */
export function assertManagedWorktreeIgnored(proof: DedicatedWorktreeProof): void {
	if (!contains(proof.primary, proof.cwd)) return;
	const result = Bun.spawnSync(
		[
			"git",
			"-C",
			proof.primary,
			"check-ignore",
			"--quiet",
			"--no-index",
			"--",
			relative(proof.primary, dirname(proof.cwd)),
		],
		{
			stdout: "pipe",
			stderr: "pipe",
			env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_"))),
		},
	);
	if (result.exitCode !== 0) throw new Error("managed_worktree_bucket_not_ignored");
}

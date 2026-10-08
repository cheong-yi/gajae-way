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

/** Git admission only: no provisioning, ownership transfer or filesystem sandbox. */
export function admitDedicatedWorktree(cwd: string, coordinatorCwd: string): DedicatedWorktreeProof {
	try {
		if (!isAbsolute(cwd) || !isAbsolute(coordinatorCwd)) throw new Error();
		const target = realpathSync(cwd);
		const coordinator = realpathSync(coordinatorCwd);
		const git = (...args: string[]): string => {
			const result = Bun.spawnSync(["git", "-C", target, ...args], {
				stdout: "pipe",
				stderr: "pipe",
				// Repository-selection environment cannot override the inspected directory.
				env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("GIT_"))),
			});
			if (result.exitCode !== 0) throw new Error();
			return result.stdout.toString().trim();
		};
		if (realpathSync(git("rev-parse", "--show-toplevel")) !== target) throw new Error();
		const commonDir = realpathSync(resolve(target, git("rev-parse", "--git-common-dir")));
		const gitDir = realpathSync(resolve(target, git("rev-parse", "--git-dir")));
		if (
			commonDir === gitDir ||
			dirname(dirname(gitDir)) !== commonDir ||
			dirname(gitDir) !== resolve(commonDir, "worktrees")
		)
			throw new Error();
		const records = git("worktree", "list", "--porcelain", "-z").split("\0\0").filter(Boolean);
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
		if (!primary || target === primary || overlaps(target, coordinator) || overlaps(target, primary)) throw new Error();
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

function overlaps(a: string, b: string): boolean {
	const inside = (parent: string, child: string) => {
		const path = relative(parent, child);
		return path === "" || (!path.startsWith(`..${sep}`) && path !== ".." && !isAbsolute(path));
	};
	return inside(a, b) || inside(b, a);
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

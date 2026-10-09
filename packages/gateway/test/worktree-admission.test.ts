import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	admitDedicatedWorktree,
	assertManagedWorktreeIgnored,
	revalidateDedicatedWorktree,
} from "../src/orchestrator/worktree-admission";

const directories: string[] = [];
afterEach(async () => {
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});
function git(cwd: string, ...args: string[]): void {
	const result = Bun.spawnSync(["git", "-C", cwd, ...args], { stdout: "pipe", stderr: "pipe" });
	if (result.exitCode !== 0) throw new Error(result.stderr.toString());
}
async function fixture() {
	const root = await mkdtemp(join(tmpdir(), "firstmate-worktree-"));
	directories.push(root);
	const primary = join(root, "primary");
	const worker = join(root, "worker");
	await mkdir(primary);
	git(primary, "init");
	git(
		primary,
		"-c",
		"user.name=Fixture",
		"-c",
		"user.email=fixture@example.invalid",
		"commit",
		"--allow-empty",
		"-m",
		"fixture",
	);
	git(primary, "worktree", "add", "-b", "worker", worker);
	return { root, primary, worker };
}

test("registered linked worktree admits without changing Git identity or branch", async () => {
	const f = await fixture();
	const proof = admitDedicatedWorktree(f.worker, f.primary);
	expect(proof.cwd).toBe(f.worker);
	expect(proof.primary).toBe(f.primary);
	expect(() => revalidateDedicatedWorktree(proof)).not.toThrow();
	const branch = Bun.spawnSync(["git", "-C", f.worker, "branch", "--show-current"], { stdout: "pipe" });
	expect(branch.stdout.toString().trim()).toBe("worker");
});

test("primary, shared, non-Git and subtree directories fail admission", async () => {
	const f = await fixture();
	const child = join(f.worker, "subdir");
	await mkdir(child);
	expect(() => admitDedicatedWorktree(f.primary, f.worker)).toThrow();
	expect(() => admitDedicatedWorktree(f.worker, f.worker)).toThrow();
	expect(() => admitDedicatedWorktree(f.root, f.primary)).toThrow();
	expect(() => admitDedicatedWorktree(child, f.primary)).toThrow();
});

test("symlink aliases cannot disguise a shared execution area", async () => {
	const f = await fixture();
	const alias = join(f.root, "alias");
	await symlink(f.worker, alias);
	expect(() => admitDedicatedWorktree(alias, f.worker)).toThrow();
});

test("unregistered moved checkout and stale registration proof fail closed", async () => {
	const f = await fixture();
	const proof = admitDedicatedWorktree(f.worker, f.primary);
	const moved = join(f.root, "moved");
	await rename(f.worker, moved);
	expect(() => admitDedicatedWorktree(moved, f.primary)).toThrow();
	expect(() => revalidateDedicatedWorktree(proof)).toThrow();
});

test("coordinator nested within worker cannot claim dedicated isolation", async () => {
	const f = await fixture();
	const coordinator = join(f.worker, "coordinator");
	await mkdir(coordinator);
	expect(() => admitDedicatedWorktree(f.worker, coordinator)).toThrow();
});

test("missing or prunable sibling worktree cannot block a valid target", async () => {
	const f = await fixture();
	const other = join(f.root, "other");
	git(f.primary, "worktree", "add", "-b", "other", other);
	await rm(other, { recursive: true, force: true });
	const listing = Bun.spawnSync(["git", "-C", f.worker, "worktree", "list", "--porcelain"], { stdout: "pipe" });
	expect(listing.stdout.toString()).toContain("prunable");
	const proof = admitDedicatedWorktree(f.worker, f.primary);
	expect(proof.cwd).toBe(f.worker);
	expect(() => revalidateDedicatedWorktree(proof)).not.toThrow();
});

test("selected registration drifting to a missing root fails closed", async () => {
	const f = await fixture();
	await writeFile(join(f.primary, ".git", "worktrees", "worker", "gitdir"), join(f.root, "gone", ".git"));
	expect(() => admitDedicatedWorktree(f.worker, f.primary)).toThrow();
});

test("administrative backpointer forged to another worktree fails closed", async () => {
	const f = await fixture();
	const other = join(f.root, "other");
	git(f.primary, "worktree", "add", "-b", "other", other);
	await writeFile(join(f.worker, ".git"), `gitdir: ${join(f.primary, ".git", "worktrees", "other")}`);
	expect(() => admitDedicatedWorktree(f.worker, f.primary)).toThrow();
});

test("two registrations naming the selected root fail admission", async () => {
	const f = await fixture();
	const other = join(f.root, "other");
	git(f.primary, "worktree", "add", "-b", "other", other);
	await writeFile(join(f.primary, ".git", "worktrees", "other", "gitdir"), join(f.worker, ".git"));
	expect(() => admitDedicatedWorktree(f.worker, f.primary)).toThrow();
});

test("native nested default placement admits a distinct registered linked checkout", async () => {
	const f = await fixture();
	const nested = join(f.primary, ".worktrees", "native");
	git(f.primary, "worktree", "add", "-b", "native", nested);
	const proof = admitDedicatedWorktree(nested, f.primary);
	expect(proof.cwd).toBe(nested);
	expect(proof.primary).toBe(f.primary);
	expect(proof.commonDir).toBe(join(f.primary, ".git"));
	expect(() => revalidateDedicatedWorktree(proof)).not.toThrow();
	// A plain enclosing coordinator directory is accepted placement as well.
	expect(admitDedicatedWorktree(nested, f.root).cwd).toBe(nested);
});

test("coordinator symlink alias and primary subdirectory cannot be admitted as worker", async () => {
	const f = await fixture();
	const alias = join(f.root, "coordinator-alias");
	await symlink(f.worker, alias);
	expect(() => admitDedicatedWorktree(f.worker, alias)).toThrow();
	await mkdir(join(f.primary, "coordinator"));
	expect(() => admitDedicatedWorktree(f.primary, join(f.primary, "coordinator"))).toThrow();
});

test("ordinary nested folder inside the source checkout fails admission", async () => {
	const f = await fixture();
	const plain = join(f.primary, ".worktrees", "plain");
	await mkdir(plain, { recursive: true });
	expect(() => admitDedicatedWorktree(plain, f.primary)).toThrow();
	expect(() => admitDedicatedWorktree(plain, f.root)).toThrow();
});

test("nested distinct linked coordinator checkout admits its containing worker", async () => {
	const f = await fixture();
	const nestedCoordinator = join(f.worker, "coordinator-checkout");
	git(f.primary, "worktree", "add", "-b", "coordinator", nestedCoordinator);
	const proof = admitDedicatedWorktree(f.worker, nestedCoordinator);
	expect(proof.cwd).toBe(f.worker);
	expect(() => revalidateDedicatedWorktree(proof)).not.toThrow();
});

test("coordinator subdirectory of the same checkout fails admission", async () => {
	const f = await fixture();
	const subdirectory = join(f.worker, "coordinator", "runner");
	await mkdir(subdirectory, { recursive: true });
	expect(() => admitDedicatedWorktree(f.worker, subdirectory)).toThrow();
});

test("native nested ignore invariant detects drift without requiring a sibling placement policy", async () => {
	const f = await fixture();
	const nested = join(f.primary, ".worktrees", "native");
	git(f.primary, "worktree", "add", "-b", "native", nested);
	const proof = admitDedicatedWorktree(nested, f.primary);
	expect(() => assertManagedWorktreeIgnored(proof)).toThrow("managed_worktree_bucket_not_ignored");
	await writeFile(join(f.primary, ".gitignore"), "/.worktrees/native/\n");
	expect(() => assertManagedWorktreeIgnored(proof)).toThrow("managed_worktree_bucket_not_ignored");
	await writeFile(join(f.primary, ".gitignore"), "/.worktrees\n");
	expect(() => assertManagedWorktreeIgnored(proof)).not.toThrow();
	await writeFile(join(f.primary, ".gitignore"), "");
	expect(() => assertManagedWorktreeIgnored(proof)).toThrow("managed_worktree_bucket_not_ignored");
	expect(() => assertManagedWorktreeIgnored(admitDedicatedWorktree(f.worker, f.primary))).not.toThrow();
});

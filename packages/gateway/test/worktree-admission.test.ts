import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rename, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { admitDedicatedWorktree, revalidateDedicatedWorktree } from "../src/orchestrator/worktree-admission";

const directories: string[] = [];
afterEach(async () => { for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true }); });
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
	git(primary, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "fixture");
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

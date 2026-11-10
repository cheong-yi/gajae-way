import { expect, test } from "bun:test";
import { access, mkdir, mkdtemp, readdir, readFile, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { type GjcInstallRunner, installPinnedGjc } from "../src/gjc-install";
import laneSwitchPatch from "../src/patches/gajae-code/0.19.3-lane-switch.patch" with { type: "text" };

async function home(): Promise<string> {
	return await mkdtemp(join(tmpdir(), "gajaeway-gjc-install-"));
}

// The patch is authored against the installed npm layout. `v0.19.3` in this
// repository holds the pristine sources the installer patches.
const GJC_019_REPO =
	process.env.GAJAEWAY_GJC_0_19_3_REPO ?? "/home/cheongnet/gajaeway/state/workspace/gjc-lane-switch-0.19.3";
const GJC_019_SOURCES: ReadonlyArray<readonly [source: string, installed: string]> = [
	["packages/agent/src/agent-loop.ts", "agent-core/src/agent-loop.ts"],
	["packages/agent/src/agent.ts", "agent-core/src/agent.ts"],
	["packages/agent/src/types.ts", "agent-core/src/types.ts"],
	["packages/coding-agent/src/session/agent-session.ts", "coding-agent/src/session/agent-session.ts"],
];

async function stageLaneSwitchSources(directory: string): Promise<void> {
	for (const [source, installed] of GJC_019_SOURCES) {
		const result = Bun.spawnSync(["git", "-C", GJC_019_REPO, "show", `v0.19.3:${source}`], {
			stdout: "pipe",
			stderr: "pipe",
		});
		if (result.exitCode !== 0) throw new Error(`git show v0.19.3:${source} failed: ${result.stderr.toString().trim()}`);
		const destination = join(directory, "node_modules", "@gajae-code", installed);
		await mkdir(dirname(destination), { recursive: true });
		await writeFile(destination, result.stdout);
	}
}

async function applyLaneSwitchPatch(directory: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
	const patchPath = join(directory, "0.19.3-lane-switch.patch");
	await writeFile(patchPath, laneSwitchPatch);
	const child = Bun.spawn(
		[
			"patch",
			"--batch",
			"--forward",
			"--fuzz=0",
			"--strip=1",
			"--directory=node_modules/@gajae-code",
			"--input",
			patchPath,
		],
		{ cwd: directory, stdin: "ignore", stdout: "pipe", stderr: "pipe" },
	);
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	return { exitCode, stdout, stderr };
}

function installRunner(): { runner: GjcInstallRunner; commands: string[][]; cwds: string[] } {
	const commands: string[][] = [];
	const cwds: string[] = [];
	const runner: GjcInstallRunner = async (command, cwd) => {
		commands.push([...command]);
		cwds.push(cwd);
		if (command[0] === "bun") {
			const binDir = join(cwd, "node_modules", ".bin");
			await mkdir(binDir, { recursive: true });
			await writeFile(join(binDir, "gjc"), "gjc shim");
			return { exitCode: 0, stdout: "", stderr: "" };
		}
		if (command[0] === "patch") {
			// The install stages the embedded patch as a real file before applying it.
			await access(command[command.length - 1] ?? "");
			return { exitCode: 0, stdout: "", stderr: "" };
		}
		const packageJson = JSON.parse(await readFile(join(cwd, "package.json"), "utf8")) as {
			dependencies: { "gajae-code": string };
		};
		return { exitCode: 0, stdout: `gjc/${packageJson.dependencies["gajae-code"]}`, stderr: "" };
	};
	return { runner, commands, cwds };
}

test("installPinnedGjc installs in a versioned prefix, verifies it, and flips current", async () => {
	const directory = await home();
	const { runner, commands } = installRunner();
	try {
		const executable = await installPinnedGjc({ home: directory, version: "0.18.8", runner });
		const layout = homeLayout(directory);
		expect(executable).toBe(layout.gjcExecutable());
		expect(await readlink(layout.gjcCurrent)).toBe("0.18.8");
		expect(await readlink(layout.gjcExecutable("0.18.8"))).toBe("../node_modules/.bin/gjc");
		expect(commands[0]).toEqual(["bun", "install", "--production"]);
		expect(commands[1]?.[0]).toMatch(/\/\.staging-[^/]+\/bin\/gjc$/);
		expect(commands[1]?.[1]).toBe("--version");
		expect(commands.every((command) => command[0] !== "patch")).toBe(true);
		expect(await readdir(layout.gjcPrefix)).toContain("0.18.8");
		expect((await readdir(layout.gjcPrefix)).some((entry) => entry.startsWith(".staging-"))).toBe(false);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("installPinnedGjc applies the 0.19.3 lane-switch patch after bun install and before verification", async () => {
	const directory = await home();
	const { runner, commands, cwds } = installRunner();
	try {
		await installPinnedGjc({ home: directory, version: "0.19.3", runner });
		const installIndex = commands.findIndex((command) => command[0] === "bun");
		const patchIndex = commands.findIndex((command) => command[0] === "patch");
		const verifyIndex = commands.findIndex((command) => command[1] === "--version");
		expect(installIndex).toBe(0);
		expect(patchIndex).toBeGreaterThan(installIndex);
		expect(verifyIndex).toBeGreaterThan(patchIndex);
		expect(commands[patchIndex]).toEqual([
			"patch",
			"--batch",
			"--forward",
			"--fuzz=0",
			"--strip=1",
			"--directory=node_modules/@gajae-code",
			"--input",
			expect.stringMatching(/\/\.gjc-lane-switch\.patch$/),
		]);
		expect(cwds[patchIndex]).toMatch(/\/\.staging-[^/]+$/);
		const layout = homeLayout(directory);
		expect(await readdir(join(layout.gjcPrefix, "0.19.3"))).not.toContain(".gjc-lane-switch.patch");
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("the embedded 0.19.3 lane-switch patch applies to pristine sources with GNU patch --fuzz=0", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-lane-switch-"));
	try {
		const version = Bun.spawnSync(["patch", "--version"], { stdout: "pipe", stderr: "pipe" });
		expect(version.stdout.toString()).toContain("GNU patch");
		await stageLaneSwitchSources(directory);
		const result = await applyLaneSwitchPatch(directory);
		expect(result.exitCode).toBe(0);
		const session = await readFile(
			join(directory, "node_modules", "@gajae-code", "coding-agent", "src", "session", "agent-session.ts"),
			"utf8",
		);
		expect(session).toContain('import { installLaneModelSwitch } from "./lane-model-switch";');
		expect(session).toContain("installLaneModelSwitch({");
		const laneSwitch = await readFile(
			join(directory, "node_modules", "@gajae-code", "coding-agent", "src", "session", "lane-model-switch.ts"),
			"utf8",
		);
		expect(laneSwitch).toContain("export function installLaneModelSwitch(");
		const agent = await readFile(
			join(directory, "node_modules", "@gajae-code", "agent-core", "src", "agent.ts"),
			"utf8",
		);
		expect(agent).toContain('beforeModelCall?: AgentLoopConfig["beforeModelCall"];');
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a lane-switch patch that does not apply cleanly fails", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-lane-switch-"));
	try {
		await stageLaneSwitchSources(directory);
		for (const [, installed] of GJC_019_SOURCES)
			await writeFile(join(directory, "node_modules", "@gajae-code", installed), "// tampered source\n");
		const result = await applyLaneSwitchPatch(directory);
		expect(result.exitCode).not.toBe(0);
		expect(`${result.stdout}\n${result.stderr}`).toMatch(/FAILED|No file to patch|can't find file/i);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a failed lane-switch patch fails the install before verification and leaves current unchanged", async () => {
	const directory = await home();
	const { runner } = installRunner();
	try {
		const layout = homeLayout(directory);
		await installPinnedGjc({ home: directory, version: "0.18.8", runner });
		const failing = installRunner();
		const patchFails: GjcInstallRunner = async (command, cwd) => {
			const result = await failing.runner(command, cwd);
			return command[0] === "patch" ? { exitCode: 1, stdout: "", stderr: "error: patch does not apply" } : result;
		};
		await expect(installPinnedGjc({ home: directory, version: "0.19.3", runner: patchFails })).rejects.toThrow(
			"error: patch does not apply",
		);
		expect(failing.commands.map((command) => command[0])).toEqual(["bun", "patch"]);
		expect(await readlink(layout.gjcCurrent)).toBe("0.18.8");
		expect((await readdir(layout.gjcPrefix)).sort()).toEqual(["0.18.8", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a failed isolated install leaves the active current link unchanged and removes staging", async () => {
	const directory = await home();
	const { runner } = installRunner();
	try {
		const layout = homeLayout(directory);
		await installPinnedGjc({ home: directory, version: "0.18.8", runner });
		const failing: GjcInstallRunner = async () => ({ exitCode: 1, stdout: "", stderr: "registry unavailable" });
		await expect(installPinnedGjc({ home: directory, version: "0.19.0", runner: failing })).rejects.toThrow(
			"registry unavailable",
		);
		expect(await readlink(layout.gjcCurrent)).toBe("0.18.8");
		expect((await readdir(layout.gjcPrefix)).sort()).toEqual(["0.18.8", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an existing unpatched pin is refused without patching it in place", async () => {
	const directory = await home();
	const { runner, commands } = installRunner();
	try {
		const layout = homeLayout(directory);
		await mkdir(join(layout.gjcPrefix, "0.19.3"), { recursive: true });
		await expect(installPinnedGjc({ home: directory, version: "0.19.3", runner })).rejects.toThrow(
			"lacks the required lane-switch patch",
		);
		expect(commands).toEqual([]);
		expect(await readdir(join(layout.gjcPrefix, "0.19.3"))).toEqual([]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an existing patched pin verifies without applying the patch twice", async () => {
	const directory = await home();
	const { runner, commands } = installRunner();
	try {
		await installPinnedGjc({ home: directory, version: "0.19.3", runner });
		commands.length = 0;
		await installPinnedGjc({ home: directory, version: "0.19.3", runner });
		expect(commands).toHaveLength(1);
		expect(commands[0]?.[1]).toBe("--version");
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("installPinnedGjc prunes old versions while retaining current and its predecessor", async () => {
	const directory = await home();
	const { runner } = installRunner();
	try {
		for (const version of ["0.18.8", "0.19.0", "0.20.0"]) await installPinnedGjc({ home: directory, version, runner });
		const layout = homeLayout(directory);
		expect(await readlink(layout.gjcCurrent)).toBe("0.20.0");
		expect((await readdir(layout.gjcPrefix)).sort()).toEqual(["0.19.0", "0.20.0", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

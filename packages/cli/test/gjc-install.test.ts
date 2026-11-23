import { expect, test } from "bun:test";
import { access, mkdir, mkdtemp, readdir, readFile, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { type GjcInstallRunner, installPinnedGjc } from "../src/gjc-install";

async function home(): Promise<string> {
	return await mkdtemp(join(tmpdir(), "gajaeway-gjc-install-"));
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

test("installPinnedGjc applies the 0.21.1 lane-switch patch after bun install and before verification", async () => {
	const directory = await home();
	const { runner, commands, cwds } = installRunner();
	try {
		await installPinnedGjc({ home: directory, version: "0.21.1", runner });
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
		expect(await readdir(join(layout.gjcPrefix, "0.21.1"))).not.toContain(".gjc-lane-switch.patch");
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("the embedded 0.21.1 lane-switch patch applies to pristine sources with GNU patch --fuzz=0", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-lane-switch-"));
	const source =
		process.env.GAJAEWAY_GJC_0_21_1_PRISTINE ??
		"/home/cheongnet/gajaeway/state/workspace/gjc-lane-switch-0.21.1-pristine";
	try {
		const version = Bun.spawnSync(["patch", "--version"], { stdout: "pipe", stderr: "pipe" });
		expect(version.stdout.toString()).toContain("GNU patch");
		const patchText = await readFile(
			join(import.meta.dir, "../src/patches/gajae-code/0.21.1-lane-switch.patch"),
			"utf8",
		);
		const patchPath = join(directory, "0.21.1-lane-switch.patch");
		await writeFile(patchPath, patchText);
		const installed = [
			"agent-core/src/agent-loop.ts",
			"agent-core/src/agent.ts",
			"agent-core/src/types.ts",
			"coding-agent/src/commands/sdk.ts",
			"coding-agent/src/sdk/host/session-runtime.ts",
			"coding-agent/src/session/agent-session.ts",
			"coding-agent/src/session/client-bridge.ts",
		];
		for (const relative of installed) {
			const destination = join(directory, "node_modules", "@gajae-code", relative);
			await mkdir(dirname(destination), { recursive: true });
			await writeFile(destination, await readFile(join(source, relative)));
		}
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
		expect(exitCode, `${stdout}\n${stderr}`).toBe(0);
		const session = await readFile(
			join(directory, "node_modules", "@gajae-code", "coding-agent", "src", "session", "agent-session.ts"),
			"utf8",
		);
		expect(session).toContain("installLaneModelSwitch({");
		expect(session).toContain('process.env.GAJAEWAY_LANE_SWITCH === "1"');
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a lane-switch patch that does not apply cleanly fails", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-lane-switch-"));
	try {
		const installed = "coding-agent/src/session/agent-session.ts";
		const destination = join(directory, "node_modules", "@gajae-code", installed);
		await mkdir(dirname(destination), { recursive: true });
		await writeFile(destination, "// tampered source\n");
		const patchPath = join(directory, "0.21.1-lane-switch.patch");
		await writeFile(patchPath, await readFile(join(import.meta.dir, "../src/patches/gajae-code/0.21.1-lane-switch.patch")));
		const child = Bun.spawn(
			["patch", "--batch", "--forward", "--fuzz=0", "--strip=1", "--directory=node_modules/@gajae-code", "--input", patchPath],
			{ cwd: directory, stdin: "ignore", stdout: "pipe", stderr: "pipe" },
		);
		const [stdout, stderr, exitCode] = await Promise.all([
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
			child.exited,
		]);
		expect(exitCode).not.toBe(0);
		expect(`${stdout}\n${stderr}`).toMatch(/FAILED|No file to patch|can't find file/i);
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
		await expect(installPinnedGjc({ home: directory, version: "0.21.1", runner: patchFails })).rejects.toThrow(
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
		await mkdir(join(layout.gjcPrefix, "0.21.1"), { recursive: true });
		await expect(installPinnedGjc({ home: directory, version: "0.21.1", runner })).rejects.toThrow(
			"lacks the required lane-switch patch",
		);
		expect(commands).toEqual([]);
		expect(await readdir(join(layout.gjcPrefix, "0.21.1"))).toEqual([]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an existing patched pin verifies without applying the patch twice", async () => {
	const directory = await home();
	const { runner, commands } = installRunner();
	try {
		await installPinnedGjc({ home: directory, version: "0.21.1", runner });
		commands.length = 0;
		await installPinnedGjc({ home: directory, version: "0.21.1", runner });
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
		for (const version of ["0.18.8", "0.19.0", "0.22.0"]) await installPinnedGjc({ home: directory, version, runner });
		const layout = homeLayout(directory);
		expect(await readlink(layout.gjcCurrent)).toBe("0.22.0");
		expect((await readdir(layout.gjcPrefix)).sort()).toEqual(["0.19.0", "0.22.0", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

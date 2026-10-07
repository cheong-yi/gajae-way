import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PROFILE_VERSION } from "@gajae-gateway/protocol";

let home = "";
let child: ReturnType<typeof Bun.spawn> | undefined;
afterEach(async () => {
	// Native test timeouts must not leave a daemon waiting on graceful shutdown.
	child?.kill("SIGKILL");
	if (child) await child.exited;
	child = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

/**
 * A `gateway.shutdown` request over stdio must let `stop()` resolve: the request
 * task awaits stop(), and stop() awaits in-flight requests, so tracking the
 * shutdown task itself would self-await forever and the daemon would never exit.
 */
test("a stdio gateway.shutdown request completes ordered shutdown and exits the daemon", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-stdio-shutdown-"));
	await Bun.write(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
	const daemon = Bun.spawn({
		cmd: ["bun", "packages/gateway/test/daemon-entry.ts", "--stdio"],
		cwd: join(import.meta.dir, "../../.."),
		env: { ...process.env, GAJAEWAY_HOME: home },
		stdin: "pipe",
		stdout: "pipe",
		stderr: "ignore",
	});
	child = daemon;
	const output = new Response(daemon.stdout as ReadableStream).text();
	daemon.stdin.write(
		`${JSON.stringify({ v: PROFILE_VERSION, type: "hello", payload: { supportedVersions: [PROFILE_VERSION] } })}\n`,
	);
	daemon.stdin.write(
		`${JSON.stringify({ v: PROFILE_VERSION, type: "request", id: "bye", verb: "gateway.shutdown", params: {} })}\n`,
	);
	daemon.stdin.flush();
	// The native test deadline bounds this wait; teardown kills only this fixture.
	expect(await daemon.exited).toBe(0);
	const frames = (await output)
		.split("\n")
		.filter(Boolean)
		.map((line) => JSON.parse(line) as { type: string; event?: string; id?: string });
	expect(frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(true);
});

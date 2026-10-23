import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { WorkFirstmateRetireParams } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { CLI_USAGE, main, parseArgs } from "../src/main";

test("closeout parser forwards exact native envelope and preserves unproven steer hold", async () => {
	const root = await mkdtemp(join(tmpdir(), "firstmate-closeout-"));
	const file = join(root, "closeout.json");
	const params: WorkFirstmateRetireParams = {
		firstmate: {
			name: "mapped",
			identity: { taskId: "task-1", jobId: "job-1", opRef: "op-1", ordinal: 1, sessionId: "session-1", epoch: 1 },
			ownerId: "owner-1",
			messageId: "message-1",
			thread: { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: "channel-1" },
			mandateId: "mandate-1",
			scope: "repo-1",
			approvalId: "approval-1",
			action: "release_session_preserve_work",
		},
	};
	const held = {
		retired: false,
		held: true,
		reason: "firstmate obligations unresolved",
		obligations: [{ id: "report-1", reason: "report_steer_acceptance_unproven" }],
	};
	const requests: { verb: string; params: unknown }[] = [];
	const lines: string[] = [];
	const errors: string[] = [];
	let closed = 0;
	let response: unknown = held;
	const client: GajaewayClient = Object.create(GajaewayClient.prototype);
	client.request = async <T>(verb: string, payload?: unknown): Promise<T> => {
		requests.push({ verb, params: payload });
		return response as T;
	};
	client.close = async () => {
		closed++;
	};
	const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
	const log = spyOn(console, "log").mockImplementation((line) => {
		lines.push(String(line));
	});
	const error = spyOn(console, "error").mockImplementation((line) => {
		errors.push(String(line));
	});
	const previousExit = process.exitCode;
	try {
		await writeFile(file, JSON.stringify(params));
		expect(CLI_USAGE).toContain("work retire --firstmate CLOSEOUT_JSON_FILE");
		expect(parseArgs(["--socket", "/test/closeout.sock", "work", "retire", "--firstmate", file])).toEqual({
			socket: "/test/closeout.sock",
			command: "work",
			rest: ["retire", "--firstmate", file],
		});
		process.exitCode = 0;
		await main(["--socket", "/test/closeout.sock", "work", "retire", "--firstmate", file]);
		expect(requests).toEqual([{ verb: "work.retire", params }]);
		expect(lines).toEqual([JSON.stringify(held)]);
		expect(errors).toEqual([]);
		expect(process.exitCode).toBe(1);
		expect(closed).toBe(1);
		expect(connect.mock.calls[0]).toEqual(["/test/closeout.sock", { requestTimeoutMs: 120_000, deadlineMs: 30_000 }]);
		// A steer/done inbox row does not distinguish acceptance from abandonment.
		// The CLI must neither invent a delivery receipt nor retry via ordinary/forced retirement.
		expect(JSON.parse(lines[0]).obligations[0].reason).toBe("report_steer_acceptance_unproven");
		for (const extra of [["--force"], ["--all-dead"], ["mapped"], ["--firstmate", file]]) {
			await main(["work", "retire", "--firstmate", file, ...extra]);
		}
		expect(connect).toHaveBeenCalledTimes(1);
		expect(requests).toHaveLength(1);
		expect(errors).toHaveLength(4);
		expect(errors.every((line) => line.includes("retire --firstmate CLOSEOUT_JSON_FILE"))).toBe(true);
		response = { retired: true, release: { releaseId: "release-1", preserved: true } };
		process.exitCode = 0;
		await main(["work", "retire", "--firstmate", file]);
		expect(lines[1]).toBe(JSON.stringify(response));
		expect(process.exitCode).toBe(0);
		expect(closed).toBe(2);
	} finally {
		connect.mockRestore();
		log.mockRestore();
		error.mockRestore();
		process.exitCode = previousExit ?? 0;
		await rm(root, { recursive: true, force: true });
	}
});

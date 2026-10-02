import { readFile } from "node:fs/promises";
import { ConfigError, gatewayHome, parseConfigFile } from "./config";

/**
 * Offline config validation (`gajaeway-gateway config check [path]`).
 *
 * Channel and boundary policies are parsed at boot and on reload. An invalid
 * reload keeps the current live config, but an invalid file prevents the next
 * gateway boot; the Discord adapter can remain up without its gateway behind
 * it. Offline validation catches that case before a restart.
 *
 * This check needs no socket, so it works before a restart or while the gateway
 * is down. It is a pre-restart gate, not a post-mortem.
 */
export interface ConfigCheckOk {
	readonly ok: true;
	readonly path: string;
	readonly channels: readonly string[];
	readonly openChannels: readonly string[];
	readonly mentionOpenChannels: readonly string[];
	readonly boundaries: readonly string[];
	readonly openBoundaries: readonly string[];
	readonly mentionOpenBoundaries: readonly string[];
}

export interface ConfigCheckFailure {
	readonly ok: false;
	readonly path: string;
	readonly code: string;
	readonly message: string;
}

export type ConfigCheckResult = ConfigCheckOk | ConfigCheckFailure;

export function defaultConfigPath(env: NodeJS.ProcessEnv = process.env): string {
	return `${gatewayHome(env)}/config.json`;
}

export async function checkConfigFile(path: string): Promise<ConfigCheckResult> {
	let raw: string;
	try {
		raw = await readFile(path, "utf8");
	} catch (error) {
		return { ok: false, path, code: "unreadable", message: messageOf(error) };
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch (error) {
		// A stray comma is the most common hand-edit break and the parser reports
		// it by offset, so the raw message is more useful than a summary.
		return { ok: false, path, code: "not_json", message: messageOf(error) };
	}
	try {
		const config = parseConfigFile(parsed);
		const channels = Object.keys(config.channels ?? {});
		const boundaries = Object.keys(config.boundaries ?? {});
		return {
			ok: true,
			path,
			channels,
			openChannels: channels.filter((id) => config.channels?.[id]?.engagement === "open"),
			mentionOpenChannels: channels.filter((id) => config.channels?.[id]?.engagement === "mention-open"),
			boundaries,
			openBoundaries: boundaries.filter((key) => config.boundaries?.[key]?.engagement === "open"),
			mentionOpenBoundaries: boundaries.filter((key) => config.boundaries?.[key]?.engagement === "mention-open"),
		};
	} catch (error) {
		return {
			ok: false,
			path,
			code: error instanceof ConfigError ? error.code : "invalid",
			message: messageOf(error),
		};
	}
}

export function renderConfigCheck(result: ConfigCheckResult): string[] {
	if (!result.ok) return [`FAIL ${result.path}`, `  ${result.code}: ${result.message}`];
	const closed = result.channels.length - result.openChannels.length - result.mentionOpenChannels.length;
	const closedBoundaries =
		result.boundaries.length - result.openBoundaries.length - result.mentionOpenBoundaries.length;
	return [
		`OK ${result.path}`,
		`  channels: ${result.channels.length} (open ${result.openChannels.length}, mention-open ${result.mentionOpenChannels.length}, closed/default ${closed})`,
		`  boundaries: ${result.boundaries.length} (open ${result.openBoundaries.length}, mention-open ${result.mentionOpenBoundaries.length}, closed/default ${closedBoundaries})`,
		"  channel and boundary policy changes apply on config reload or gateway restart.",
	];
}

/** 0 when the file would boot, 1 when it would not. */
export function configCheckExitCode(result: ConfigCheckResult): number {
	return result.ok ? 0 : 1;
}

function messageOf(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

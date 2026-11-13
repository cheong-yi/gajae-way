import { expect, test } from "bun:test";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import {
	type DiscordClientLike,
	openDiscordPane,
	settleDiscordDelivery,
	subscribeDiscordPaneRequests,
} from "../src/main";

const origin: OriginRef = {
	platform: "discord",
	kind: "thread",
	conversationId: "child",
	parentId: "parent",
	threadVisibility: "public",
};
for (const failure of ["missing", "archived"] as const) {
	test(`pane noChildFallback refuses ${failure} thread without fetching parent`, async () => {
		const fetched: string[] = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		const discord: DiscordClientLike = {
			channels: {
				fetch: async (id) => {
					fetched.push(id);
					if (failure === "missing") throw Object.assign(new Error("Unknown Channel"), { code: 10003 });
					return {
						archived: true,
						setArchived: async () => {
							throw new Error("Missing Permissions");
						},
						send: async () => {
							throw new Error("must not send");
						},
					};
				},
			},
		};
		const payload: ChatMessagePayload = {
			origin,
			turnId: "turn",
			deliveryId: "delivery",
			role: "assistant",
			text: "private pane transcript",
			final: false,
			noChildFallback: true,
		};
		await settleDiscordDelivery(
			{
				request: async <T>(verb: string, params?: unknown) => {
					requests.push({ verb, params });
					return {} as T;
				},
			},
			discord,
			payload,
		);
		expect(fetched).toEqual(["child"]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.verb).toBe("delivery.fail");
	});
}

interface PaneChannel {
	readonly id: string;
	readonly name: string;
	readonly parent?: { readonly name: string } | null;
	readonly parentId?: string;
}

/** A client whose guild cache lists these channels; fetch records the chosen pane parent. */
function paneClient(channels: readonly PaneChannel[]) {
	const fetched: string[] = [];
	const anchors: unknown[] = [];
	const starts: unknown[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id) => {
				fetched.push(id);
				return {
					send: async (text: unknown) => {
						anchors.push(text);
						return {
							startThread: async (input: unknown) => {
								starts.push(input);
								return { id: `thread-${id}` };
							},
						};
					},
				};
			},
		},
		guilds: { cache: [{ channels: { cache: channels } }] },
	};
	return { discord, fetched, anchors, starts };
}

const PANE_FALLBACK_ID = "1511673764574793798";
const ACTIVE_CATEGORY: PaneChannel = { id: "1520000000000000001", name: "ACTIVE REPOS" };
const INACTIVE_CATEGORY: PaneChannel = { id: "1520000000000000000", name: "INACTIVE-REPOS" };
const ACTIVE_REPO: PaneChannel = {
	id: "1520000000000000009",
	name: "repo-gajae-way",
	parent: { name: "ACTIVE REPOS" },
};
const INACTIVE_REPO: PaneChannel = {
	id: "1530000000000000003",
	name: "repo-gajae-way",
	parentId: INACTIVE_CATEGORY.id,
};
const OTHER_CATEGORY: PaneChannel = { id: "1520000000000000002", name: "ARCHIVE" };
const UNCATEGORIZED_REPO: PaneChannel = { id: "1540000000000000000", name: "repo-gajae-way" };
const OTHER_CATEGORY_REPO: PaneChannel = {
	id: "1550000000000000000",
	name: "repo-gajae-way",
	parent: { name: "ARCHIVE" },
};
const TEXT_CHANNEL: PaneChannel = {
	id: "1560000000000000000",
	name: "general",
	parent: { name: "ACTIVE REPOS" },
};

test("pane gateway request uses the ACTIVE REPOS channel for the repo slug", async () => {
	const { discord, fetched, anchors, starts } = paneClient([
		ACTIVE_CATEGORY,
		INACTIVE_CATEGORY,
		OTHER_CATEGORY,
		ACTIVE_REPO,
		INACTIVE_REPO,
		UNCATEGORIZED_REPO,
		OTHER_CATEGORY_REPO,
		TEXT_CHANNEL,
	]);
	const result = await openDiscordPane(discord, { parent: origin, name: "FirstMate parser", repoSlug: "gajae-way" });
	expect(result).toEqual({ threadId: `thread-${ACTIVE_REPO.id}`, parentChannelId: ACTIVE_REPO.id });
	expect(result.parentChannelId).toBe(fetched[0]!);
	expect(fetched).toEqual([ACTIVE_REPO.id]);
	expect(anchors).toEqual(["FirstMate parser"]);
	expect(starts).toEqual([{ name: "FirstMate parser", autoArchiveDuration: 1440 }]);
});

test("pane gateway request falls back to the INACTIVE-REPOS channel for the repo slug", async () => {
	const { discord, fetched } = paneClient([
		ACTIVE_CATEGORY,
		INACTIVE_CATEGORY,
		OTHER_CATEGORY,
		INACTIVE_REPO,
		UNCATEGORIZED_REPO,
		OTHER_CATEGORY_REPO,
	]);
	const result = await openDiscordPane(discord, { parent: origin, name: "FirstMate parser", repoSlug: "gajae-way" });
	expect(result).toEqual({ threadId: `thread-${INACTIVE_REPO.id}`, parentChannelId: INACTIVE_REPO.id });
	expect(result.parentChannelId).toBe(fetched[0]!);
	expect(fetched).toEqual([INACTIVE_REPO.id]);
});

test("pane gateway request uses the fallback channel when no repo channel matches the slug", async () => {
	const { discord, fetched } = paneClient([ACTIVE_CATEGORY, INACTIVE_CATEGORY, OTHER_CATEGORY, UNCATEGORIZED_REPO]);
	const result = await openDiscordPane(discord, { parent: origin, name: "FirstMate parser", repoSlug: "gajae-way" });
	expect(result).toEqual({ threadId: `thread-${PANE_FALLBACK_ID}`, parentChannelId: PANE_FALLBACK_ID });
	expect(result.parentChannelId).toBe(fetched[0]!);
	expect(fetched).toEqual([PANE_FALLBACK_ID]);
});

test("pane gateway request without a repo slug uses the fallback channel", async () => {
	const { discord, fetched } = paneClient([ACTIVE_CATEGORY, INACTIVE_CATEGORY, ACTIVE_REPO]);
	const result = await openDiscordPane(discord, { parent: origin, name: "FirstMate parser" });
	expect(result).toEqual({ threadId: `thread-${PANE_FALLBACK_ID}`, parentChannelId: PANE_FALLBACK_ID });
	expect(result.parentChannelId).toBe(fetched[0]!);
	expect(fetched).toEqual([PANE_FALLBACK_ID]);
});

test("pane gateway request picks the lowest channel id among two ACTIVE REPOS hits", async () => {
	const { discord, fetched } = paneClient([
		ACTIVE_CATEGORY,
		ACTIVE_REPO,
		{ id: "1520000000000000002", name: "Repo-Gajae-Way", parent: { name: "ACTIVE REPOS" } },
	]);
	const result = await openDiscordPane(discord, { parent: origin, name: "FirstMate parser", repoSlug: "gajae-way" });
	expect(result).toEqual({ threadId: "thread-1520000000000000002", parentChannelId: "1520000000000000002" });
	expect(result.parentChannelId).toBe(fetched[0]!);
	expect(fetched).toEqual(["1520000000000000002"]);
});

test("pane subscription reports request success and failure to the gateway", async () => {
	let handler!: (payload: unknown) => void;
	let unsubscribed = false;
	const responses: unknown[] = [];
	const fetched: string[] = [];
	const off = subscribeDiscordPaneRequests(
		{
			on: (event, callback) => {
				expect(event).toBe("discord.thread.open");
				handler = callback;
				return () => {
					unsubscribed = true;
				};
			},
			onChatMessage: () => () => {},
			request: async <T>(_verb: string, params?: unknown) => {
				responses.push(params);
				return {} as T;
			},
		},
		{
			channels: {
				fetch: async (id) => {
					fetched.push(id);
					return { send: async () => ({ startThread: async () => ({ id: "child" }) }) };
				},
			},
		},
	);
	handler({
		requestId: "request",
		parent: { platform: "discord", kind: "channel", conversationId: "parent" },
		name: "pane",
		repoSlug: "gajae-way",
	});
	for (let index = 0; index < 100 && responses.length === 0; index++) await Bun.sleep(1);
	expect(fetched).toEqual([PANE_FALLBACK_ID]);
	expect(responses).toEqual([{ requestId: "request", threadId: "child", parentChannelId: PANE_FALLBACK_ID }]);
	handler({
		requestId: "invalid",
		parent: { platform: "discord", kind: "dm", conversationId: "dm", peerId: "peer" },
		name: "pane",
	});
	for (let index = 0; index < 100 && responses.length < 2; index++) await Bun.sleep(1);
	expect(responses[1]).toMatchObject({ requestId: "invalid", error: "invalid pane parent" });
	off();
	expect(unsubscribed).toBe(true);
});

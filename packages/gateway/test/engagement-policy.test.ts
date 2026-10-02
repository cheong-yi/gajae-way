import { expect, test } from "bun:test";
import type { GatewayConfig } from "../src/config";
import { decideEngagement, resolveBotAudienceLimits, resolveChannelPolicy } from "../src/engagement/policy";

const config: GatewayConfig = {
	schemaVersion: 1,
	home: "/tmp/home",
	configPath: "/tmp/home/config.json",
	socketPath: "/tmp/home/gateway.sock",
	dbPath: "/tmp/home/gateway.db",
	logVerbosity: "info",
};
const engagement = { mentioned: false, group: true, authorId: "author" };
test("policy lookup ignores inherited exact, parent, and boundary entries", () => {
	const origin = {
		platform: "discord",
		kind: "thread",
		conversationId: "toString",
		parentId: "parent",
		boundaryId: "guild",
	} as const;
	const channels = Object.create({ "discord:toString": { engagement: "open" }, parent: { engagement: "open" } });
	const boundaries = Object.create({ "discord:guild": { engagement: "open" } });
	expect(resolveChannelPolicy(origin, { ...config, channels, boundaries })).toBeUndefined();
	boundaries["discord:guild"] = { engagement: "mention-open" };
	expect(resolveChannelPolicy(origin, { ...config, channels, boundaries })).toEqual({ engagement: "mention-open" });
	channels["discord:toString"] = {};
	expect(resolveChannelPolicy(origin, { ...config, channels, boundaries })).toEqual({});
});
test("loopback engages while unmentioned groups and unauthorised DMs decline", () => {
	// A DM from nobody in particular is no longer a free turn: with no owner and
	// no allowlist configured, the DM path fails closed.
	expect(decideEngagement({ platform: "discord", kind: "dm", conversationId: "dm" }, undefined, config)).toEqual({
		engaged: false,
		botAudienceAdmission: false,
	});
	expect(
		decideEngagement({ platform: "loopback", kind: "loopback", conversationId: "loopback" }, undefined, config),
	).toEqual({ engaged: true, botAudienceAdmission: false });
	expect(
		decideEngagement({ platform: "discord", kind: "channel", conversationId: "channel" }, engagement, config),
	).toEqual({ engaged: false, botAudienceAdmission: false });
});
test("contextOnly is recorded but never opens a turn, on every surface", () => {
	const open = { ...config, channels: { channel: { engagement: "open" as const } } };
	const owner = { mentioned: true, group: true, authorId: "owner-1", contextOnly: true };
	// Would otherwise engage: open channel, owner, mentioned.
	expect(decideEngagement({ platform: "discord", kind: "channel", conversationId: "channel" }, owner, open)).toEqual({
		engaged: false,
		botAudienceAdmission: false,
	});
	// DMs too - the DM gate normally admits the owner unconditionally.
	expect(
		decideEngagement({ platform: "discord", kind: "dm", conversationId: "d1" }, { ...owner, group: false }, config)
			.engaged,
	).toBe(false);
	// And loopback, which is otherwise always engaged.
	expect(
		decideEngagement({ platform: "loopback", kind: "loopback", conversationId: "loopback" }, owner, config).engaged,
	).toBe(false);
});

test("per-channel open override engages group messages", () => {
	expect(
		decideEngagement({ platform: "discord", kind: "channel", conversationId: "channel" }, engagement, {
			...config,
			channels: { channel: { engagement: "open" } },
		}),
	).toEqual({ engaged: true, botAudienceAdmission: false });
});

test("mention allowlist gates group mention commands and DMs but never open channels", () => {
	const base = {
		schemaVersion: 1 as const,
		home: "/tmp/x",
		configPath: "/tmp/x/config.json",
		socketPath: "/tmp/x/s",
		dbPath: "/tmp/x/db",
		logVerbosity: "info" as const,
		mentionAllowlist: ["owner-1"],
	};
	const channel = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
	const allowed = { mentioned: true, group: true, authorId: "owner-1" };
	const stranger = { mentioned: true, group: true, authorId: "intruder-9" };
	expect(decideEngagement(channel, allowed, base as never).engaged).toBe(true);
	expect(decideEngagement(channel, stranger, base as never).engaged).toBe(false);
	// Open channels are rooms the persona inhabits: allowlist does not gate listening.
	const open = { ...base, channels: { c1: { engagement: "open" as const } } };
	expect(
		decideEngagement(channel, { mentioned: false, group: true, authorId: "intruder-9" }, open as never).engaged,
	).toBe(true);
	// DMs are authorised like anything else: allowlisted in, stranger out.
	const dm = { platform: "discord", kind: "dm", conversationId: "d1", peerId: "p" } as const;
	expect(decideEngagement(dm, stranger, base as never).engaged).toBe(false);
	expect(decideEngagement(dm, { mentioned: false, group: false, authorId: "owner-1" }, base as never).engaged).toBe(
		true,
	);
});

test("bot authors never get the open-channel free pass; a bot mention still engages", () => {
	const base = {
		schemaVersion: 1 as const,
		home: "/tmp/x",
		configPath: "/tmp/x/config.json",
		socketPath: "/tmp/x/s",
		dbPath: "/tmp/x/db",
		logVerbosity: "info" as const,
		mentionAllowlist: ["owner-1", "sibling-bot"],
	};
	const channel = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
	const open = { ...base, channels: { c1: { engagement: "open" as const } } };
	// Sibling-bot chatter (progress spam, replies to each other) must not burn turns.
	expect(
		decideEngagement(
			channel,
			{ mentioned: false, group: true, authorId: "sibling-bot", authorIsBot: true },
			open as never,
		).engaged,
	).toBe(false);
	// A bot that explicitly mentions us gets a turn through the normal allowlisted mention path.
	expect(
		decideEngagement(
			channel,
			{ mentioned: true, group: true, authorId: "sibling-bot", authorIsBot: true },
			open as never,
		).engaged,
	).toBe(true);
	// An unlisted bot mention stays context, never a turn.
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "rogue-bot", authorIsBot: true }, open as never)
			.engaged,
	).toBe(false);
	// Humans keep the open-channel free pass.
	expect(decideEngagement(channel, { mentioned: false, group: true, authorId: "human-2" }, open as never).engaged).toBe(
		true,
	);
});

test("channel policy precedence is exact, thread parent, then platform boundary", () => {
	const thread = {
		platform: "discord",
		kind: "thread",
		conversationId: "thread-1",
		parentId: "channel-1",
		boundaryId: "guild-1",
	} as const;
	const exactNamespaced = { engagement: "open" as const };
	const exactLegacy = { engagement: "mention-open" as const };
	const parentNamespaced = { engagement: "closed" as const };
	const parentLegacy = { audience: "human-only" as const };
	const boundary = { engagement: "open" as const, audience: "all" as const };
	const allEntries = {
		...config,
		channels: {
			"discord:thread-1": exactNamespaced,
			"thread-1": exactLegacy,
			"discord:channel-1": parentNamespaced,
			"channel-1": parentLegacy,
		},
		boundaries: { "discord:guild-1": boundary },
	};
	expect(resolveChannelPolicy(thread, allEntries)).toBe(exactNamespaced);
	expect(
		resolveChannelPolicy(thread, {
			...allEntries,
			channels: {
				"thread-1": exactLegacy,
				"discord:channel-1": parentNamespaced,
				"channel-1": parentLegacy,
			},
		}),
	).toBe(exactLegacy);
	expect(resolveChannelPolicy(thread, { ...allEntries, channels: { "discord:channel-1": parentNamespaced } })).toBe(
		parentNamespaced,
	);
	expect(resolveChannelPolicy(thread, { ...allEntries, channels: { "channel-1": parentLegacy } })).toBe(parentLegacy);
	expect(resolveChannelPolicy(thread, { ...config, boundaries: { "discord:guild-1": boundary } })).toBe(boundary);
});

test("whole policy entries block boundary fallback and are never field-merged", () => {
	const channel = { platform: "discord", kind: "channel", conversationId: "c1", boundaryId: "guild-1" } as const;
	const boundary = { engagement: "open" as const, audience: "human-only" as const };
	const emptyOverride = {
		...config,
		channels: { "discord:c1": {} },
		boundaries: { "discord:guild-1": boundary },
	};
	expect(resolveChannelPolicy(channel, emptyOverride)).toEqual({});
	expect(decideEngagement(channel, engagement, emptyOverride)).toEqual({ engaged: false, botAudienceAdmission: false });

	const closedOverride = {
		...config,
		channels: { "discord:c1": { engagement: "closed" as const } },
		boundaries: { "discord:guild-1": boundary },
	};
	expect(decideEngagement(channel, engagement, closedOverride).engaged).toBe(false);

	// The exact entry's omitted audience retains legacy bot gating; it does not
	// inherit the boundary's explicit human-only exclusion.
	const exactWithoutAudience = {
		...config,
		mentionAllowlist: ["owner"],
		channels: { "discord:c1": { engagement: "open" as const } },
		boundaries: { "discord:guild-1": boundary },
	};
	expect(
		decideEngagement(
			channel,
			{ mentioned: true, group: true, authorId: "owner", authorIsBot: true },
			exactWithoutAudience,
		).engaged,
	).toBe(true);
});

test("boundaries are platform-qualified, missing boundaries stay closed, and DMs ignore them", () => {
	const discord = {
		platform: "discord",
		kind: "channel",
		conversationId: "d1",
		boundaryId: "1510336487894286436",
	} as const;
	const slack = {
		platform: "slack",
		kind: "channel",
		conversationId: "s1",
		boundaryId: "1510336487894286436",
	} as const;
	const configWithBoundaries = {
		...config,
		channels: { s1: { engagement: "open" as const } },
		boundaries: {
			"discord:1510336487894286436": { engagement: "open" as const },
			"slack:1510336487894286436": { engagement: "mention-open" as const },
		},
	};
	expect(decideEngagement(discord, engagement, configWithBoundaries).engaged).toBe(true);
	expect(decideEngagement(slack, engagement, configWithBoundaries).engaged).toBe(false);
	expect(decideEngagement(slack, { ...engagement, mentioned: true }, configWithBoundaries).engaged).toBe(true);
	expect(decideEngagement({ ...slack, boundaryId: "missing-guild" }, engagement, configWithBoundaries).engaged).toBe(
		false,
	);
	expect(
		decideEngagement({ ...discord, boundaryId: "1520004470489223219" }, engagement, configWithBoundaries).engaged,
	).toBe(false);

	const dm = { platform: "discord", kind: "dm", conversationId: "dm-1", boundaryId: "guild-1" } as const;
	expect(resolveChannelPolicy(dm, configWithBoundaries)).toBeUndefined();
	expect(
		decideEngagement(dm, { mentioned: false, group: false, authorId: "stranger" }, configWithBoundaries).engaged,
	).toBe(false);
});

test("boundary policy preserves owner, allowlist, and audience semantics", () => {
	const channel = { platform: "discord", kind: "channel", conversationId: "c1", boundaryId: "guild-1" } as const;
	const closedBoundary = {
		...config,
		boundaries: { "discord:guild-1": { engagement: "closed" as const } },
	};
	const ownerConfig = {
		...closedBoundary,
		ownerTarget: {
			origin: { platform: "discord" as const, kind: "dm" as const, conversationId: "owner-dm", peerId: "owner" },
		},
	};
	expect(decideEngagement(channel, { mentioned: true, group: true, authorId: "owner" }, ownerConfig).engaged).toBe(
		true,
	);
	expect(decideEngagement(channel, { mentioned: true, group: true, authorId: "stranger" }, ownerConfig).engaged).toBe(
		false,
	);
	const allowlistedConfig = { ...closedBoundary, mentionAllowlist: ["listed"] };
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "listed" }, allowlistedConfig).engaged,
	).toBe(true);
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "stranger" }, allowlistedConfig).engaged,
	).toBe(false);

	const humanOnly = {
		...config,
		mentionAllowlist: ["owner"],
		boundaries: { "discord:guild-1": { engagement: "open" as const, audience: "human-only" as const } },
	};
	expect(decideEngagement(channel, engagement, humanOnly).engaged).toBe(true);
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "owner", authorIsBot: true }, humanOnly)
			.engaged,
	).toBe(false);

	const legacyAudience = {
		...config,
		mentionAllowlist: ["owner"],
		boundaries: { "discord:guild-1": { engagement: "open" as const } },
	};
	expect(
		decideEngagement(channel, { ...engagement, authorId: "owner", authorIsBot: true }, legacyAudience).engaged,
	).toBe(false);
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "owner", authorIsBot: true }, legacyAudience)
			.engaged,
	).toBe(true);
});

test("Telegram topics retain parent policy precedence and legacy budget inputs retain inheritance", () => {
	const policy = { engagement: "closed" as const, botAudienceMaxTurnsPerWindow: 7 };
	const configured = {
		...config,
		channels: { "telegram:chat": policy },
		boundaries: { "telegram:community": { engagement: "open" as const } },
	};
	const topic = {
		platform: "telegram",
		kind: "topic",
		conversationId: "chat:topic",
		parentId: "chat",
		boundaryId: "community",
	} as const;
	expect(resolveChannelPolicy(topic, configured)).toBe(policy);
	expect(decideEngagement(topic, engagement, configured).engaged).toBe(false);
	expect(
		resolveBotAudienceLimits({ platform: "telegram", conversationId: "chat:topic", parentId: "chat" }, configured),
	).toEqual({ maxTurnsPerWindow: 7 });
});

test("boundary policy supplies bot budgets and thread follow-up without weakening author gates", () => {
	const channel = { platform: "discord", kind: "channel", conversationId: "c1", boundaryId: "guild-1" } as const;
	const boundaryBudget = {
		...config,
		boundaries: {
			"discord:guild-1": { botAudienceMaxConsecutiveTurns: 4, botAudienceMaxTurnsPerWindow: 8 },
		},
	};
	expect(resolveBotAudienceLimits(channel, boundaryBudget)).toEqual({
		maxConsecutiveTurns: 4,
		maxTurnsPerWindow: 8,
	});
	const threadWithParent = {
		platform: "discord",
		kind: "thread",
		conversationId: "t1",
		parentId: "c1",
		boundaryId: "guild-1",
	} as const;
	expect(
		resolveBotAudienceLimits(threadWithParent, {
			...boundaryBudget,
			channels: { "discord:c1": { botAudienceMaxConsecutiveTurns: 2, botAudienceMaxTurnsPerWindow: 3 } },
		}),
	).toEqual({ maxConsecutiveTurns: 2, maxTurnsPerWindow: 3 });
	expect(
		resolveBotAudienceLimits(channel, {
			...boundaryBudget,
			channels: { "discord:c1": { engagement: "open" as const } },
		}),
	).toEqual({ maxTurnsPerWindow: 30 });

	const thread = {
		platform: "discord",
		kind: "thread",
		conversationId: "t1",
		parentId: "c1",
		boundaryId: "guild-1",
	} as const;
	const followed = {
		...config,
		boundaries: { "discord:guild-1": { engagement: "mention-open" as const } },
	};
	expect(decideEngagement(thread, engagement, followed, true).engaged).toBe(true);
	expect(
		decideEngagement(
			thread,
			{ ...engagement, authorId: "stranger" },
			{
				...config,
				mentionAllowlist: ["owner"],
				boundaries: { "discord:guild-1": { engagement: "closed" as const } },
			},
			true,
		).engaged,
	).toBe(false);
});

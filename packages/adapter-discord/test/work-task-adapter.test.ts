import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type {
	ChatMessagePayload,
	ChatSendParams,
	WorkTaskProjection,
	WorkTaskThreadOrigin,
	WorkThreadBindParams,
	WorkThreadClaimParams,
} from "@gajae-gateway/protocol";
import { ChannelType } from "discord.js";
import {
	type DiscordClientLike,
	type DiscordInboundMessage,
	decideInbound,
	describeMessageEdit,
	type GatewayClientLike,
	ReconnectingGateway,
	reconcileDiscordWorkTasks,
	settleDiscordDelivery,
	type TypingPort,
} from "../src/main";

const taskId = "11111111-1111-4111-8111-111111111111";
const secondTaskId = "22222222-2222-4222-8222-222222222222";
const claimId = "33333333-3333-4333-8333-333333333333";
const origin: WorkTaskThreadOrigin = {
	platform: "discord",
	kind: "thread",
	conversationId: "1556589606403842128",
	parentId: "1511673764574793798",
	boundaryId: "1510336487894286436",
};
const parentOrigin = {
	platform: "discord",
	kind: "channel",
	conversationId: origin.parentId,
	boundaryId: origin.boundaryId,
} as const;
const quiet = { error: (_message: string) => {} };
const me = { id: "900" };

function task(surface: WorkTaskProjection["surface"], id = taskId): WorkTaskProjection {
	return {
		taskId: id,
		name: "investigation",
		kind: "read_only",
		jobId: "job",
		opRef: "op-original",
		sessionId: null,
		epoch: null,
		surface,
		obligation: "awaiting_final",
		finalReport: { reportId: null, completeness: "unavailable", disposition: "pending" },
	};
}
function gateway(handler: (verb: string, params: unknown) => unknown | Promise<unknown>): GatewayClientLike {
	return {
		request: async <T>(verb: string, params?: unknown): Promise<T> => (await handler(verb, params)) as T,
		onChatMessage: () => () => {},
	};
}
function thread(overrides: Record<string, unknown> = {}) {
	return {
		id: origin.conversationId,
		guildId: origin.boundaryId,
		parentId: origin.parentId,
		type: ChannelType.PublicThread,
		archived: false,
		locked: false,
		isThread: () => true,
		guild: { members: { me } },
		permissionsFor: () => ({ has: () => true }),
		send: async (_body: unknown): Promise<unknown> => ({ id: "100", channelId: origin.conversationId }),
		...overrides,
	};
}
function message(overrides: Partial<ChatMessagePayload> = {}): ChatMessagePayload {
	return {
		origin,
		role: "assistant",
		text: "result",
		final: true,
		turnId: "task-turn",
		deliveryId: "delivery-original",
		workTask: { taskId, opRef: "op-original", sourceId: "source-original", mappedOnly: true },
		...overrides,
	};
}
function inbound(createdTimestamp: number): DiscordInboundMessage {
	return {
		id: ((BigInt(createdTimestamp) - 1420070400000n) << 22n).toString(),
		content: "owner direction without a mention",
		createdTimestamp,
		author: { id: "800", username: "owner", globalName: "Owner" },
		guildId: origin.boundaryId,
		channel: { id: origin.conversationId, type: ChannelType.PublicThread, parentId: origin.parentId },
	};
}

// No SDK connection or Discord client is created in these boundary fixtures.
test("fresh claim precedes one create, exact verification precedes bind, and rediscovery never recreates", async () => {
	let surface: WorkTaskProjection["surface"] = { phase: "pending", request: { parentOrigin, title: "Task" } };
	const events: string[] = [];
	const selected = thread();
	const parent = {
		...thread(),
		id: origin.parentId,
		type: ChannelType.GuildText,
		isThread: () => false,
		threads: {
			create: async () => {
				events.push("create");
				return selected;
			},
		},
	};
	const discord = {
		channels: {
			fetch: async (id: string, options?: { force?: boolean }) => {
				expect(options?.force).toBe(true);
				events.push(`fetch:${id}`);
				return id === origin.parentId ? parent : selected;
			},
		},
	};
	const client = gateway((verb, params) => {
		events.push(verb);
		if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
		if (verb === "work.thread.claim") {
			expect(params).toEqual({ taskId, claimId: taskId });
			surface = { phase: "claimed", claimId: taskId, request: { parentOrigin } };
			return { taskId, claimId: taskId, create: true, parentOrigin, title: "Task" };
		}
		const bind = params as WorkThreadBindParams;
		expect(verb).toBe("work.thread.bind");
		expect(bind.outcome).toEqual({ kind: "bound", origin });
		surface = { phase: "bound", claimId: taskId, origin };
		return { taskId, claimId: taskId, disposition: "recorded", surface };
	});
	const first = await reconcileDiscordWorkTasks(client, discord, quiet);
	expect(first.incomplete).toBe(false);
	expect(first.origins.get(origin.conversationId)).toEqual(origin);
	expect(events).toEqual([
		"work.jobs",
		"work.thread.claim",
		`fetch:${origin.parentId}`,
		"create",
		`fetch:${origin.conversationId}`,
		"work.thread.bind",
	]);
	await reconcileDiscordWorkTasks(client, discord, quiet);
	expect(events.filter((event) => event === "create")).toHaveLength(1);
	expect(events.filter((event) => event === "work.thread.bind")).toHaveLength(1);
});

test("selected thread claims then verifies and binds without creation permission", async () => {
	const surface = { phase: "pending", request: { threadOrigin: origin } } as const;
	const events: string[] = [];
	const client = gateway((verb, params) => {
		events.push(verb);
		if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
		if (verb === "work.thread.claim")
			return { ...(params as WorkThreadClaimParams), create: false, disposition: "duplicate", surface };
		expect(params).toEqual({ taskId, claimId: taskId, outcome: { kind: "bound", origin } });
		return { taskId, claimId: taskId, disposition: "recorded", surface: { phase: "bound", origin } };
	});
	const result = await reconcileDiscordWorkTasks(
		client,
		{
			channels: {
				fetch: async (id) => {
					expect(id).toBe(origin.conversationId);
					events.push("verify");
					return thread();
				},
			},
		},
		quiet,
	);
	expect(result.incomplete).toBe(false);
	expect(events).toEqual(["work.jobs", "work.thread.claim", "verify", "work.thread.bind"]);
});

test("discovery follows nextTaskId, retains bound recovery targets, and rejects cursor loops", async () => {
	const nextOrigin = { ...origin, conversationId: "1556589606403842129" };
	const pages: unknown[] = [];
	const client = gateway((verb, params) => {
		expect(verb).toBe("work.jobs");
		pages.push(params);
		return params === undefined
			? { jobs: [], tasks: [task({ phase: "bound", origin })], taskErrors: [], nextTaskId: taskId }
			: { jobs: [], tasks: [task({ phase: "bound", origin: nextOrigin }, secondTaskId)], taskErrors: [] };
	});
	const result = await reconcileDiscordWorkTasks(client, { channels: { fetch: async (id) => thread({ id }) } }, quiet);
	expect(pages).toEqual([undefined, { afterTaskId: taskId }]);
	expect([...result.origins.keys()]).toEqual([origin.conversationId, nextOrigin.conversationId]);
	let calls = 0;
	await expect(
		reconcileDiscordWorkTasks(
			gateway(() => {
				calls++;
				return { jobs: [], tasks: [], taskErrors: [], nextTaskId: taskId };
			}),
			{
				channels: {
					fetch: async () => {
						throw new Error("must not fetch");
					},
				},
			},
			quiet,
		),
	).rejects.toThrow("cursor loop");
	expect(calls).toBe(2);
});

test("persisted duplicate create claim keeps the original claim ID and holds without creating", async () => {
	const surface = { phase: "claimed", request: { parentOrigin }, claimId } as const;
	const requests: Array<{ verb: string; params: unknown }> = [];
	const result = await reconcileDiscordWorkTasks(
		gateway((verb, params) => {
			requests.push({ verb, params });
			if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
			if (verb === "work.thread.claim") return { taskId, claimId, create: false, disposition: "duplicate", surface };
			return { taskId, claimId, disposition: "held", surface: { phase: "held", reason: "uncertain", claimId } };
		}),
		{
			channels: {
				fetch: async () => {
					throw new Error("must not create or fetch");
				},
			},
		},
		quiet,
	);
	expect(result.incomplete).toBe(true);
	expect(requests[1]?.params).toEqual({ taskId, claimId });
	expect(requests[2]?.params).toMatchObject({ taskId, claimId, outcome: { kind: "held" } });
});

test("projection tombstone beside healthy admission does not block binding across pages", async () => {
	const thirdTaskId = "44444444-4444-4444-8444-444444444444";
	const nextOrigin = { ...origin, conversationId: "1556589606403842129" };
	const diagnostics: string[] = [];
	const requests: Array<{ verb: string; params: unknown }> = [];
	const client = gateway((verb, params) => {
		requests.push({ verb, params });
		if (verb === "work.jobs")
			return params === undefined
				? {
						jobs: [],
						tasks: [task({ phase: "pending", request: { threadOrigin: origin } }, secondTaskId)],
						taskErrors: [{ taskId, reason: "corrupt task record" }],
						nextTaskId: secondTaskId,
					}
				: {
						jobs: [],
						tasks: [task({ phase: "pending", request: { threadOrigin: nextOrigin } }, thirdTaskId)],
						taskErrors: [],
					};
		const input = params as WorkThreadClaimParams;
		const target = input.taskId === secondTaskId ? origin : nextOrigin;
		expect([secondTaskId, thirdTaskId]).toContain(input.taskId);
		if (verb === "work.thread.claim")
			return {
				...input,
				create: false,
				disposition: "duplicate",
				surface: { phase: "pending", request: { threadOrigin: target } },
			};
		expect(verb).toBe("work.thread.bind");
		expect(params).toMatchObject({ outcome: { kind: "bound", origin: target } });
		return {
			taskId: input.taskId,
			claimId: input.claimId,
			disposition: "recorded",
			surface: { phase: "bound", origin: target },
		};
	});
	const result = await reconcileDiscordWorkTasks(
		client,
		{ channels: { fetch: async (id) => thread({ id }) } },
		{ error: (line) => diagnostics.push(line) },
	);
	expect(result.incomplete).toBe(true);
	expect([...result.origins.values()]).toEqual([origin, nextOrigin]);
	expect(requests.filter((entry) => entry.verb === "work.jobs").map((entry) => entry.params)).toEqual([
		undefined,
		{ afterTaskId: secondTaskId },
	]);
	expect(requests.filter((entry) => entry.verb === "work.thread.bind")).toHaveLength(2);
	expect(diagnostics).toHaveLength(1);
	expect(diagnostics[0]).toContain(`task ${taskId} projection unavailable`);
});

test("empty tombstone pages remain incomplete on repeats and retain only known recovery addressing", async () => {
	const diagnostics: string[] = [];
	const requests: string[] = [];
	const client = gateway((verb) => {
		requests.push(verb);
		return { jobs: [], tasks: [], taskErrors: [{ taskId, reason: "corrupt original mapping" }] };
	});
	const discord = {
		channels: {
			fetch: async () => {
				throw new Error("tombstone must not fetch or recreate");
			},
		},
	};
	const log = { error: (line: string) => diagnostics.push(line) };
	const unknown = await reconcileDiscordWorkTasks(client, discord, log);
	expect(unknown.incomplete).toBe(true);
	expect(unknown.origins.size).toBe(0);
	let known: ReadonlyMap<string, WorkTaskThreadOrigin> = new Map([[origin.conversationId, origin]]);
	for (let repeat = 0; repeat < 3; repeat++) {
		const result = await reconcileDiscordWorkTasks(client, discord, log, known);
		expect(result.incomplete).toBe(true);
		expect([...result.origins.values()]).toEqual([origin]);
		known = result.origins;
	}
	expect(requests).toEqual(Array(4).fill("work.jobs"));
	expect(diagnostics).toHaveLength(4);
});

for (const [label, errors] of [
	["missing", undefined],
	["null", null],
	["object", {}],
	["null entry", [null]],
	["bad identity", [{ taskId: "corrupt-json-not-an-id", reason: "bad" }]],
	["missing identity", [{ reason: "bad" }]],
	["missing reason", [{ taskId }]],
	["non-string reason", [{ taskId, reason: { text: "bad" } }]],
	["blank reason", [{ taskId, reason: " \t " }]],
	["invented origin", [{ taskId, reason: "bad", origin }]],
] as const) {
	test(`malformed projection errors (${label}) remain incomplete without blocking a healthy task`, async () => {
		const diagnostics: string[] = [];
		const requests: string[] = [];
		const client = gateway((verb, params) => {
			requests.push(verb);
			if (verb === "work.jobs")
				return {
					jobs: [],
					tasks: [task({ phase: "pending", request: { threadOrigin: origin } }, secondTaskId)],
					...(errors === undefined ? {} : { taskErrors: errors }),
				};
			expect(params).toMatchObject({ taskId: secondTaskId, claimId: secondTaskId });
			if (verb === "work.thread.claim")
				return {
					taskId: secondTaskId,
					claimId: secondTaskId,
					create: false,
					disposition: "duplicate",
					surface: { phase: "pending", request: { threadOrigin: origin } },
				};
			return {
				taskId: secondTaskId,
				claimId: secondTaskId,
				disposition: "recorded",
				surface: { phase: "bound", origin },
			};
		});
		const result = await reconcileDiscordWorkTasks(
			client,
			{ channels: { fetch: async () => thread() } },
			{ error: (line) => diagnostics.push(line) },
		);
		expect(result.incomplete).toBe(true);
		expect([...result.origins.values()]).toEqual([origin]);
		expect(requests).toEqual(["work.jobs", "work.thread.claim", "work.thread.bind"]);
		expect(diagnostics).toHaveLength(1);
		expect(diagnostics[0]).toMatch(/Invalid work.jobs taskErrors/);
	});
}

test("projection diagnostics bound reason size and count across pages without halting discovery", async () => {
	const diagnostics: string[] = [];
	let pages = 0;
	const client = gateway((verb) => {
		expect(verb).toBe("work.jobs");
		pages++;
		return pages < 3
			? {
					jobs: [],
					tasks: [],
					taskErrors: Array.from({ length: 20 }, () => ({
						taskId: crypto.randomUUID(),
						reason: `${"\n".repeat(1000)}untrusted trailer`,
					})),
					nextTaskId: pages === 1 ? taskId : secondTaskId,
				}
			: { jobs: [], tasks: [task({ phase: "bound", origin })], taskErrors: [] };
	});
	const result = await reconcileDiscordWorkTasks(
		client,
		{ channels: { fetch: async () => thread() } },
		{ error: (line) => diagnostics.push(line) },
	);
	expect(pages).toBe(3);
	expect(result.incomplete).toBe(true);
	expect(result.origins.get(origin.conversationId)).toEqual(origin);
	expect(diagnostics).toHaveLength(20);
	expect(
		diagnostics.every((line) => line.length < 2000 && !line.includes("\n") && !line.includes("untrusted trailer")),
	).toBe(true);
});

test("conflicting healthy and error identities cannot authorize a surface claim", async () => {
	const requests: string[] = [];
	const result = await reconcileDiscordWorkTasks(
		gateway((verb) => {
			requests.push(verb);
			return {
				jobs: [],
				tasks: [task({ phase: "pending", request: { parentOrigin } })],
				taskErrors: [{ taskId, reason: "corrupt" }],
			};
		}),
		{
			channels: {
				fetch: async () => {
					throw new Error("must not fetch");
				},
			},
		},
		quiet,
	);
	expect(result.incomplete).toBe(true);
	expect(result.origins.size).toBe(0);
	expect(requests).toEqual(["work.jobs"]);
});

test("lost creation response records a hold; restarting does not retry creation", async () => {
	let surface: WorkTaskProjection["surface"] = { phase: "pending", request: { parentOrigin } };
	let creates = 0;
	const client = gateway((verb, params) => {
		if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
		if (verb === "work.thread.claim") {
			surface = { phase: "claimed", request: { parentOrigin }, claimId: taskId };
			return { taskId, claimId: taskId, create: true, parentOrigin };
		}
		expect((params as WorkThreadBindParams).outcome.kind).toBe("held");
		surface = { phase: "held", claimId: taskId, reason: "ambiguous creation" };
		return { taskId, claimId: taskId, disposition: "held", surface };
	});
	const discord = {
		channels: {
			fetch: async () => ({
				...thread(),
				id: origin.parentId,
				type: ChannelType.GuildText,
				isThread: () => false,
				threads: {
					create: async () => {
						creates++;
						throw new Error("response lost after creation");
					},
				},
			}),
		},
	};
	expect((await reconcileDiscordWorkTasks(client, discord, quiet)).incomplete).toBe(true);
	await reconcileDiscordWorkTasks(client, discord, quiet);
	expect(creates).toBe(1);
});

test("lost claim response cannot authorize a creation on rediscovery", async () => {
	let surface: WorkTaskProjection["surface"] = { phase: "pending", request: { parentOrigin } };
	let claims = 0;
	let holds = 0;
	const client = gateway((verb) => {
		if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
		if (verb === "work.thread.claim") {
			if (++claims === 1) {
				surface = { phase: "claimed", request: { parentOrigin }, claimId: taskId };
				throw new Error("lost claim response");
			}
			return { taskId, claimId: taskId, create: false, disposition: "duplicate", surface };
		}
		holds++;
		return { taskId, claimId: taskId, disposition: "held", surface: { phase: "held", reason: "unknown" } };
	});
	let fetches = 0;
	const discord = {
		channels: {
			fetch: async () => {
				fetches++;
				return thread();
			},
		},
	};
	await reconcileDiscordWorkTasks(client, discord, quiet);
	await reconcileDiscordWorkTasks(client, discord, quiet);
	expect(fetches).toBe(0);
	expect(holds).toBe(1);
});

test("selected archived thread holds; a bound archived target remains mapped but is not rebound", async () => {
	for (const bound of [false, true]) {
		const surface: WorkTaskProjection["surface"] = bound
			? { phase: "bound", origin, claimId }
			: { phase: "pending", request: { threadOrigin: origin } };
		const requests: Array<{ verb: string; params: unknown }> = [];
		const fetched: string[] = [];
		const result = await reconcileDiscordWorkTasks(
			gateway((verb, params) => {
				requests.push({ verb, params });
				if (verb === "work.jobs") return { jobs: [], tasks: [task(surface)], taskErrors: [] };
				if (verb === "work.thread.claim")
					return { taskId, claimId: taskId, create: false, disposition: "duplicate", surface };
				expect((params as WorkThreadBindParams).outcome.kind).toBe("held");
				return { taskId, claimId: taskId, disposition: "held", surface: { phase: "held", reason: "archived" } };
			}),
			{
				channels: {
					fetch: async (id) => {
						fetched.push(id);
						return thread({
							archived: true,
							setArchived: async () => {
								throw new Error("must never reactivate");
							},
						});
					},
				},
			},
			quiet,
		);
		expect(result.incomplete).toBe(true);
		expect(fetched).toEqual([origin.conversationId]);
		expect(requests.map((entry) => entry.verb)).toEqual(
			bound ? ["work.jobs"] : ["work.jobs", "work.thread.claim", "work.thread.bind"],
		);
		if (bound) expect(result.origins.get(origin.conversationId)).toEqual(origin);
	}
});

test("unsupported forum parent holds rather than inventing an alternate thread", async () => {
	const requests: string[] = [];
	let creates = 0;
	await reconcileDiscordWorkTasks(
		gateway((verb, params) => {
			requests.push(verb);
			if (verb === "work.jobs")
				return { jobs: [], tasks: [task({ phase: "pending", request: { parentOrigin } })], taskErrors: [] };
			if (verb === "work.thread.claim") return { taskId, claimId: taskId, create: true, parentOrigin };
			expect((params as WorkThreadBindParams).outcome.kind).toBe("held");
			return { taskId, claimId: taskId, disposition: "held", surface: { phase: "held", reason: "unsupported" } };
		}),
		{
			channels: {
				fetch: async () => ({
					...thread(),
					id: origin.parentId,
					type: ChannelType.GuildForum,
					isThread: () => false,
					threads: {
						create: async () => {
							creates++;
							return thread();
						},
					},
				}),
			},
		},
		quiet,
	);
	expect(creates).toBe(0);
	expect(requests).toEqual(["work.jobs", "work.thread.claim", "work.thread.bind"]);
});

for (const [label, value] of [
	["deleted", null],
	["archived", thread({ archived: true })],
	["locked", thread({ locked: true })],
	["wrong parent", thread({ parentId: "123" })],
	["wrong guild", thread({ guildId: "123" })],
	["not a thread", thread({ isThread: () => false })],
	["no access", thread({ permissionsFor: () => ({ has: () => false }) })],
] as const) {
	test(`mapped-only ${label} delivery never falls back, unarchives, or confirms`, async () => {
		const fetched: string[] = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleDiscordDelivery(
			gateway((verb, params) => {
				requests.push({ verb, params });
			}),
			{
				channels: {
					fetch: async (id) => {
						fetched.push(id);
						return value;
					},
				},
			},
			message(),
		);
		expect(fetched).toEqual([origin.conversationId]);
		expect(requests).toHaveLength(1);
		expect(requests[0]).toMatchObject({
			verb: "delivery.fail",
			params: { deliveryId: "delivery-original", ambiguous: false },
		});
	});
}

test("malformed mapped metadata is rejected before any Discord call", async () => {
	let fetches = 0;
	const requests: string[] = [];
	await settleDiscordDelivery(
		gateway((verb) => {
			requests.push(verb);
		}),
		{
			channels: {
				fetch: async () => {
					fetches++;
					return thread();
				},
			},
		},
		message({ workTask: { taskId, opRef: "op-original", sourceId: "source-original", mappedOnly: false } as never }),
	);
	expect(fetches).toBe(0);
	expect(requests).toEqual(["delivery.fail"]);
});

test("complete multichunk mapped text confirms ordered real IDs only after all sends, without persona presence", async () => {
	const events: string[] = [];
	const requests: Array<{ verb: string; params: unknown }> = [];
	let sequence = 100;
	const typing: TypingPort = {
		begin: () => events.push("begin"),
		end: () => events.push("end"),
		refresh: () => events.push("refresh"),
	};
	const selected = thread({
		send: async () => {
			const id = String(++sequence);
			events.push(id);
			return { id, channelId: origin.conversationId };
		},
	});
	await settleDiscordDelivery(
		gateway((verb, params) => {
			events.push(verb);
			requests.push({ verb, params });
		}),
		{ channels: { fetch: async () => selected } },
		message({ text: "x".repeat(4500) }),
		typing,
		{ clear: async () => events.push("clear") } as never,
	);
	expect(events).toEqual(["101", "102", "103", "delivery.confirm"]);
	expect(requests[0]?.params).toEqual({
		deliveryId: "delivery-original",
		platformReceipt: { origin, messageIds: ["101", "102", "103"] },
	});
});

for (const id of [undefined, null, 123, "", "0", "01", "-1", "18446744073709551616", "not-an-id"]) {
	test(`mapped send returning invalid ID ${String(id)} is ambiguous, not confirmed`, async () => {
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleDiscordDelivery(
			gateway((verb, params) => {
				requests.push({ verb, params });
			}),
			{ channels: { fetch: async () => thread({ send: async () => ({ id }) }) } },
			message(),
		);
		expect(requests).toHaveLength(1);
		expect(requests[0]).toMatchObject({ verb: "delivery.fail", params: { ambiguous: true } });
	});
}

test("duplicate IDs or receipts from a different channel cannot confirm a mapped payload", async () => {
	for (const wrongChannel of [false, true]) {
		const verbs: string[] = [];
		await settleDiscordDelivery(
			gateway((verb) => {
				verbs.push(verb);
			}),
			{
				channels: {
					fetch: async () =>
						thread({
							send: async () => ({ id: "100", channelId: wrongChannel ? origin.parentId : origin.conversationId }),
						}),
				},
			},
			message({ text: "x".repeat(2100) }),
		);
		expect(verbs).toEqual(["delivery.fail"]);
	}
});

test("partial and ambiguous sends never confirm a prefix; duplicate retry retains delivery identity", async () => {
	for (const failAt of [1, 2]) {
		let sent = 0;
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleDiscordDelivery(
			gateway((verb, params) => {
				requests.push({ verb, params });
			}),
			{
				channels: {
					fetch: async () =>
						thread({
							send: async () => {
								if (++sent === failAt)
									throw Object.assign(new Error("send failed"), failAt === 2 ? { code: 50013 } : {});
								return { id: String(100 + sent) };
							},
						}),
				},
			},
			message({ text: "x".repeat(2100) }),
		);
		expect(requests).toHaveLength(1);
		expect(requests[0]).toMatchObject({
			verb: "delivery.fail",
			params: { deliveryId: "delivery-original", ambiguous: true },
		});
	}
	const sent: unknown[] = [];
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(
		gateway((verb, params) => {
			requests.push({ verb, params });
		}),
		{
			channels: {
				fetch: async () =>
					thread({
						send: async (body: unknown) => {
							sent.push(body);
							return { id: "105" };
						},
					}),
			},
		},
		message({ duplicateWarning: true }),
	);
	expect(sent).toEqual(["[recovered - may be a duplicate] result"]);
	expect(requests[0]).toMatchObject({ verb: "delivery.confirm", params: { deliveryId: "delivery-original" } });
});

test("archiving between chunks stops the payload without reactivation or prefix confirmation", async () => {
	let sends = 0;
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(
		gateway((verb, params) => {
			requests.push({ verb, params });
		}),
		{
			channels: {
				fetch: async () =>
					thread({
						archived: sends > 0,
						send: async () => {
							sends++;
							return { id: "101" };
						},
						setArchived: async () => {
							throw new Error("must never reactivate");
						},
					}),
			},
		},
		message({ text: "x".repeat(2100) }),
	);
	expect(sends).toBe(1);
	expect(requests).toHaveLength(1);
	expect(requests[0]).toMatchObject({ verb: "delivery.fail", params: { ambiguous: true } });
});

test("lost whole-payload confirmation is reported ambiguous with the original ledger ID", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(
		gateway((verb, params) => {
			requests.push({ verb, params });
			if (verb === "delivery.confirm") throw new Error("gateway acknowledgement lost");
		}),
		{ channels: { fetch: async () => thread() } },
		message(),
	);
	expect(requests.map((entry) => entry.verb)).toEqual(["delivery.confirm", "delivery.fail"]);
	expect(requests[1]?.params).toMatchObject({ deliveryId: "delivery-original", ambiguous: true });
});

test("ordinary thread delivery still falls back to parent and confirms without a mapped receipt", async () => {
	const fetched: string[] = [];
	const bodies: unknown[] = [];
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(
		gateway((verb, params) => {
			requests.push({ verb, params });
		}),
		{
			channels: {
				fetch: async (id) => {
					fetched.push(id);
					return id === origin.conversationId
						? thread({
								archived: true,
								setArchived: async () => {
									throw new Error("cannot reopen");
								},
							})
						: {
								send: async (body: unknown) => {
									bodies.push(body);
								},
							};
				},
			},
		},
		message({ workTask: undefined }),
	);
	expect(fetched).toEqual([origin.conversationId, origin.parentId]);
	expect(bodies).toHaveLength(1);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-original" } }]);
});

test("live and edit mapped ingress retain creation evidence and actual author without persona typing", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-task-ingress-"));
	try {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const presence: string[] = [];
		const client = gateway((verb, params) => {
			requests.push({ verb, params });
			return {
				route: "work_task",
				taskId,
				controlId: "control-original",
				opRef: "op-original",
				acceptance: "durable",
				delivery: "pending",
			};
		});
		const gw = new ReconnectingGateway(
			"unused",
			{ channels: { fetch: async () => undefined } },
			{ channels: {} } as never,
			{ begin: () => presence.push("begin"), end: () => presence.push("end"), refresh: () => presence.push("refresh") },
			{ arm: () => presence.push("arm") } as never,
			join(home, "cursor.json"),
			() => me,
			client as never,
		);
		const created = Date.now() - 1000;
		const source = inbound(created);
		const engagement = decideInbound(source, me, { [origin.parentId]: { engagement: "mention-open" } });
		expect(engagement).toMatchObject({ mentioned: false, authorId: "800", authorName: "Owner", authorHandle: "owner" });
		await gw.requestInbound(
			source.id,
			origin,
			source.content,
			engagement!,
			new Date(created).toISOString(),
			false,
			source.createdTimestamp,
		);
		const edit = describeMessageEdit({ ...source, content: "revised", editedTimestamp: created + 500 }, me, undefined)!;
		gw.sendEdit(edit.messageId, edit.origin, edit.text, edit.engagement, edit.receivedAt, edit.originSource);
		for (let tick = 0; tick < 20 && gw.pendingEdits.length > 0; tick++) await Promise.resolve();
		expect(gw.pendingEdits).toHaveLength(0);
		expect(requests.map((entry) => entry.verb)).toEqual(["chat.send", "chat.edit"]);
		expect(requests[0]?.params).toMatchObject({
			messageId: source.id,
			originSource: { platformCreatedAt: created },
			engagement: { authorId: "800", mentioned: false },
		});
		expect(requests[1]?.params).toMatchObject({
			messageId: source.id,
			originSource: { platformCreatedAt: created },
			receivedAt: new Date(created + 500).toISOString(),
		});
		expect(presence).toEqual([]);
		await gw.cursorsFlushed;
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("existing recovery pass discovers a bound task outside configured channels and preserves creation time", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-task-recovery-"));
	try {
		const source = inbound(Date.now() - 1000);
		const sends: ChatSendParams[] = [];
		let historyReads = 0;
		let tombstone = false;
		const selected = thread({
			messages: {
				fetch: async () => {
					historyReads++;
					return historyReads === 1 ? [source] : [];
				},
			},
		});
		const client = gateway((verb, params) => {
			if (verb === "work.jobs")
				return tombstone
					? { jobs: [], tasks: [], taskErrors: [{ taskId, reason: "corrupt mapping" }] }
					: { jobs: [], tasks: [task({ phase: "bound", origin })], taskErrors: [] };
			expect(verb).toBe("chat.send");
			sends.push(params as ChatSendParams);
			return {
				route: "work_task",
				taskId,
				controlId: "control-original",
				opRef: "op-original",
				acceptance: "durable",
				delivery: "pending",
			};
		});
		const discord: DiscordClientLike = {
			channels: {
				fetch: async (id) => {
					expect(id).toBe(origin.conversationId);
					return selected;
				},
			},
		};
		const gw = new ReconnectingGateway(
			"unused",
			discord,
			{ channels: {} } as never,
			undefined,
			undefined,
			join(home, "cursor.json"),
			() => me,
			client as never,
			async () => {},
		);
		await gw.recoverMissedMessages();
		expect(gw.recoveryRetryPending).toBe(false);
		expect(historyReads).toBe(1);
		expect(sends).toHaveLength(1);
		expect(sends[0]).toMatchObject({
			messageId: source.id,
			origin,
			originSource: { platformCreatedAt: source.createdTimestamp, recovered: true },
			engagement: { authorId: "800", mentioned: false },
		});
		tombstone = true;
		try {
			for (let repeat = 0; repeat < 2; repeat++) {
				await gw.recoverMissedMessages();
				expect(gw.recoveryRetryPending).toBe(true);
				expect(historyReads).toBe(repeat + 2);
				expect(sends).toHaveLength(1);
			}
		} finally {
			// End the existing retry timer without introducing a separate scheduler.
			tombstone = false;
			await gw.recoverMissedMessages();
		}
		expect(gw.recoveryRetryPending).toBe(false);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

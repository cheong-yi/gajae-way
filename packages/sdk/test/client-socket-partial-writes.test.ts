import { describe, expect, test } from "bun:test";
import type { Frame, FrameWriterSink, RequestFrame } from "@gajae-gateway/protocol";
import { encodeFrame, FrameDecoder, OrderedFrameWriter, PROFILE_VERSION } from "@gajae-gateway/protocol";
import { GajaewayClient } from "../src/client";

describe("SDK socket partial writes (issue #420 acceptance)", () => {
	test("64KB chat.send arrives intact with stub sink accepting 64 bytes per write", async () => {
		const received: Buffer[] = [];
		let writeCount = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Simulate small send buffer: only accept 64 bytes at a time
				const accepted = Math.min(bytes.length, 64);
				received.push(Buffer.from(bytes.subarray(0, accepted)));
				// If not all accepted, signal that drain will be needed
				if (accepted < bytes.length && sinkState.writer) {
					// Schedule drain asynchronously
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		// Create a 64KB+ text payload (Korean characters are 3 bytes in UTF-8)
		const largeText = "안녕하세요 ".repeat(6400); // ~80KB in UTF-8
		const largeFrame: Frame = {
			v: "0.1",
			type: "request",
			id: "large-1",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: largeText },
		};

		writer.write(largeFrame);
		await writer.settled();

		// Verify all bytes were written through multiple write() calls
		const combined = Buffer.concat(received);
		expect(writeCount).toBeGreaterThan(1); // Should require multiple writes

		// Verify the frame decodes correctly from combined bytes
		const decoder = new FrameDecoder();
		const frames = decoder.feed(combined.toString("utf-8"));
		expect(frames.length).toBe(1);
		expect(frames[0].type).toBe("request");
		if (frames[0].type === "request") {
			expect(frames[0].id).toBe("large-1");
			// biome-ignore lint/suspicious/noExplicitAny: test convenience
			const params = frames[0].params as any;
			expect(params.text).toBe(largeText);
		}
	});

	test("burst of 50 4KB frames all arrive intact with stub sink accepting 512 bytes per write", async () => {
		const received: Buffer[] = [];
		let writeCount = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Simulate small send buffer: only accept 512 bytes at a time
				const accepted = Math.min(bytes.length, 512);
				received.push(Buffer.from(bytes.subarray(0, accepted)));
				if (accepted < bytes.length && sinkState.writer) {
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		// Send 50 frames, each with ~4KB of data
		const frameData = "x".repeat(4000);
		for (let i = 0; i < 50; i++) {
			const frame: Frame = {
				v: "0.1",
				type: "request",
				id: String(i),
				verb: "chat.send",
				params: { origin: { platform: "slack", id: "C123" }, text: frameData },
			};
			writer.write(frame);
		}
		await writer.settled();

		// Verify all bytes were written through multiple write() calls
		const combined = Buffer.concat(received);
		expect(writeCount).toBeGreaterThan(50); // Should require many writes due to small buffer

		// Verify all 50 frames decode correctly in order
		const decoder = new FrameDecoder();
		const frames = decoder.feed(combined.toString("utf-8"));
		expect(frames.length).toBe(50);

		for (let i = 0; i < 50; i++) {
			const frame = frames[i];
			expect(frame.type).toBe("request");
			if (frame.type === "request") {
				expect(frame.id).toBe(String(i));
				expect(frame.verb).toBe("chat.send");
			}
		}
	});

	test("partial writes are retried until complete (no data loss)", async () => {
		let writeCount = 0;
		let totalWritten = 0;

		const sinkState: { writer?: OrderedFrameWriter } = {};
		const sink: FrameWriterSink = {
			write: (bytes: Uint8Array) => {
				writeCount++;
				// Only accept 128 bytes per write
				const accepted = Math.min(bytes.length, 128);
				totalWritten += accepted;
				if (accepted < bytes.length && sinkState.writer) {
					setTimeout(() => sinkState.writer?.drain(), 0);
				}
				return accepted;
			},
			close: () => {},
		};

		const writer = new OrderedFrameWriter(sink);
		sinkState.writer = writer;

		const frame: Frame = {
			v: "0.1",
			type: "request",
			id: "partial-test",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: "test".repeat(100) },
		};

		writer.write(frame);
		await writer.settled();

		// Verify that write was called multiple times (partial writes)
		expect(writeCount).toBeGreaterThan(1);
		// Verify total bytes written equals frame size
		const frameBytes = Buffer.byteLength(encodeFrame(frame), "utf-8");
		expect(totalWritten).toBe(frameBytes);
	});
});

type InjectedSocket = {
	write(attempt: Uint8Array): number;
	end(): void;
};

type InjectedHandlers = {
	open(socket: InjectedSocket): void;
	data(socket: InjectedSocket, data: Buffer): void;
	close(socket: InjectedSocket): void;
	drain(socket: InjectedSocket): void;
};

type InjectedGatewayConfig = {
	/** Bytes accepted per sink write; 0 stalls the writer until an external drain (never-drain mode). */
	accept?: (attempt: Uint8Array) => number;
	/** Terminal sink failure for the matching attempt; write #1 (negotiation) stays healthy. */
	writeError?: (attempt: Uint8Array, writes: number) => Error | undefined;
};

type InjectedGateway = {
	/** Frames decoded from the real client's writes, in FIFO order. */
	received: Frame[];
	/** Sink write attempts observed on the injected socket. */
	writes(): number;
	/** Deliver a fake gateway frame to the client. */
	send(frame: Frame): void;
	/** Deliver raw gateway bytes as separate socket data events. */
	sendRaw(chunks: Uint8Array[]): void;
};

/**
 * Replaces only OS socket creation, mirroring the B8 reproduction harness:
 * negotiation, the ordered frame writer, request timers and close run unchanged.
 */
async function withInjectedGateway(
	config: InjectedGatewayConfig,
	body: (gateway: InjectedGateway) => Promise<void>,
): Promise<void> {
	const bun = Bun as unknown as { connect: unknown };
	const originalConnect = bun.connect;
	const received: Frame[] = [];
	let writes = 0;
	let session: { socket: InjectedSocket; handlers: InjectedHandlers } | undefined;
	const sendRaw = (chunks: Uint8Array[]): void => {
		if (!session) throw new Error("injected gateway is not connected");
		for (const chunk of chunks) session.handlers.data(session.socket, Buffer.from(chunk));
	};
	const send = (frame: Frame): void => sendRaw([Buffer.from(encodeFrame(frame))]);
	bun.connect = async (options: { socket: InjectedHandlers }): Promise<InjectedSocket> => {
		const decoder = new TextDecoder();
		let inbound = "";
		const socket: InjectedSocket = {
			write(attempt: Uint8Array): number {
				writes++;
				const failure = config.writeError?.(attempt, writes);
				if (failure) throw failure;
				const accepted = Math.min(attempt.length, config.accept?.(attempt) ?? attempt.length);
				if (accepted > 0) {
					// Stream-decode accepted bytes: a UTF-8 codepoint may straddle accepts.
					inbound += decoder.decode(attempt.subarray(0, accepted), { stream: true });
					let newline = inbound.indexOf("\n");
					while (newline >= 0) {
						const line = inbound.slice(0, newline);
						inbound = inbound.slice(newline + 1);
						const frame = JSON.parse(line) as Frame;
						received.push(frame);
						if (frame.type === "hello")
							send({
								v: PROFILE_VERSION,
								type: "negotiated",
								payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
							});
						newline = inbound.indexOf("\n");
					}
				}
				// A partial accept waits for drain; a zero accept stalls until close.
				if (accepted > 0 && accepted < attempt.length) setTimeout(() => options.socket.drain(socket), 0);
				return accepted;
			},
			end(): void {
				options.socket.close(socket);
			},
		};
		session = { socket, handlers: options.socket };
		options.socket.open(socket);
		return socket;
	};
	try {
		await body({
			received,
			writes: () => writes,
			send,
			sendRaw,
		});
	} finally {
		bun.connect = originalConnect;
	}
}

async function until(predicate: () => boolean): Promise<void> {
	const deadline = Date.now() + 1_000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for injected gateway");
		await Bun.sleep(5);
	}
}

/** B8 seam: negotiation is fully accepted, then the sink never drains again. */
function acceptHelloThenStall(attempt: Uint8Array): number {
	return Buffer.from(attempt).toString("utf8").includes('"hello"') ? attempt.length : 0;
}

describe("GajaewayClient ordered socket writes (B8 composition)", () => {
	test("a never-draining request reaches its deadline without an unhandled rejection", async () => {
		const unhandled: unknown[] = [];
		const observe = (reason: unknown): void => {
			unhandled.push(reason);
		};
		process.on("unhandledRejection", observe);
		try {
			await withInjectedGateway({ accept: acceptHelloThenStall }, async (gateway) => {
				const client = await GajaewayClient.connectSocket("injected-never-drain", { requestTimeoutMs: 50 });
				try {
					let disconnects = 0;
					client.onDisconnect(() => disconnects++);
					const observation: {
						state: "pending" | "resolved" | "rejected";
						failure?: Error;
					} = { state: "pending" };
					const outcome = client.request("gateway.status").then(
						() => {
							observation.state = "resolved";
						},
						(error: Error) => {
							observation.state = "rejected";
							observation.failure = error;
						},
					);
					await new Promise<void>((resolve) => setTimeout(resolve, 200));
					// Regression: the caller observes the deadline while the writer is stalled,
					// instead of the timeout rejection surfacing unobserved until close.
					expect(observation.state).toBe("rejected");
					expect(observation.failure?.message).toBe("request timed out after 50ms");
					expect(unhandled).toEqual([]);
					// No timeout-driven global disconnect and no replayed request write.
					expect(disconnects).toBe(0);
					expect(gateway.writes()).toBe(2);
					await outcome;
					expect(unhandled).toEqual([]);
				} finally {
					// Release the stalled writer even when an assertion fails first.
					await client.close();
				}
			});
		} finally {
			process.off("unhandledRejection", observe);
		}
	});

	test("the actual client preserves frame order and payload across ordered partial writes", async () => {
		await withInjectedGateway({ accept: (attempt) => Math.min(attempt.length, 512) }, async (gateway) => {
			const client = await GajaewayClient.connectSocket("injected-partial-writes", { requestTimeoutMs: 5_000 });
			try {
				const text = "안녕하세요 ".repeat(300);
				const first = client.request<string>("chat.send", { seq: 1, text });
				const second = client.request<string>("chat.send", { seq: 2, text });
				// Keep an early assertion failure from leaking these as unhandled rejections.
				void first.catch(() => {});
				void second.catch(() => {});
				await until(() => gateway.received.filter((frame) => frame.type === "request").length === 2);
				const requests = gateway.received.filter((frame): frame is RequestFrame => frame.type === "request");
				// Frames decode intact and in launch order; interleaved partial writes would
				// corrupt the NDJSON stream before these assertions could pass.
				expect(requests.map((frame) => [frame.id, (frame.params as { seq: number }).seq])).toEqual([
					["1", 1],
					["2", 2],
				]);
				for (const frame of requests) expect((frame.params as { text: string }).text).toBe(text);
				// Negotiation plus multiple partial accepts per frame prove the retry loop ran.
				expect(gateway.writes()).toBeGreaterThan(3);
				gateway.send({ v: PROFILE_VERSION, type: "response", id: "1", result: "first" });
				gateway.send({ v: PROFILE_VERSION, type: "response", id: "2", result: "second" });
				expect(await first).toBe("first");
				expect(await second).toBe("second");
			} finally {
				await client.close();
			}
		});
	});

	test("a terminal sink write failure rejects concurrent pending requests once without replay", async () => {
		const writeFailure = new Error("socket write failed");
		await withInjectedGateway(
			{
				// Negotiation write #1 succeeds; the request write fails terminally.
				writeError: (_attempt, writes) => (writes > 1 ? writeFailure : undefined),
			},
			async (gateway) => {
				const client = await GajaewayClient.connectSocket("injected-write-failure", {
					requestTimeoutMs: 5_000,
				});
				try {
					let disconnects = 0;
					client.onDisconnect(() => disconnects++);
					// Both requests are pending when the terminal failure hits. Observe both
					// before yielding: #fail rejects them together, so sequential awaits can
					// leave the second caller's rejection temporarily unhandled.
					const first = client.request("gateway.status");
					const second = client.request("gateway.status");
					const outcomes = await Promise.allSettled([first, second]);
					for (const outcome of outcomes) {
						expect(outcome.status).toBe("rejected");
						if (outcome.status === "rejected") expect(outcome.reason).toBe(writeFailure);
					}
					expect(disconnects).toBe(1);
					await expect(client.request("after failure")).rejects.toThrow("client is not connected");
					expect(gateway.writes()).toBe(2);
				} finally {
					await client.close();
				}
			},
		);
	});
});

describe("GajaewayClient receive path (B9 deterministic endpoint split)", () => {
	test("a response split inside a multibyte codepoint recovers the exact payload", async () => {
		await withInjectedGateway({ accept: (attempt) => Math.min(attempt.length, 512) }, async (gateway) => {
			const client = await GajaewayClient.connectSocket("injected-split-response", { requestTimeoutMs: 5_000 });
			try {
				let disconnects = 0;
				client.onDisconnect(() => disconnects++);
				const result = { text: "응답: 한글 처리" };
				const pending = client.request<{ text: string }>("gateway.status");
				// Keep an early assertion failure from leaking the pending request as an unhandled rejection.
				void pending.catch(() => {});
				await until(() => gateway.received.some((frame) => frame.type === "request" && frame.id === "1"));
				const bytes = Buffer.from(encodeFrame({ v: PROFILE_VERSION, type: "response", id: "1", result }));
				const lead = bytes.findIndex((byte) => byte >= 0xc0);
				expect(lead).toBeGreaterThan(0);
				// A continuation byte follows the cut: the boundary sits inside one codepoint.
				expect(bytes[lead + 1] & 0xc0).toBe(0x80);
				const writesBefore = gateway.writes();
				gateway.sendRaw([bytes.subarray(0, lead + 1), bytes.subarray(lead + 1)]);
				expect(await pending).toEqual(result);
				// Receive-side recovery is writer-silent: no replayed write, no disconnect.
				expect(gateway.writes()).toBe(writesBefore);
				expect(disconnects).toBe(0);
			} finally {
				await client.close();
			}
		});
	});

	test("an event split inside a multibyte codepoint reaches the subscriber with the exact payload", async () => {
		await withInjectedGateway({}, async (gateway) => {
			const client = await GajaewayClient.connectSocket("injected-split-event", { requestTimeoutMs: 5_000 });
			try {
				const payload = { text: "이벤트: 안녕하세요", final: true };
				let seen: unknown;
				client.on("chat.message", (value) => {
					seen = value;
				});
				const bytes = Buffer.from(encodeFrame({ v: PROFILE_VERSION, type: "event", event: "chat.message", payload }));
				const lead = bytes.findIndex((byte) => byte >= 0xc0);
				expect(lead).toBeGreaterThan(0);
				expect(bytes[lead + 1] & 0xc0).toBe(0x80);
				gateway.sendRaw([bytes.subarray(0, lead + 1), bytes.subarray(lead + 1)]);
				await until(() => seen !== undefined);
				expect(seen).toEqual(payload);
			} finally {
				await client.close();
			}
		});
	});
});

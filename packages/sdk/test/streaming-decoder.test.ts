import { describe, expect, test } from "bun:test";
import type { Frame } from "@gajae-gateway/protocol";
import { encodeFrame, FrameDecoder } from "@gajae-gateway/protocol";

describe("FrameDecoder unit: deterministic multibyte split (issue #420 acceptance)", () => {
	test("streaming decode across a split inside one codepoint recovers the exact frame", () => {
		const frame: Frame = {
			v: "0.1",
			type: "request",
			id: "multibyte-test",
			verb: "chat.send",
			params: { origin: { platform: "slack", id: "C123" }, text: "한글 테스트" },
		};
		const bytes = Buffer.from(encodeFrame(frame));
		const lead = bytes.findIndex((byte) => byte >= 0xc0);
		expect(lead).toBeGreaterThan(0);
		// A continuation byte follows the cut: the boundary sits inside one codepoint.
		expect(bytes[lead + 1] & 0xc0).toBe(0x80);

		const decoder = new TextDecoder();
		const first = decoder.decode(bytes.subarray(0, lead + 1), { stream: true });
		const second = decoder.decode(bytes.subarray(lead + 1), { stream: true });
		const text = first + second;
		expect(text).not.toContain("\uFFFD");
		expect(new FrameDecoder().feed(text)).toEqual([frame]);
	});
});

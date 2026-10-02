import { describe, expect, test } from "bun:test";
import { memoryAuditLog } from "../src/audit";
import { createAdminApp, startAdminServer } from "../src/server";

const body = { operationId: "ops.integrity", actor: "self-typed", confirm: "ops.integrity" };

async function attempt(login: string | undefined, payload: unknown = body, trust = true, mutationsEnabled = true) {
	const auditLog = memoryAuditLog();
	const calls: string[] = [];
	const app = createAdminApp({
		trustTailscaleLogin: trust,
		gate: { mutationsEnabled },
		auditLog,
		reconcileMs: 0,
		request: async (method, params) => {
			calls.push(method);
			if (method === "monitor.inspect") {
				if ((params as { monitorId?: string })?.monitorId === "found") return { monitor: { name: "real-name" } };
				throw new Error("fixture missing target");
			}
			return {};
		},
	});
	try {
		const headers = new Headers({ "content-type": "application/json" });
		if (login !== undefined) headers.set("Tailscale-User-Login", login);
		const response = await app.handler(
			new Request("http://not-the-listener.example/api/mutations", {
				method: "POST",
				headers,
				body: JSON.stringify(payload),
			}),
		);
		return { status: response.status, entries: await auditLog.tail(10), calls };
	} finally {
		app.stop();
	}
}

describe("opt-in Tailscale audit attribution", () => {
	test("trusted login is the actor; typed name remains a label", async () => {
		const result = await attempt("owner@example.com");
		expect(result.status).toBe(200);
		expect(result.entries[0]).toMatchObject({
			actor: "owner@example.com",
			actorLabel: "self-typed",
			actorSource: "tailscale-proxy",
			decision: "allowed",
		});
		expect(result.calls).toContain("ops.integrity");
	});

	test("absent identity and opt-out retain ordinary audit semantics", async () => {
		for (const [login, trust] of [
			[undefined, true],
			["forged@example.com", false],
		] as const) {
			const result = await attempt(login, body, trust);
			expect(result.status).toBe(200);
			expect(result.entries[0]?.actor).toBe("self-typed");
			expect(result.entries[0]?.actorSource).toBeUndefined();
			expect(result.entries[0]?.actorLabel).toBeUndefined();
		}
	});

	test("Go MIME Q UTF-8 identity decoding", async () => {
		const result = await attempt("=?UTF-8?q?=EC=B2=AD@example.com?=");
		expect(result.entries[0]?.actor).toBe("청@example.com");
		const split = await attempt("=?UTF-8?q?=EC=B2=AD?= =?UTF-8?q?@example.com?=");
		expect(split.entries[0]?.actor).toBe("청@example.com");
	});

	test("malformed and duplicate identities never receive proxy attribution", async () => {
		for (const login of [
			"a@example.com, b@example.com",
			"=?utf-8?q?=ZZ?=",
			"=?utf-8?q?=FF?=",
			"=?utf-8?q?=EF=BB=BFuser?=",
			"=?utf-8?b?YQ==?=",
			"=?utf-8?q?a=0Ab?=",
			"=?utf-8?q?a_b?=",
			"=?utf-8?q??=",
			"a b",
			"x".repeat(1025),
		]) {
			const result = await attempt(login);
			expect(result.status).toBe(200);
			expect(result.entries[0]?.actor).toBe("self-typed");
			expect(result.entries[0]?.actorSource).toBeUndefined();
		}
		const headers = new Headers();
		headers.append("Tailscale-User-Login", "one@example.com");
		headers.append("Tailscale-User-Login", "two@example.com");
		const result = await attempt(headers.get("Tailscale-User-Login") ?? undefined);
		expect(result.entries[0]?.actorSource).toBeUndefined();
	});

	test("gate refusals preserve attribution without bypassing actor or confirmation validation", async () => {
		for (const [payload, status] of [
			[{ ...body, actor: "" }, 401],
			[{ ...body, actor: "\u200b" }, 401],
			[{ ...body, confirm: "wrong" }, 428],
			[{ ...body, operationId: "unknown" }, 404],
		] as const) {
			const result = await attempt("owner@example.com", payload);
			expect(result.status).toBe(status);
			expect(result.entries[0]).toMatchObject({
				actor: "owner@example.com",
				actorSource: "tailscale-proxy",
				decision: "rejected",
			});
			expect(result.calls).not.toContain("ops.integrity");
		}
	});

	test("target-resolution refusals use the same attribution", async () => {
		for (const params of [undefined, { monitorId: "missing" }]) {
			const result = await attempt("owner@example.com", { ...body, operationId: "monitor.remove", params });
			expect(result.status).toBe(params ? 502 : 400);
			expect(result.entries[0]).toMatchObject({
				actor: "owner@example.com",
				actorLabel: "self-typed",
				actorSource: "tailscale-proxy",
				decision: "rejected",
			});
		}
	});

	test("disabled mutations and target-confirmation mismatch retain attribution", async () => {
		const disabled = await attempt("owner@example.com", body, true, false);
		expect(disabled.status).toBe(403);
		expect(disabled.entries[0]).toMatchObject({
			actor: "owner@example.com",
			actorLabel: "self-typed",
			decision: "rejected",
		});
		const mismatch = await attempt("owner@example.com", {
			...body,
			operationId: "monitor.remove",
			params: { monitorId: "found" },
			targetConfirm: "wrong",
		});
		expect(mismatch.status).toBe(428);
		expect(mismatch.entries[0]).toMatchObject({
			actor: "owner@example.com",
			actorLabel: "self-typed",
			decision: "rejected",
		});
		expect(mismatch.calls).not.toContain("monitor.remove");
	});

	test("configured bind, not URL or forwarded headers, determines trust", () => {
		for (const hostname of ["0.0.0.0", "::", "localhost", "192.0.2.1", "[::1]"]) {
			expect(() =>
				startAdminServer({ hostname, trustTailscaleLogin: true, request: async () => ({}), reconcileMs: 0 }),
			).toThrow("requires a 127.0.0.1 or ::1 listener");
		}
	});

	test("real wildcard listener ignores identity when trust is off", async () => {
		const auditLog = memoryAuditLog();
		const server = startAdminServer({
			hostname: "0.0.0.0",
			port: 0,
			auditLog,
			request: async () => ({}),
			reconcileMs: 0,
		});
		try {
			const response = await fetch(`http://127.0.0.1:${server.port}/api/mutations`, {
				method: "POST",
				headers: {
					"content-type": "application/json",
					"Tailscale-User-Login": "forged@example.com",
					"X-Forwarded-Host": "127.0.0.1",
				},
				body: JSON.stringify(body),
			});
			expect(response.status).toBe(200);
			expect((await auditLog.tail(1))[0]?.actor).toBe("self-typed");
		} finally {
			server.stop();
		}
	});

	test("real IPv6 loopback listener supports trusted attribution", async () => {
		const auditLog = memoryAuditLog();
		const server = startAdminServer({
			hostname: "::1",
			port: 0,
			trustTailscaleLogin: true,
			auditLog,
			request: async () => ({}),
			reconcileMs: 0,
		});
		try {
			const response = await fetch(`${server.url}/api/mutations`, {
				method: "POST",
				headers: { "content-type": "application/json", "Tailscale-User-Login": "owner@example.com" },
				body: JSON.stringify(body),
			});
			expect(response.status).toBe(200);
			expect((await auditLog.tail(1))[0]?.actor).toBe("owner@example.com");
		} finally {
			server.stop();
		}
	});
});

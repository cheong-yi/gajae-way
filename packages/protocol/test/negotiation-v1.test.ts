import { describe, expect, test } from "bun:test";
import { negotiate, PROFILE_VERSION, SUPPORTED_PROFILE_VERSIONS } from "../src/index";
import priorClientHello from "./fixtures/prior-client-hello-0.1.json";

/**
 * Firstmate current-profile cutover: retired and future clients reject typed.
 */
describe("profile 1.1 cutover", () => {
	test("retired clients cannot silently negotiate task semantics", () => {
		for (const supportedVersions of [priorClientHello.payload.supportedVersions, ["1.0"], ["0.1", "1.0"]]) {
			const result = negotiate({ supportedVersions });
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.code).toBe("incompatible_profile_version");
				expect(result.supportedVersions).toEqual(["1.1"]);
			}
		}
	});

	test("future-only client rejects typed with the supported range", () => {
		const result = negotiate({ supportedVersions: ["2.0"] });
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.code).toBe("incompatible_profile_version");
			expect(result.supportedVersions).toEqual(SUPPORTED_PROFILE_VERSIONS);
		}
	});

	test("profile aliases and extra version segments cannot opt into the current wire", () => {
		for (const version of ["1.1.0", "01.1", "1.01", "1.1 ", "v1.1", "1.1-task"]) {
			const result = negotiate({ supportedVersions: [version], requiredCapabilities: ["work.tasks"] });
			expect(result.ok).toBe(false);
			if (!result.ok) expect(result.code).toBe("incompatible_profile_version");
		}
	});

	test("mixed-range client negotiates the highest mutual version", () => {
		const result = negotiate({ supportedVersions: ["0.1", "1.0", PROFILE_VERSION], requiredCapabilities: ["work.tasks"] });
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.negotiated.profileVersion).toBe("1.1");
			expect(result.negotiated.capabilities).toContain("work.tasks");
		}
	});

	test("matching profile without task capability rejects required task semantics", () => {
		const result = negotiate({ supportedVersions: ["1.1"], requiredCapabilities: ["work.tasks"] }, ["1.1"], ["gateway.core"]);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.code).toBe("missing_required_capability");
	});
});

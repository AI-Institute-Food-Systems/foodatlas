import { afterEach, describe, expect, it, vi } from "vitest";

import { LIMITS, isCrossSite, isEmail, recipients, str } from "@/utils/formGuard";

// The two SES-backed POST routes were presence-validated only, with the length
// caps living exclusively as maxLength on the React inputs — so a direct POST
// could put megabytes of arbitrary text into the team inbox, and an unvalidated
// address reached ReplyToAddresses where SES rejects it as a 500.
//
// These pin the properties, not the current numbers: a cap that stops bounding,
// an email check that starts accepting whitespace, or a recipients() that stops
// failing closed would each break a test here.

describe("str", () => {
  it("rejects values over the cap", () => {
    expect(str("a".repeat(LIMITS.name), LIMITS.name)).not.toBeNull();
    expect(str("a".repeat(LIMITS.name + 1), LIMITS.name)).toBeNull();
  });

  it("rejects non-strings rather than coercing them", () => {
    for (const v of [42, {}, [], true]) {
      expect(str(v, 100), String(v)).toBeNull();
    }
  });

  it("treats whitespace-only as missing when required", () => {
    expect(str("   ", 100)).toBeNull();
    expect(str("   ", 100, { required: false })).toBe("");
  });

  it("trims, and measures the cap after trimming", () => {
    expect(str("  hi  ", 100)).toBe("hi");
    expect(str(`  ${"a".repeat(10)}  `, 10)).toBe("a".repeat(10));
  });

  it("allows an absent optional field but not an oversized one", () => {
    expect(str(undefined, 10, { required: false })).toBe("");
    expect(str("a".repeat(11), 10, { required: false })).toBeNull();
  });
});

describe("isEmail", () => {
  it("accepts ordinary addresses", () => {
    for (const v of ["a@b.co", "first.last+tag@sub.example.org"]) {
      expect(isEmail(v), v).toBe(true);
    }
  });

  it("rejects what would make SES throw", () => {
    for (const v of ["", "no-at-sign", "a@b", "a b@c.dk", "a@b c.dk", "@b.co"]) {
      expect(isEmail(v), JSON.stringify(v)).toBe(false);
    }
  });

  it("is bounded, so a huge string cannot reach SES", () => {
    expect(isEmail(`${"a".repeat(LIMITS.email)}@b.co`)).toBe(false);
  });
});

describe("recipients", () => {
  afterEach(() => vi.unstubAllEnvs());

  it("parses the fan-out list", () => {
    vi.stubEnv("CONTACT_EMAIL", '["a@b.co","c@d.co"]');
    expect(recipients()).toEqual(["a@b.co", "c@d.co"]);
  });

  it("fails closed instead of throwing", () => {
    // Previously `JSON.parse(process.env.CONTACT_EMAIL!)` — an unset or
    // malformed value threw and surfaced as a 500.
    for (const v of ["", "not json", "{}", "[]", '["not-an-email"]', '"a@b.co"']) {
      vi.stubEnv("CONTACT_EMAIL", v);
      expect(recipients(), JSON.stringify(v)).toBeNull();
    }
    vi.stubEnv("CONTACT_EMAIL", undefined as unknown as string);
    expect(recipients()).toBeNull();
  });
});

describe("isCrossSite", () => {
  const req = (h: Record<string, string>) =>
    new Request("https://www.foodatlas.ai/contact/send", { headers: h });

  it("flags only an explicit cross-site fetch", () => {
    expect(isCrossSite(req({ "sec-fetch-site": "cross-site" }))).toBe(true);
    expect(isCrossSite(req({ "sec-fetch-site": "same-origin" }))).toBe(false);
    expect(isCrossSite(req({ "sec-fetch-site": "none" }))).toBe(false);
  });

  it("allows a missing header", () => {
    // Non-browser clients omit it; this is abuse friction, not a CSRF boundary.
    expect(isCrossSite(req({}))).toBe(false);
  });
});

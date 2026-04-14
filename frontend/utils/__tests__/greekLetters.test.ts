import { describe, expect, it } from "vitest";

import { greekVariants, matchesWithGreek } from "@/utils/greekLetters";

describe("greekVariants", () => {
  it("returns empty array for null/undefined/empty input", () => {
    expect(greekVariants(null)).toEqual([]);
    expect(greekVariants(undefined)).toEqual([]);
    expect(greekVariants("")).toEqual([]);
  });

  it("returns escaped original for names without Greek letters", () => {
    expect(greekVariants("tocopherol")).toEqual(["tocopherol"]);
  });

  it("generates Greek variant for Latin name", () => {
    const variants = greekVariants("alpha-tocopherol");
    expect(variants).toContain("alpha-tocopherol");
    expect(variants).toContain("\u03b1-tocopherol");
  });

  it("generates Latin variant for Greek name", () => {
    const variants = greekVariants("\u03b1-tocopherol");
    expect(variants).toContain("\u03b1-tocopherol");
    expect(variants).toContain("alpha-tocopherol");
  });

  it("handles beta mapping", () => {
    const variants = greekVariants("beta-carotene");
    expect(variants).toContain("beta-carotene");
    expect(variants).toContain("\u03b2-carotene");
  });

  it("handles case-insensitive Latin replacement", () => {
    const variants = greekVariants("Alpha-tocopherol");
    expect(variants).toContain("Alpha-tocopherol");
    expect(variants).toContain("\u03b1-tocopherol");
  });

  it("escapes regex special characters", () => {
    const variants = greekVariants("alpha (test)");
    expect(variants).toContain("alpha \\(test\\)");
    expect(variants).toContain("\u03b1 \\(test\\)");
  });
});

describe("matchesWithGreek", () => {
  it("returns false for null/undefined inputs", () => {
    expect(matchesWithGreek(null, "alpha")).toBe(false);
    expect(matchesWithGreek("alpha", null)).toBe(false);
    expect(matchesWithGreek(undefined, undefined)).toBe(false);
  });

  it("matches exact same string", () => {
    expect(matchesWithGreek("tocopherol", "tocopherol")).toBe(true);
  });

  it("matches case-insensitively", () => {
    expect(matchesWithGreek("Alpha", "alpha")).toBe(true);
  });

  it("matches Greek character against Latin name", () => {
    expect(
      matchesWithGreek("\u03b1-tocopherol", "alpha-tocopherol")
    ).toBe(true);
  });

  it("matches Latin name against Greek character", () => {
    expect(
      matchesWithGreek("alpha-tocopherol", "\u03b1-tocopherol")
    ).toBe(true);
  });

  it("matches beta variants", () => {
    expect(matchesWithGreek("\u03b2-carotene", "beta-carotene")).toBe(true);
    expect(matchesWithGreek("beta-carotene", "\u03b2-carotene")).toBe(true);
  });

  it("returns false for non-matching strings", () => {
    expect(matchesWithGreek("gamma-linolenic", "alpha-tocopherol")).toBe(
      false
    );
  });
});

import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { CANONICAL_PUBLICATION } from "@/utils/publications";

// CITATION.cff (repo root) is YAML, so it cannot import the single source of
// truth. This keeps its preferred citation in step with it instead.
const cff = readFileSync(join(process.cwd(), "..", "CITATION.cff"), "utf8");

describe("CITATION.cff", () => {
  const p = CANONICAL_PUBLICATION;

  it("prefers the canonical publication", () => {
    expect(cff).toContain(`doi: "${p.doi}"`);
    expect(cff).toContain(`title: "${p.title}"`);
    expect(cff).toContain(`journal: "${p.venue}"`);
    expect(cff).toContain(`year: ${p.year}`);
    expect(cff).toContain(`volume: ${p.volume}`);
    expect(cff).toContain(`issue: ${p.issue}`);
    expect(cff).toContain(`start: ${p.articleNumber}`);
  });

  it("lists the same authors in the same order", () => {
    const family = Array.from(cff.matchAll(/family-names: (.+)/g), (m) => m[1]);
    const fromApa = p.authors
      .replace("& ", "")
      .split(/,\s*(?=[A-Z][a-z])/)
      .map((a) => a.split(",")[0].trim());
    expect(family).toEqual(fromApa);
  });

  it("licenses code as MIT and data as CC BY-NC 4.0", () => {
    expect(cff).toMatch(/^license:\n  - MIT\n  - CC-BY-NC-4\.0$/m);
  });
});

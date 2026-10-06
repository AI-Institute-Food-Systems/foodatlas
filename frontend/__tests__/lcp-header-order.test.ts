import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { ENTITY_TYPES } from "@/utils/site";

const read = (p: string) => readFileSync(join(process.cwd(), p), "utf8");

// The entity h1 is the mobile LCP element (H1). When HeaderSection fetched
// for itself, React serialised it after the whole page, tab snapshots and
// all: the h1 sat at byte ~640k of a ~650k document. It now renders in
// order, from the metadata the page already awaited.
describe("entity header renders in document order", () => {
  it("HeaderSection is synchronous", () => {
    const src = read("components/entities/HeaderSection.tsx");
    expect(src).not.toMatch(/const HeaderSection = async/);
    expect(src).not.toContain("getMetaData(");
  });

  it("every entity page hands it the metadata, outside a Suspense", () => {
    for (const type of ENTITY_TYPES) {
      const src = read(`app/(everything-else)/${type}/[slug]/page.tsx`);
      expect(src, type).toContain("metadata={metaPayload}");
      expect(src, type).not.toContain("HeaderSectionSuspense");
    }
  });
});

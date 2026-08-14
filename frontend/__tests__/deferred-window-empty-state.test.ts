// An empty state must never fill the deferred-skeleton window.
//
// useDeferredLoading holds the skeleton back for 200ms, so for that window
// a table is loading with no rows and no placeholder. Any "no results"
// branch gated only on `rows.length === 0` renders into that gap and
// asserts something false — a worse flash than the one the delay removes.
// It shipped in two files before being caught by eye.
//
// Structural rather than behavioural: rendering these sections needs
// promises that never settle, which deadlocks a jsdom run.

import { readFileSync } from "fs";
import { join } from "path";
import { describe, expect, it } from "vitest";

const ROOT = join(__dirname, "..");
const read = (p: string) => readFileSync(join(ROOT, p), "utf8");

// Every component that both defers its skeleton and owns an empty state.
const FILES = [
  "components/entities/CorrelationTable.tsx",
  "components/entities/food/FoodCompositionSection.tsx",
  "components/entities/bioactivity/BioactivityTable.tsx",
  "components/entities/bioactivity/FoodInferredBioactivitiesSection.tsx",
  "components/entities/AssayInferredAssociationsTable.tsx",
  "components/entities/bioactivity/BioactivityDiseasesSection.tsx",
  "components/entities/disease/DiseaseBioactivitiesSection.tsx",
];

// The two shapes that correctly close the window: an inline
// `isLoading ? null :` guard on the empty branch, or a derived flag /
// early return that already requires !isLoading.
const GUARD = /isLoading \?\s*(\/\/[^\n]*\n\s*)*null\s*:|!isLoading &&|!isLoading \?/;

describe("deferred-window empty states", () => {
  for (const file of FILES) {
    it(`${file.split("/").pop()} gates its empty state on isLoading`, () => {
      const src = read(file);
      expect(
        GUARD.test(src),
        `${file}: has a deferred skeleton but no !isLoading guard on its empty state — ` +
          `the 200ms deferred window will render "no results" instead of nothing`
      ).toBe(true);
    });
  }

  it("every deferring component is listed here", () => {
    // Keeps the list honest: a new table that adopts useDeferredLoading
    // has to be added above, at which point it gets the guard check.
    const consumers = FILES.filter((f) =>
      read(f).includes("useDeferredLoading")
    );
    expect(consumers).toEqual(FILES);
  });
});

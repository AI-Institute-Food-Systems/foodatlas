// The two list rules every facet follows — see facetOptions.ts.

import { describe, expect, it } from "vitest";

import {
  facetOptions,
  facetUniverse,
  overlayFacetCounts,
  sortFacetOptions,
  sumFacetCounts,
} from "@/components/entities/shared/filters/facetOptions";

describe("sortFacetOptions", () => {
  it("sorts alphabetically, ignoring case", () => {
    const out = sortFacetOptions(["uM", "IU", "mg/kg", "ug/mL", "%"], (s) => s);
    expect(out).toEqual(["%", "IU", "mg/kg", "ug/mL", "uM"]);
  });

  it("does not sort by count", () => {
    // Busiest-first was the default everywhere, and it is exactly what
    // made the assays modal's Evidence list move under the cursor.
    const out = sortFacetOptions(
      [
        { value: "in vitro", count: 300 },
        { value: "adme/tox", count: 1 },
        { value: "molecular-level", count: 50 },
      ],
      (o) => o.value
    );
    expect(out.map((o) => o.value)).toEqual([
      "adme/tox",
      "in vitro",
      "molecular-level",
    ]);
  });

  it("pins reset options first and catch-alls last, in the order given", () => {
    const out = sortFacetOptions(
      ["unclassified", "vitamin", "All", "alkaloid", "Any"],
      (s) => s,
      { pinFirst: ["All", "Any"], pinLast: ["unclassified"] }
    );
    expect(out).toEqual(["All", "Any", "alkaloid", "vitamin", "unclassified"]);
  });

  it("is stable under count changes", () => {
    // The whole point: the same values in the same order, whatever the
    // counts happen to be right now.
    const before = sortFacetOptions(
      [
        { value: "b", count: 9 },
        { value: "a", count: 1 },
      ],
      (o) => o.value
    ).map((o) => o.value);
    const after = sortFacetOptions(
      [
        { value: "b", count: 0 },
        { value: "a", count: 7 },
      ],
      (o) => o.value
    ).map((o) => o.value);
    expect(after).toEqual(before);
  });

  it("does not mutate its input", () => {
    const input = ["b", "a"];
    sortFacetOptions(input, (s) => s);
    expect(input).toEqual(["b", "a"]);
  });
});

describe("overlayFacetCounts", () => {
  it("returns every universe value, zero when the counts omit it", () => {
    // A GROUP BY never returns an empty group. The universe is what
    // keeps the option on screen; the zero is what greys it out.
    const out = overlayFacetCounts(["uM", "nM", "ug/mL"], new Map([["uM", 4]]));
    expect(out).toEqual([
      { value: "uM", count: 4 },
      { value: "nM", count: 0 },
      { value: "ug/mL", count: 0 },
    ]);
  });

  it("accepts a plain record as well as a Map", () => {
    expect(overlayFacetCounts(["a", "b"], { b: 2 })).toEqual([
      { value: "a", count: 0 },
      { value: "b", count: 2 },
    ]);
  });

  it("drops counts for values outside the universe", () => {
    // A stale response from the previous entity must not add options.
    expect(overlayFacetCounts(["a"], { a: 1, stale: 5 })).toEqual([
      { value: "a", count: 1 },
    ]);
  });

  it("dedupes the universe", () => {
    expect(overlayFacetCounts(["a", "a"], { a: 1 })).toHaveLength(1);
  });
});

describe("facetOptions", () => {
  it("is overlay then sort", () => {
    expect(facetOptions(["nM", "uM"], { uM: 2 })).toEqual([
      { value: "nM", count: 0 },
      { value: "uM", count: 2 },
    ]);
  });
});

describe("sumFacetCounts", () => {
  it("sums the same value across lists", () => {
    // The food page's sidebar spans a direct and an inferred table.
    const out = sumFacetCounts(
      [
        { value: "uM", count: 2 },
        { value: "nM", count: 1 },
      ],
      [{ value: "uM", count: 3 }]
    );
    expect(out).toEqual([
      { value: "uM", count: 5 },
      { value: "nM", count: 1 },
    ]);
  });

  it("drops blank values", () => {
    expect(sumFacetCounts([{ value: " ", count: 9 }])).toEqual([]);
  });

  it("collapses the same value listed under several endpoints", () => {
    // /bioactivity/endpoints is per (endpoint, unit); the facet is per unit.
    const out = sumFacetCounts([
      { value: "uM", count: 2 },
      { value: "uM", count: 3 },
    ]);
    expect(out).toEqual([{ value: "uM", count: 5 }]);
  });
});

describe("facetUniverse", () => {
  it("collects the distinct non-blank values", () => {
    const rows = [
      { et: "in vitro" },
      { et: "in vitro" },
      { et: " " },
      { et: null },
      { et: "adme/tox" },
    ];
    expect(facetUniverse(rows, (r) => r.et)).toEqual(["in vitro", "adme/tox"]);
  });
});

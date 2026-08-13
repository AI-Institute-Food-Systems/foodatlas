import { describe, expect, it } from "vitest";

import {
  ChemicalCompositionRow,
  MIN_BAR_PERCENT,
  barPercent,
  computeMaxValue,
  filterRows,
  formatPercentByMass,
  mergeBuckets,
  paginate,
  rowSourceLabels,
  sortRows,
} from "@/utils/chemicalComposition";

const row = (
  name: string,
  value: number | null,
  extra: Partial<ChemicalCompositionRow> = {}
): ChemicalCompositionRow => ({
  id: `id-${name}`,
  name,
  median_concentration:
    value === null ? null : { value, unit: "mg/100g" },
  ...extra,
});

describe("barPercent", () => {
  it("scales linearly against the max", () => {
    expect(barPercent(50, 100)).toBe(50);
    expect(barPercent(100, 100)).toBe(100);
  });

  it("floors trace values so they stay visible", () => {
    // 0.001 of the max would round to a zero-width bar and read as
    // "no data" rather than "a little".
    expect(barPercent(1, 100000)).toBe(MIN_BAR_PERCENT);
  });

  it("never exceeds 100 even if a value is above the max", () => {
    expect(barPercent(500, 100)).toBe(100);
  });

  it("returns null for unmeasured rows and degenerate maxima", () => {
    expect(barPercent(null, 100)).toBeNull();
    expect(barPercent(5, 0)).toBeNull();
    expect(barPercent(0, 100)).toBeNull();
  });
});

describe("computeMaxValue", () => {
  it("ignores rows without a measured concentration", () => {
    expect(
      computeMaxValue([row("a", 10), row("b", null), row("c", 4)])
    ).toBe(10);
  });

  it("is 0 for an empty set, which barPercent treats as no bar", () => {
    expect(computeMaxValue([])).toBe(0);
    expect(barPercent(5, computeMaxValue([]))).toBeNull();
  });
});

describe("sortRows", () => {
  const rows = [row("cherry", 5), row("apple", 100), row("kale", null)];

  it("defaults to ranking by concentration descending", () => {
    expect(sortRows(rows, "median_concentration", "desc").map((r) => r.name))
      .toEqual(["apple", "cherry", "kale"]);
  });

  it("keeps unmeasured rows last when reversed", () => {
    // Flipping direction reverses the ranking of rows that HAVE a value;
    // letting blanks float to the top would bury the actual data.
    expect(sortRows(rows, "median_concentration", "asc").map((r) => r.name))
      .toEqual(["cherry", "apple", "kale"]);
  });

  it("sorts by name and by evidence count", () => {
    expect(sortRows(rows, "name", "asc").map((r) => r.name)).toEqual([
      "apple",
      "cherry",
      "kale",
    ]);
    const byEvidence = sortRows(
      [
        row("a", 1, { evidence_count: 2 }),
        row("b", 1, { evidence_count: 9 }),
      ],
      "evidence_count",
      "desc"
    );
    expect(byEvidence.map((r) => r.name)).toEqual(["b", "a"]);
  });

  it("does not mutate its input", () => {
    const input = [row("b", 1), row("a", 2)];
    sortRows(input, "name", "asc");
    expect(input.map((r) => r.name)).toEqual(["b", "a"]);
  });
});

describe("mergeBuckets", () => {
  it("appends unmeasured rows normalised to a null concentration", () => {
    const merged = mergeBuckets(
      [row("apple", 10)],
      [row("kale", 999)],
      true
    );
    expect(merged).toHaveLength(2);
    expect(merged[1]!.median_concentration).toBeNull();
  });

  it("omits the unmeasured bucket when the toggle is off", () => {
    expect(mergeBuckets([row("apple", 10)], [row("kale", null)], false))
      .toHaveLength(1);
  });

  it("tolerates null buckets", () => {
    expect(mergeBuckets(null, undefined, true)).toEqual([]);
  });
});

describe("filterRows", () => {
  const rows = [
    row("apple", 1, { fdc_count: 2 }),
    row("green apple", 2, { ptfi_count: 1 }),
    row("kale", 3, { foodatlas_count: 4 }),
  ];

  it("matches food names case-insensitively on substrings", () => {
    expect(
      filterRows(rows, { search: "APPLE", sources: [] }).map((r) => r.name)
    ).toEqual(["apple", "green apple"]);
  });

  it("treats an empty source selection as no filter", () => {
    // Clearing the last checkbox must not blank the table.
    expect(filterRows(rows, { search: "", sources: [] })).toHaveLength(3);
  });

  it("keeps rows contributing to any selected source", () => {
    expect(
      filterRows(rows, { search: "", sources: ["fdc", "ptfi"] }).map(
        (r) => r.name
      )
    ).toEqual(["apple", "green apple"]);
  });
});

describe("rowSourceLabels", () => {
  it("lists only sources with a data point, in display order", () => {
    expect(
      rowSourceLabels(row("a", 1, { ptfi_count: 1, fdc_count: 3 }))
    ).toEqual(["FDC", "PTFI"]);
  });

  it("is empty when the API omits the counts entirely", () => {
    // An older API build must degrade to a blank cell, not a crash.
    expect(rowSourceLabels(row("a", 1))).toEqual([]);
  });
});

describe("formatPercentByMass", () => {
  it("scales decimals with magnitude", () => {
    expect(formatPercentByMass(row("a", 200000))).toBe("200%");
    expect(formatPercentByMass(row("a", 2000))).toBe("2.0%");
    expect(formatPercentByMass(row("a", 50))).toBe("0.05%");
  });

  it("returns null when the unit is not mg/100g", () => {
    expect(
      formatPercentByMass({
        id: "x",
        name: "x",
        median_concentration: { value: 5, unit: "relative_abundance" },
      })
    ).toBeNull();
  });
});

describe("paginate", () => {
  it("slices by page", () => {
    const rows = [1, 2, 3, 4, 5];
    expect(paginate(rows, 1, 2)).toEqual([1, 2]);
    expect(paginate(rows, 3, 2)).toEqual([5]);
  });
});

import { describe, expect, it } from "vitest";

import {
  displayUnit,
  formatTopMeasurement,
} from "@/components/entities/bioactivity/format";
import {
  sortInferred,
  type InferredRow,
} from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import type { FoodEfficacyRow } from "@/types";

// The API emits the literal string "None" as a sentinel for a null/blank
// unit (backend _bioact_hotfix.normalize_unit). It must never reach the UI
// as text. This regressed once — the guard was dropped on the belief that
// the upstream cleanup had shipped, and a later merge re-introduced the
// backend hotfix that produces the sentinel — so it is pinned here.
describe("displayUnit", () => {
  it('renders nothing for the "None" sentinel', () => {
    expect(displayUnit("None")).toBe("");
  });

  it("renders nothing for null / undefined / empty", () => {
    expect(displayUnit(null)).toBe("");
    expect(displayUnit(undefined)).toBe("");
    expect(displayUnit("")).toBe("");
  });

  it("renders a real unit with a leading space", () => {
    expect(displayUnit("uM")).toBe(" uM");
    expect(displayUnit("mg/100g")).toBe(" mg/100g");
  });

  it("does not swallow units that merely contain 'None'", () => {
    expect(displayUnit("Nonel")).toBe(" Nonel");
  });
});

describe("formatTopMeasurement", () => {
  it('omits the "None" sentinel from the composed label', () => {
    expect(
      formatTopMeasurement({ endpoint: "Glycemic Load", value: 15.056, unit: "None" })
    ).toBe("Glycemic Load: 15.1");
  });

  it("keeps a real unit", () => {
    expect(
      formatTopMeasurement({ endpoint: "IC50", value: 1.8, unit: "uM" })
    ).toBe("IC50: 1.8 uM");
  });
});

// Minimal row factory — only the fields sortInferred reads.
const row = (
  chemical: string,
  opts: {
    fraction?: number | null;
    logRatio?: number | null;
    concentration?: number | null;
  } = {}
): InferredRow => {
  const { fraction = null, logRatio = null, concentration = null } = opts;
  return {
    bioactivity: "anticancer",
    bioactivity_id: "e1",
    chemical,
    chemical_id: `c-${chemical}`,
    median_concentration:
      concentration === null ? null : { value: concentration, unit: "mg/100g" },
    n_curves: 0,
    n_measurements_total: 0,
    efficacy: {
      efficacy_fraction: fraction,
      dose_over_ac50_log: logRatio,
    } as unknown as FoodEfficacyRow,
  };
};

const names = (rows: InferredRow[]) => rows.map((r) => r.chemical);

describe("sortInferred — efficacy", () => {
  // efficacy_fraction saturates: about half a typical food's rows sit above
  // 0.99 and all render as ">99%". Ordering them by fraction alone is
  // effectively arbitrary, so ties fall through to dose_over_ac50_log.
  it("breaks ties on dose_over_ac50_log when fractions are equal", () => {
    const rows = [
      row("weak", { fraction: 0.999, logRatio: 0.62 }),
      row("strong", { fraction: 0.999, logRatio: 6.65 }),
      row("mid", { fraction: 0.999, logRatio: 3.1 }),
    ];
    expect(names(sortInferred(rows, "efficacy", "desc"))).toEqual([
      "strong",
      "mid",
      "weak",
    ]);
    expect(names(sortInferred(rows, "efficacy", "asc"))).toEqual([
      "weak",
      "mid",
      "strong",
    ]);
  });

  it("still orders primarily by fraction", () => {
    const rows = [
      row("low", { fraction: 0.2, logRatio: 9 }),
      row("high", { fraction: 0.9, logRatio: 1 }),
    ];
    expect(names(sortInferred(rows, "efficacy", "desc"))).toEqual([
      "high",
      "low",
    ]);
  });

  it("keeps null efficacy last in BOTH directions", () => {
    const rows = [
      row("null-row", { fraction: null }),
      row("a", { fraction: 0.5, logRatio: 1 }),
      row("b", { fraction: 0.8, logRatio: 2 }),
    ];
    expect(names(sortInferred(rows, "efficacy", "desc")).at(-1)).toBe(
      "null-row"
    );
    expect(names(sortInferred(rows, "efficacy", "asc")).at(-1)).toBe(
      "null-row"
    );
  });
});

describe("sortInferred — concentration", () => {
  // Default sort is concentration/desc, so a nulls-first bug here would put
  // an em-dash row at the very top of page 1.
  it("keeps null concentration last in BOTH directions", () => {
    const rows = [
      row("null-row", { concentration: null }),
      row("small", { concentration: 1 }),
      row("big", { concentration: 100 }),
    ];
    expect(names(sortInferred(rows, "concentration", "desc"))).toEqual([
      "big",
      "small",
      "null-row",
    ]);
    expect(names(sortInferred(rows, "concentration", "asc"))).toEqual([
      "small",
      "big",
      "null-row",
    ]);
  });
});

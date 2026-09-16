import { describe, expect, it } from "vitest";

import {
  displayUnit,
  formatTopMeasurement,
  logAC50InUnit,
} from "@/components/entities/bioactivity/format";

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

// PubChem's logAC50 is log10 molar; the row's value is in the assay's
// unit. The Hill-fit panel printed 10^logAC50 next to the row's unit —
// "AC50 0.0000141 uM" for a 14.1 uM row, a million times too potent.
describe("logAC50InUnit", () => {
  it("shifts a molar log AC50 into the row's micromolar unit", () => {
    // AID 588834, L-lysine × cardioprotective: value 14.1254 uM,
    // efficacy_logac50_value −4.85.
    const { logAC50, unit } = logAC50InUnit(-4.85, "uM");
    expect(unit).toBe("uM");
    expect(10 ** logAC50).toBeCloseTo(14.125, 2);
  });

  it.each([
    ["nM", 9],
    ["mM", 3],
    ["M", 0],
    ["pM", 12],
    ["MICROMOLAR", 6],
    ["µM", 6],
  ])("knows %s", (unit, shift) => {
    expect(logAC50InUnit(-6, unit).logAC50).toBeCloseTo(-6 + shift, 9);
    expect(logAC50InUnit(-6, unit).unit).toBe(unit);
  });

  it("falls back to molar, and says so, for units it cannot convert", () => {
    for (const unit of ["ug/mL", undefined, null, ""]) {
      expect(logAC50InUnit(-6, unit)).toEqual({ logAC50: -6, unit: "M" });
    }
  });
});

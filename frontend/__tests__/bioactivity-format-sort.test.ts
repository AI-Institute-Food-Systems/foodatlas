import { describe, expect, it } from "vitest";

import {
  displayUnit,
  formatTopMeasurement,
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

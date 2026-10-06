import { afterEach, describe, expect, it, vi } from "vitest";

const composition = vi.fn();
const bioactivities = vi.fn();
const literature = vi.fn();
const inferred = vi.fn();
vi.mock("@/utils/fetching", () => ({
  getChemicalCompositionData: (...a: unknown[]) => composition(...a),
  getChemicalBioactivities: (...a: unknown[]) => bioactivities(...a),
  getDiseaseData: (...a: unknown[]) => literature(...a),
  getChemicalDiseaseAssociations: (...a: unknown[]) => inferred(...a),
  getDiseaseChemicalAssociations: vi.fn(),
  getBioactivityDiseases: vi.fn(),
}));

import { chemicalHasNoRelations } from "@/utils/tabCounts";

const empty = () => {
  composition.mockResolvedValue({ with_concentrations: [], without_concentrations: [] });
  bioactivities.mockResolvedValue({ metadata: { total_rows: 0 } });
  literature.mockResolvedValue({ metadata: { total_rows: 0 } });
  inferred.mockResolvedValue({ metadata: { row_count: 0 } });
};

afterEach(() => vi.resetAllMocks());

// A chemical page with no foods, bioactivities or diseases gets noindex.
describe("chemicalHasNoRelations", () => {
  it("is true when every tab is empty", async () => {
    empty();
    expect(await chemicalHasNoRelations("imidazopyridine")).toBe(true);
  });

  it("is false when any tab has rows", async () => {
    empty();
    inferred.mockResolvedValue({ metadata: { row_count: 3 } });
    expect(await chemicalHasNoRelations("methotrexate")).toBe(false);
  });

  it("is false when a count fails, so the page stays indexable", async () => {
    empty();
    composition.mockRejectedValue(new Error("api down"));
    expect(await chemicalHasNoRelations("quercetin")).toBe(false);
  });
});

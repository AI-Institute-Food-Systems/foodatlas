import { afterEach, describe, expect, it, vi } from "vitest";

const lookupMetaData = vi.fn();
const getChemicalBioactivities = vi.fn();
const getDiseaseChemicalAssociations = vi.fn();
vi.mock("@/utils/fetching", () => ({
  lookupMetaData: (...a: unknown[]) => lookupMetaData(...a),
  getChemicalBioactivities: (...a: unknown[]) => getChemicalBioactivities(...a),
  getDiseaseChemicalAssociations: (...a: unknown[]) =>
    getDiseaseChemicalAssociations(...a),
}));
vi.mock("next/navigation", () => ({
  notFound: () => {
    throw new Error("NEXT_NOT_FOUND");
  },
}));

import { requireEntity } from "@/components/entities/requireEntity";

const meta = { id: "e1", common_name: "apple" };

afterEach(() => vi.clearAllMocks());

describe("requireEntity", () => {
  it("passes a known entity, looked up by its decoded name", async () => {
    lookupMetaData.mockResolvedValue(meta);
    await expect(requireEntity("food", "cow--milk")).resolves.toBeUndefined();
    expect(lookupMetaData).toHaveBeenCalledWith("cow milk", "food");
  });

  it("404s a slug the API says does not exist", async () => {
    lookupMetaData.mockResolvedValue("missing");
    getDiseaseChemicalAssociations.mockResolvedValue({
      metadata: { row_count: 0 },
    });
    await expect(requireEntity("disease", "nope")).rejects.toThrow(
      "NEXT_NOT_FOUND"
    );
  });

  describe("diseases without metadata", () => {
    // Bioactivity pages link every disease their assays reach; those
    // without a CTD correlation have no metadata row but a real page.
    it("keep their page when assays link them to chemicals", async () => {
      lookupMetaData.mockResolvedValue("missing");
      getDiseaseChemicalAssociations.mockResolvedValue({
        metadata: { row_count: 26 },
      });
      await expect(
        requireEntity("disease", "favism")
      ).resolves.toBeUndefined();
      expect(getDiseaseChemicalAssociations).toHaveBeenCalledWith("favism");
    });

    it("keep their page when the assay check errors", async () => {
      lookupMetaData.mockResolvedValue("missing");
      getDiseaseChemicalAssociations.mockResolvedValue(null);
      await expect(
        requireEntity("disease", "favism")
      ).resolves.toBeUndefined();
    });
  });

  it("fails open when the API errors", async () => {
    // A 404 on a blip would deindex a real page.
    lookupMetaData.mockResolvedValue("error");
    await expect(requireEntity("food", "apple")).resolves.toBeUndefined();
  });

  it("404s malformed percent-encoding instead of throwing a 500", async () => {
    await expect(requireEntity("food", "%E0")).rejects.toThrow(
      "NEXT_NOT_FOUND"
    );
    expect(lookupMetaData).not.toHaveBeenCalled();
  });

  describe("chemicals without metadata", () => {
    it("keep their page when bioassays know them", async () => {
      lookupMetaData.mockResolvedValue("missing");
      getChemicalBioactivities.mockResolvedValue({
        metadata: { total_rows: 11 },
      });
      await expect(
        requireEntity("chemical", "benzoin")
      ).resolves.toBeUndefined();
    });

    it("keep their page when the bioactivity check errors", async () => {
      lookupMetaData.mockResolvedValue("missing");
      getChemicalBioactivities.mockResolvedValue(null);
      await expect(
        requireEntity("chemical", "benzoin")
      ).resolves.toBeUndefined();
    });

    it("404 when nothing knows them", async () => {
      lookupMetaData.mockResolvedValue("missing");
      getChemicalBioactivities.mockResolvedValue({
        metadata: { total_rows: 0 },
      });
      await expect(requireEntity("chemical", "nope")).rejects.toThrow(
        "NEXT_NOT_FOUND"
      );
    });
  });

  it("never consults bioactivities for other types", async () => {
    lookupMetaData.mockResolvedValue("missing");
    await expect(requireEntity("food", "nope")).rejects.toThrow();
    expect(getChemicalBioactivities).not.toHaveBeenCalled();
    expect(getDiseaseChemicalAssociations).not.toHaveBeenCalled();
  });
});

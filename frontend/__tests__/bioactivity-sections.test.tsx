import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BioactivityChemicalRow,
  BioactivityFoodRow,
} from "@/types";

vi.mock("@/utils/fetching", () => ({
  getBioactivityChemicals: vi.fn(),
  getBioactivityFoods: vi.fn(),
  getChemicalBioactivities: vi.fn(),
  getFoodBioactivities: vi.fn(),
}));

import {
  getBioactivityChemicals,
  getBioactivityFoods,
  getChemicalBioactivities,
  getFoodBioactivities,
} from "@/utils/fetching";
import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";

async function renderAsync(node: Promise<JSX.Element>) {
  render(await node);
}

const chemicalRow: BioactivityChemicalRow = {
  id: "c1",
  name: "quercetin",
  measurement_count: 755,
  active_count: 83,
  inactive_count: 261,
  potency_summary: [
    { endpoint: "IC50", unit: "MICROMOLAR", median: 17.175, n: 130 },
    { endpoint: "GI50", unit: "nM", median: 59292.53, n: 61 },
  ],
  measurements: [
    {
      endpoint: "AC50",
      outcome: "Active",
      value: 0.035,
      unit: "MICROMOLAR",
      assay: "AID: 364",
    },
  ],
};

const foodRow: BioactivityFoodRow = {
  id: "f1",
  name: "snail",
  measurement_count: 1,
  measurements: [
    {
      endpoint: "Activity",
      outcome: "Unspecified",
      value: 0.519,
      unit: "mmol/100g",
      assay: "FoodAtlasModel: RF_antioxidant_v1",
    },
  ],
};

describe("BioactivityChemicalsSection", () => {
  it("renders empty state when no chemicals", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [],
      metadata: { row_count: 0 },
    });
    await renderAsync(
      BioactivityChemicalsSection({ commonName: "antioxidant" })
    );
    expect(
      screen.getByText(/no chemical-bioactivity measurements/i)
    ).toBeInTheDocument();
  });

  it("renders rows with active/inactive counts and top potency", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [chemicalRow],
      metadata: { row_count: 1 },
    });
    await renderAsync(
      BioactivityChemicalsSection({ commonName: "antioxidant" })
    );
    expect(screen.getByText(/quercetin/i)).toBeInTheDocument();
    expect(screen.getByText("755")).toBeInTheDocument();
    expect(screen.getByText("83")).toBeInTheDocument();
    expect(screen.getByText("261")).toBeInTheDocument();
    expect(screen.getByText(/IC50: 17\.2 MICROMOLAR \(n=130\)/)).toBeInTheDocument();
  });
});

describe("BioactivityFoodsSection", () => {
  it("renders first measurement value+unit", async () => {
    vi.mocked(getBioactivityFoods).mockResolvedValueOnce({
      data: [foodRow],
      metadata: { row_count: 1 },
    });
    await renderAsync(BioactivityFoodsSection({ commonName: "antioxidant" }));
    expect(screen.getByText(/snail/i)).toBeInTheDocument();
    expect(screen.getByText(/0\.519 mmol\/100g/)).toBeInTheDocument();
  });
});

describe("ChemicalBioactivitiesSection", () => {
  it("renders linked bioactivity rows", async () => {
    vi.mocked(getChemicalBioactivities).mockResolvedValueOnce({
      data: [{ ...chemicalRow, name: "antioxidant", id: "bio1" }],
      metadata: { row_count: 1 },
    });
    await renderAsync(
      ChemicalBioactivitiesSection({ commonName: "quercetin" })
    );
    const link = screen.getByRole("link", { name: /antioxidant/i });
    expect(link).toHaveAttribute("href", "/bioactivity/antioxidant");
  });
});

describe("FoodBioactivitiesSection", () => {
  it("links bioactivities and shows first value", async () => {
    vi.mocked(getFoodBioactivities).mockResolvedValueOnce({
      data: [{ ...foodRow, name: "antioxidant", id: "bio1" }],
      metadata: { row_count: 1 },
    });
    await renderAsync(FoodBioactivitiesSection({ commonName: "snail" }));
    expect(
      screen.getByRole("link", { name: /antioxidant/i })
    ).toBeInTheDocument();
    expect(screen.getByText(/0\.519 mmol\/100g/)).toBeInTheDocument();
  });
});

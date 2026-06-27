import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ReactElement } from "react";

import type {
  BioactivityChemicalRow,
  BioactivityFoodRow,
} from "@/types";

vi.mock("@/utils/fetching", () => ({
  getBioactivityChemicals: vi.fn(),
  getBioactivityFoods: vi.fn(),
  getChemicalBioactivities: vi.fn(),
  getFoodBioactivities: vi.fn(),
  // BioactivityTable calls this on mount to populate the endpoint·unit
  // filter chips; stub it so the mock graph is complete.
  getBioactivityEndpointOptions: vi.fn().mockResolvedValue([]),
}));

// next/navigation isn't mounted in the vitest jsdom env; Button uses
// useRouter for its built-in <Link> shortcut, so stub it.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
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
import { PaginationsProvider } from "@/context/paginationsContext";

const renderWithPagination = (node: ReactElement) =>
  render(<PaginationsProvider>{node}</PaginationsProvider>);

const chemicalRow: BioactivityChemicalRow = {
  id: "c1",
  name: "quercetin",
  measurement_count: 755,
  active_count: 83,
  inactive_count: 261,
  top_measurement: { endpoint: "IC50", value: 17.175, unit: "MICROMOLAR" },
  measurements: [
    {
      endpoint: "IC50",
      outcome: "Active",
      value: 17.175,
      unit: "MICROMOLAR",
      assay: "AID: 364",
    },
  ],
};

const foodRow: BioactivityFoodRow = {
  id: "f1",
  name: "snail",
  measurement_count: 1,
  top_measurement: { endpoint: "Activity", value: 0.519, unit: "mmol/100g" },
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
    renderWithPagination(<BioactivityChemicalsSection commonName="antioxidant" />);
    expect(
      await screen.findByText(/no chemical-bioactivity measurements/i)
    ).toBeInTheDocument();
  });

  it("renders rows with active/inactive counts and top measurement", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [chemicalRow],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<BioactivityChemicalsSection commonName="antioxidant" />);
    expect(await screen.findByText(/quercetin/i)).toBeInTheDocument();
    // Total count moved into the "View N assays" button; standalone count
    // column was removed in the apothecary redesign.
    expect(screen.getByText(/View 755 assays/)).toBeInTheDocument();
    expect(screen.getByText("83")).toBeInTheDocument();
    expect(screen.getByText("261")).toBeInTheDocument();
    expect(screen.getByText(/IC50: 17\.2 MICROMOLAR/)).toBeInTheDocument();
  });
});

describe("BioactivityFoodsSection", () => {
  it("renders top measurement value + View assays button", async () => {
    vi.mocked(getBioactivityFoods).mockResolvedValueOnce({
      data: [foodRow],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<BioactivityFoodsSection commonName="antioxidant" />);
    expect(await screen.findByText(/snail/i)).toBeInTheDocument();
    expect(screen.getByText(/Activity: 0\.519 mmol\/100g/)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /view 1 assay/i })
    ).toBeInTheDocument();
  });
});

describe("ChemicalBioactivitiesSection", () => {
  it("renders linked bioactivity rows", async () => {
    vi.mocked(getChemicalBioactivities).mockResolvedValueOnce({
      data: [{ ...chemicalRow, name: "antioxidant", id: "bio1" }],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<ChemicalBioactivitiesSection commonName="quercetin" />);
    const link = await screen.findByRole("link", { name: /antioxidant/i });
    expect(link).toHaveAttribute("href", "/bioactivity/antioxidant");
  });
});

describe("FoodBioactivitiesSection", () => {
  it("links bioactivities and shows top measurement", async () => {
    vi.mocked(getFoodBioactivities).mockResolvedValueOnce({
      data: [{ ...foodRow, name: "antioxidant", id: "bio1" }],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<FoodBioactivitiesSection commonName="snail" />);
    expect(
      await screen.findByRole("link", { name: /antioxidant/i })
    ).toBeInTheDocument();
    expect(screen.getByText(/Activity: 0\.519 mmol\/100g/)).toBeInTheDocument();
  });
});

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
  // BioactivityTable calls these on mount to populate the sidebar's
  // unit chips + Category filter; stub both so the mock graph is complete.
  getBioactivityEndpointOptions: vi.fn().mockResolvedValue([]),
  getBioactivityCategoryOptions: vi.fn().mockResolvedValue([]),
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

// Every bioactivity section renders BOTH a desktop <table> and a mobile
// card list; the two are toggled by `hidden md:block` / `md:hidden`
// classes. JSDOM ignores those display rules, so any query that hits a
// row value matches twice. Use *AllBy* variants + `.length` /
// `[0]` assertions instead of the single-match helpers.

describe("BioactivityChemicalsSection", () => {
  it("renders empty state when no chemicals", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [],
      metadata: { row_count: 0 },
    });
    renderWithPagination(<BioactivityChemicalsSection commonName="antioxidant" />);
    const empties = await screen.findAllByText(
      /no chemical-bioactivity measurements/i,
    );
    expect(empties.length).toBeGreaterThan(0);
  });

  it("renders rows with active/inactive counts and top measurement", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [chemicalRow],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<BioactivityChemicalsSection commonName="antioxidant" />);
    expect((await screen.findAllByText(/quercetin/i)).length).toBeGreaterThan(0);
    // "755 assays" Chip pill — the visible action button on each row.
    // Rendered once per view (desktop + mobile), so match all.
    expect(
      screen.getAllByRole("button", { name: /755 assays/i }).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText("83").length).toBeGreaterThan(0);
    expect(screen.getAllByText("261").length).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/IC50: 17\.2 MICROMOLAR/).length,
    ).toBeGreaterThan(0);
  });
});

describe("BioactivityFoodsSection", () => {
  it("renders top measurement value + View assays button", async () => {
    vi.mocked(getBioactivityFoods).mockResolvedValueOnce({
      data: [foodRow],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<BioactivityFoodsSection commonName="antioxidant" />);
    expect((await screen.findAllByText(/snail/i)).length).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/Activity: 0\.519 mmol\/100g/).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByRole("button", { name: /1 assay\b/i }).length,
    ).toBeGreaterThan(0);
  });
});

describe("ChemicalBioactivitiesSection", () => {
  it("renders linked bioactivity rows", async () => {
    vi.mocked(getChemicalBioactivities).mockResolvedValueOnce({
      data: [{ ...chemicalRow, name: "antioxidant", id: "bio1" }],
      metadata: { row_count: 1 },
    });
    renderWithPagination(<ChemicalBioactivitiesSection commonName="quercetin" />);
    const links = await screen.findAllByRole("link", { name: /antioxidant/i });
    expect(links.length).toBeGreaterThan(0);
    // Every rendered link (desktop + mobile) targets the same href.
    for (const l of links) {
      expect(l).toHaveAttribute("href", "/bioactivity/antioxidant");
    }
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
      (await screen.findAllByRole("link", { name: /antioxidant/i })).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/Activity: 0\.519 mmol\/100g/).length,
    ).toBeGreaterThan(0);
  });
});

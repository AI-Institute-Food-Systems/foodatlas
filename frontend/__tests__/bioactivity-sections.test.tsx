import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type {
  BioactivityChemicalRow,
  BioactivityDiseaseRow,
  BioactivityFoodRow,
} from "@/types";

// Mock the fetcher module before importing the components so the dynamic
// `await getX(...)` calls in the async server components are deterministic.
vi.mock("@/utils/fetching", () => ({
  getBioactivityChemicals: vi.fn(),
  getBioactivityFoods: vi.fn(),
  getBioactivityDiseases: vi.fn(),
  getChemicalBioactivities: vi.fn(),
  getFoodBioactivities: vi.fn(),
  getDiseaseBioactivities: vi.fn(),
}));

import {
  getBioactivityChemicals,
  getBioactivityDiseases,
  getBioactivityFoods,
  getChemicalBioactivities,
  getDiseaseBioactivities,
  getFoodBioactivities,
} from "@/utils/fetching";
import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import DiseaseBioactivitiesSection from "@/components/entities/bioactivity/DiseaseBioactivitiesSection";

async function renderAsync(node: Promise<JSX.Element>) {
  render(await node);
}

describe("BioactivityChemicalsSection", () => {
  it("renders empty state when no measurements", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: [],
      metadata: { total_rows: 0 },
    });
    await renderAsync(
      BioactivityChemicalsSection({ commonName: "anti-inflammatory" })
    );
    expect(
      screen.getByText(/no chemical-bioactivity measurements/i)
    ).toBeInTheDocument();
  });

  it("renders rows with first-measurement potency", async () => {
    const rows: BioactivityChemicalRow[] = [
      {
        id: "c1",
        name: "quercetin",
        measurement_count: 2,
        measurements: [
          {
            attestation_id: "ba1",
            bioactivity_metadata_id: "BAM000001",
            source_assay_id: "PubChem AID:1",
            target_ids: ["UniProt:P1"],
            potency: { value: 5.0119, unit: "uM" },
            hill_curve: {
              zero_activity: 0.5,
              infinite_activity: -50,
              log_ac50: -5.3,
              hill_slope: 1.0,
            },
            evidence_source: "PubChem AID:1",
            evidence_type: "In vitro",
          },
        ],
      },
    ];
    vi.mocked(getBioactivityChemicals).mockResolvedValueOnce({
      data: rows,
      metadata: { total_rows: 1 },
    });
    await renderAsync(
      BioactivityChemicalsSection({ commonName: "anti-inflammatory" })
    );
    expect(screen.getByText(/quercetin/i)).toBeInTheDocument();
    expect(screen.getByText(/5.012 uM/)).toBeInTheDocument();
  });
});

describe("BioactivityFoodsSection", () => {
  it("renders direct/inherited counts and via-chemical column", async () => {
    const rows: BioactivityFoodRow[] = [
      {
        id: "f1",
        name: "strawberry",
        exhibit_type: "inherited",
        via_chemical_id: "c1",
        via_chemical_name: "quercetin",
        efficacy_pred: null,
        evidence_count: 2,
        evidences: [],
      },
    ];
    vi.mocked(getBioactivityFoods).mockResolvedValueOnce({
      data: rows,
      metadata: { total_rows: 1 },
    });
    await renderAsync(
      BioactivityFoodsSection({ commonName: "anti-inflammatory" })
    );
    expect(screen.getByText(/strawberry/i)).toBeInTheDocument();
    expect(screen.getAllByText(/quercetin/i).length).toBeGreaterThan(0);
    expect(screen.getByText(/0 direct, 1 inherited/)).toBeInTheDocument();
  });
});

describe("BioactivityDiseasesSection", () => {
  it("renders empty state when no associations", async () => {
    vi.mocked(getBioactivityDiseases).mockResolvedValueOnce({
      data: [],
      metadata: { total_rows: 0 },
    });
    await renderAsync(
      BioactivityDiseasesSection({ commonName: "anti-inflammatory" })
    );
    expect(
      screen.getByText(/no disease associations recorded/i)
    ).toBeInTheDocument();
  });

  it("renders target ids when present", async () => {
    const rows: BioactivityDiseaseRow[] = [
      {
        id: "d1",
        name: "asthma",
        polarity: null,
        target_ids: ["UniProt:P18054"],
        evidence_count: 1,
        evidences: [],
      },
    ];
    vi.mocked(getBioactivityDiseases).mockResolvedValueOnce({
      data: rows,
      metadata: { total_rows: 1 },
    });
    await renderAsync(
      BioactivityDiseasesSection({ commonName: "anti-inflammatory" })
    );
    expect(screen.getByText(/asthma/i)).toBeInTheDocument();
    expect(screen.getByText(/UniProt:P18054/)).toBeInTheDocument();
  });
});

describe("ChemicalBioactivitiesSection", () => {
  it("renders linked bioactivity rows", async () => {
    vi.mocked(getChemicalBioactivities).mockResolvedValueOnce({
      data: [
        {
          id: "bio1",
          name: "anti-inflammatory",
          measurement_count: 2,
          measurements: [],
        },
      ],
      metadata: { total_rows: 1 },
    });
    await renderAsync(
      ChemicalBioactivitiesSection({ commonName: "quercetin" })
    );
    const link = screen.getByRole("link", { name: /anti-inflammatory/i });
    expect(link).toHaveAttribute("href", "/bioactivity/anti-inflammatory");
  });
});

describe("FoodBioactivitiesSection", () => {
  it("links bioactivities and shows direct/inherited split", async () => {
    vi.mocked(getFoodBioactivities).mockResolvedValueOnce({
      data: [
        {
          id: "bio1",
          name: "anti-inflammatory",
          exhibit_type: "inherited",
          via_chemical_id: "c1",
          via_chemical_name: "quercetin",
          efficacy_pred: null,
          evidence_count: 1,
        },
      ],
      metadata: { total_rows: 1 },
    });
    await renderAsync(
      FoodBioactivitiesSection({ commonName: "strawberry" })
    );
    expect(
      screen.getByRole("link", { name: /anti-inflammatory/i })
    ).toBeInTheDocument();
    expect(screen.getByText(/0 direct, 1 inherited/)).toBeInTheDocument();
  });
});

describe("DiseaseBioactivitiesSection", () => {
  it("renders empty state when no associations", async () => {
    vi.mocked(getDiseaseBioactivities).mockResolvedValueOnce({
      data: [],
      metadata: { total_rows: 0 },
    });
    await renderAsync(DiseaseBioactivitiesSection({ commonName: "asthma" }));
    expect(
      screen.getByText(/no bioactivity associations recorded/i)
    ).toBeInTheDocument();
  });
});

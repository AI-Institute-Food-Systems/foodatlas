// Composition rows and the data-points modal must agree, for every source
// selection: the row's "N data points" = the modal total it opens on, a
// data point is one extraction, and deselected sources are hidden.

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});
afterEach(cleanup);

vi.mock("@/utils/fetching", () => ({
  getChemicalCompositionEvidence: vi.fn(),
  getFoodCompositionData: vi.fn(),
  getFoodCompositionCounts: vi.fn(),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/chemical/quercetin",
  useSearchParams: () => new URLSearchParams(),
}));

import ChemicalCompositionTable from "@/components/entities/chemical/ChemicalCompositionTable";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";
import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import { PaginationsProvider } from "@/context/paginationsContext";
import { TabCountsProvider, useTabCounts } from "@/context/tabCountsContext";
import type { FoodEvidence } from "@/types/Evidence";
import {
  getChemicalCompositionEvidence,
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

type Source = "FDC" | "FoodAtlas" | "PTFI";
const KEY: Record<Source, string> = { FDC: "fdc", FoodAtlas: "foodatlas", PTFI: "ptfi" };

// Each record: (source, low-trust flags per extraction). PTFI holds one
// record with three extractions, the case record-counting got wrong.
const RECORDS: [Source, boolean[]][] = [
  ["FDC", [false]],
  ["FoodAtlas", [false, true]],
  ["PTFI", [false]],
  ["PTFI", [false, true, false]],
];

const EVIDENCES: FoodEvidence[] = RECORDS.map(([source, flags], i) => ({
  premise: "",
  reference: {
    id: `${source}-${i}`,
    source_name: source,
    display_name: `${source} record ${i}`,
    url: "https://example.test",
  },
  extraction: flags.map((low, j) => ({
    extracted_chemical_name: `chem ${i}-${j}`,
    trust_low: low,
  })),
})) as never;

/** Oracle: extractions in `sources` (all when empty), optionally low-trust only. */
const expected = (sources: Source[], lowTrustOnly: boolean) =>
  RECORDS.filter(([s]) => sources.length === 0 || sources.includes(s)).reduce(
    (n, [, flags]) => n + flags.filter((low) => !lowTrustOnly || low).length,
    0
  );

const ALL: Source[] = ["FDC", "FoodAtlas", "PTFI"];
const SUBSETS: Source[][] = [1, 2, 3, 4, 5, 6, 7].map((mask) =>
  ALL.filter((_, i) => mask & (1 << i))
);

const shownCount = async (): Promise<number> => {
  let n = NaN;
  await waitFor(() => {
    n = Number(
      document.body.querySelector("[data-filtered-count]")?.getAttribute("data-filtered-count")
    );
    expect(Number.isNaN(n)).toBe(false);
  });
  return n;
};

describe("data-points modal opens on the page's sources", () => {
  it.each(
    SUBSETS.flatMap((s) => [
      [s.join("+"), "all", s] as const,
      [s.join("+"), "low-trust", s] as const,
    ])
  )("sources %s, quality %s", async (_label, quality, sources) => {
    render(
      <FoodCompositionEvidenceModal
        foodName="pepper"
        chemicalName="quercetin"
        evidences={EVIDENCES}
        isOpen
        onClose={() => {}}
        initialFilter={quality}
        selectedSources={sources.map((s) => KEY[s])}
      />
    );
    expect(await shownCount()).toBe(
      expected(sources.length === 3 ? [] : [...sources], quality === "low-trust")
    );
    // Deselected sources are hidden, not greyed out.
    for (const s of ALL.filter((x) => !sources.includes(x))) {
      expect(document.body.textContent).not.toContain(`${s} record`);
    }
  });
});

const Badge = () => {
  const { counts } = useTabCounts();
  return <output data-testid="badge">{String(counts.composition ?? "")}</output>;
};

// Row counts as the API sends them: extractions per source.
const ROW = {
  id: "f1",
  name: "pepper",
  median_concentration: { value: 5, unit: "mg/100g" },
  fdc_count: expected(["FDC"], false),
  foodatlas_count: expected(["FoodAtlas"], false),
  ptfi_count: expected(["PTFI"], false),
  evidence_count: expected([], false),
};
const OTHER = { id: "f2", name: "kale", median_concentration: { value: 1, unit: "mg/100g" }, fdc_count: 1, evidence_count: 1 };

describe("chemical composition table → modal", () => {
  it.each(SUBSETS.map((s) => [s.join("+"), s] as const))(
    "sources %s: button = modal total, badge = table rows",
    async (_label, sources) => {
      vi.mocked(getChemicalCompositionEvidence).mockResolvedValue(EVIDENCES as never);
      render(
        <TabCountsProvider>
          <PaginationsProvider>
            <ChemicalCompositionTable
              withConcentrations={[ROW, OTHER] as never}
              withoutConcentrations={[]}
              commonName="quercetin"
              chemicalId="c1"
            />
          </PaginationsProvider>
          <Badge />
        </TabCountsProvider>
      );
      for (const s of sources) {
        const chip = screen
          .getAllByRole("button")
          .find((b) => new RegExp(`^${s}\\s*\\d*$`).test((b.textContent ?? "").trim()));
        expect(chip, `no ${s} chip`).toBeTruthy();
        fireEvent.click(chip!);
      }
      const want = expected(sources.length === 3 ? [] : [...sources], false);
      const rowsShown = sources.includes("FDC") ? 2 : 1;
      await waitFor(() =>
        expect(screen.getByTestId("badge").textContent).toBe(String(rowsShown))
      );
      const label = `${want} data point${want === 1 ? "" : "s"}`;
      fireEvent.click(screen.getAllByRole("button", { name: new RegExp(`^${label}$`) })[0]);
      // The modal fetches its evidence on open; wait for the loaded total.
      await waitFor(async () => expect(await shownCount()).toBe(want));
    }
  );
});

describe("food composition table → modal", () => {
  const byKey = (key: string) =>
    EVIDENCES.filter((ev) => KEY[ev.reference.source_name as Source] === key);

  it.each(SUBSETS.map((s) => [s.join("+"), s] as const))(
    "sources %s: button = modal total",
    async (_label, sources) => {
      vi.mocked(getFoodCompositionData).mockImplementation((async () => ({
        data: [
          {
            id: "c1",
            name: "quercetin",
            median_concentration: null,
            chemical_classification: [],
            fdc_evidences: byKey("fdc"),
            foodatlas_evidences: byKey("foodatlas"),
            ptfi_evidences: byKey("ptfi"),
          },
        ],
        metadata: {
          row_count: 1,
          rows_per_page: 25,
          current_row: 1,
          current_page: 1,
          total_rows: 1,
          total_pages: 1,
          highlight_page: null,
        },
      })) as never);
      vi.mocked(getFoodCompositionCounts).mockResolvedValue({
        source_counts: { fdc: 1, foodatlas: 1, ptfi: 1 },
        classification_counts: {},
        no_concentration_count: 1,
        low_trust_count: 0,
      } as never);
      render(
        <TabCountsProvider>
          <PaginationsProvider>
            <FoodCompositionSection commonName="pepper" />
          </PaginationsProvider>
        </TabCountsProvider>
      );
      await screen.findAllByText(/quercetin/i);
      // Every source starts selected; deselect the ones not in this case.
      for (const s of ALL.filter((x) => !sources.includes(x))) {
        const chip = screen
          .getAllByRole("button")
          .find((b) => new RegExp(`^${s}\\s*\\d*$`).test((b.textContent ?? "").trim()));
        expect(chip, `no ${s} chip`).toBeTruthy();
        fireEvent.click(chip!);
      }
      const want = expected(sources.length === 3 ? [] : [...sources], false);
      const label = `${want} data point${want === 1 ? "" : "s"}`;
      const button = await screen.findAllByRole("button", { name: new RegExp(`^${label}$`) });
      fireEvent.click(button[0]);
      await waitFor(async () => expect(await shownCount()).toBe(want));
    }
  );
});

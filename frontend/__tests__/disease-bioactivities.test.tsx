import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/utils/fetching", () => ({
  getDiseaseBioactivities: vi.fn(),
  getDiseaseBioactivityChemicals: vi.fn(),
}));

vi.mock("@/context/pageReadyContext", () => ({
  useLoadingGate: () => undefined,
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import DiseaseBioactivitiesSection from "@/components/entities/disease/DiseaseBioactivitiesSection";
import {
  getDiseaseBioactivities,
  getDiseaseBioactivityChemicals,
} from "@/utils/fetching";

const dietary = (over: Record<string, unknown> = {}) => ({
  food_name: "olive",
  food_foodatlas_id: "f1",
  food_conc_mg_per_100g: 6124,
  conc_quality_flag: "ok",
  efficacy_fraction: 1,
  dose_over_ac50_log: 3.97,
  conc_vs_ac50: "above",
  logac50: -5.1,
  n_curves: 2,
  endpoint_type: "IC50",
  saturated: true,
  ...over,
});

const chemRow = (over: Record<string, unknown> = {}) => ({
  bioactivity_name: "anticancer",
  bioactivity_foodatlas_id: "b1",
  chemical_name: "quercetin",
  chemical_foodatlas_id: "c1",
  n_assays: 5,
  n_active_measurements: 12,
  relationships: ["therapeutic"],
  dietary: dietary(),
  ...over,
});

const summaryRow = (over: Record<string, unknown> = {}) => ({
  bioactivity_name: "anticancer",
  bioactivity_foodatlas_id: "b1",
  n_chemicals: 2,
  n_dietary_chemicals: 1,
  n_assays: 5,
  n_active_measurements: 12,
  best_dose_over_ac50_log: 3.97,
  ...over,
});

const mount = async (
  rows: Record<string, unknown>[],
  summary: Record<string, unknown>[] = [summaryRow()]
) => {
  vi.mocked(getDiseaseBioactivities).mockResolvedValue({
    data: summary,
    metadata: { row_count: summary.length },
  });
  vi.mocked(getDiseaseBioactivityChemicals).mockResolvedValue({
    data: rows,
    metadata: { row_count: rows.length, n_dietary: 0 },
  });
  render(<DiseaseBioactivitiesSection commonName="melanoma" />);
  await waitFor(() =>
    expect(vi.mocked(getDiseaseBioactivityChemicals)).toHaveBeenCalled()
  );
};

// The section renders the desktop table and the mobile card list at the same
// time — CSS decides which is visible, so every row's text appears twice in
// jsdom. Assert on presence/absence rather than on a single node.
const shown = (text: string | RegExp) => screen.queryAllByText(text).length > 0;

describe("DiseaseBioactivitiesSection", () => {
  it("renders a dietary-backed row with its food and efficacy", async () => {
    await mount([chemRow()]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    expect(shown("olive")).toBe(true);
    expect(shown(">99%")).toBe(true);
    expect(shown("+3.97")).toBe(true);
  });

  it("hides assay-only rows by default and reveals them on toggle", async () => {
    await mount([
      chemRow(),
      chemRow({
        chemical_name: "vorinostat",
        chemical_foodatlas_id: "c2",
        dietary: null,
      }),
    ]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    // Assay-only chemical is filtered out while "Found in food" is active.
    expect(shown("vorinostat")).toBe(false);

    fireEvent.click(screen.getByRole("button", { name: /found in food/i }));
    await waitFor(() => expect(shown("vorinostat")).toBe(true));
  });

  it("filters rows by bioactivity chip", async () => {
    await mount(
      [
        chemRow(),
        chemRow({
          bioactivity_name: "antiviral",
          bioactivity_foodatlas_id: "b2",
          chemical_name: "resveratrol",
          chemical_foodatlas_id: "c3",
        }),
      ],
      [
        summaryRow(),
        summaryRow({
          bioactivity_name: "antiviral",
          bioactivity_foodatlas_id: "b2",
        }),
      ]
    );
    await waitFor(() => expect(shown("resveratrol")).toBe(true));

    fireEvent.click(screen.getByRole("button", { name: /^antiviral/i }));
    await waitFor(() => expect(shown("quercetin")).toBe(false));
    expect(shown("resveratrol")).toBe(true);
  });

  it("flags a suspect concentration rather than hiding the row", async () => {
    await mount([
      chemRow({ dietary: dietary({ conc_quality_flag: "suspect_high" }) }),
    ]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    expect(
      screen.getAllByLabelText(/flagged as implausibly high/i).length
    ).toBeGreaterThan(0);
  });

  it("shows an empty state when the disease has no rows", async () => {
    await mount([], []);
    expect(
      await screen.findByText(/no assay-attributed bioactivities/i)
    ).toBeInTheDocument();
  });
});

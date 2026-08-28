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

const chemRow = (over: Record<string, unknown> = {}) => ({
  bioactivity_name: "anticancer",
  bioactivity_foodatlas_id: "b1",
  chemical_name: "quercetin",
  chemical_foodatlas_id: "c1",
  n_assays: 5,
  n_active_measurements: 12,
  relationships: ["therapeutic"],
  target_genes: ["NCBIGene: 7157"],
  targets: [{ id: "NCBIGene: 7157", label: "Cellular tumor antigen p53" }],
  assays: ["AID: 1"],
  literature_directions: [],
  ...over,
});

const summaryRow = (over: Record<string, unknown> = {}) => ({
  bioactivity_name: "anticancer",
  bioactivity_foodatlas_id: "b1",
  n_chemicals: 2,
  n_assays: 5,
  n_active_measurements: 12,
  n_therapeutic: 1,
  n_marker: 1,
  n_literature: 0,
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
    metadata: { row_count: rows.length },
  });
  render(<DiseaseBioactivitiesSection commonName="melanoma" />);
  await waitFor(() =>
    expect(vi.mocked(getDiseaseBioactivityChemicals)).toHaveBeenCalled()
  );
};

// Desktop table and mobile card list both render; CSS picks one, so every
// row's text appears twice in jsdom. Assert on presence, not on a single node.
const shown = (text: string | RegExp) => screen.queryAllByText(text).length > 0;

// Same reason: FilterPanel puts the search box in the sidebar AND the
// trigger row, so this is never a single node either.
const searchBox = () =>
  screen.getAllByRole("textbox", { name: /search/i })[0];

describe("DiseaseBioactivitiesSection", () => {
  it("renders a chemical row with its assay counts", async () => {
    await mount([chemRow()]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    expect(shown("anticancer")).toBe(true);
    expect(shown("therapeutic")).toBe(true);
  });

  it("shows every chemical, including ones absent from food", async () => {
    await mount([
      chemRow(),
      chemRow({ chemical_name: "vorinostat", chemical_foodatlas_id: "c2" }),
    ]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    expect(shown("vorinostat")).toBe(true);
  });

  it("filters rows by the activity facet", async () => {
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

    // A radio, not a Chip pill: the shared FilterPanel renders the facet
    // list, and a row belongs to exactly one activity.
    fireEvent.click(screen.getByRole("radio", { name: /^antiviral/i }));
    await waitFor(() => expect(shown("quercetin")).toBe(false));
    expect(shown("resveratrol")).toBe(true);
  });

  it("filters rows by the shared search box", async () => {
    await mount([
      chemRow(),
      chemRow({ chemical_name: "vorinostat", chemical_foodatlas_id: "c2" }),
    ]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));

    // FilterPanel renders the search node into both the sidebar and the
    // trigger row, so there are two of them; either drives the same state.
    fireEvent.change(searchBox(), { target: { value: "vorinostat" } });
    await waitFor(() => expect(shown("quercetin")).toBe(false));
    expect(shown("vorinostat")).toBe(true);
  });

  it("offers a way out when the filters match nothing", async () => {
    await mount([chemRow()]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));

    fireEvent.change(searchBox(), { target: { value: "nothing-matches" } });
    await waitFor(() =>
      expect(shown(/no bioactivities match your filters/i)).toBe(true)
    );

    fireEvent.click(screen.getByRole("button", { name: /clear filters/i }));
    await waitFor(() => expect(shown("quercetin")).toBe(true));
  });

  it("does not surface food doses or efficacy figures", async () => {
    // Regression guard: these were pulled for overclaiming precision the
    // density-1 concentration proxy can't support.
    await mount([chemRow()]);
    await waitFor(() => expect(shown("quercetin")).toBe(true));
    expect(screen.queryByText(/best dietary source/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/efficacy/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/dose vs ac50/i)).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /found in food/i })
    ).not.toBeInTheDocument();
  });

  it("shows an empty state when the disease has no rows", async () => {
    await mount([], []);
    expect(
      await screen.findByText(/no assay-attributed bioactivities/i)
    ).toBeInTheDocument();
  });
});

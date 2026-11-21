// Every table that opens the measurements modal must open it on the
// page's Evidence, Unit and Source filters, and the modal must then show
// the same number as the row's "N assays" button.

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});
afterEach(cleanup);

const m = (evidence_type: string, unit: string, evidence_source: string) => ({
  assay: `a-${evidence_type}-${unit}-${evidence_source}`,
  endpoint: "IC50",
  outcome: "Active",
  value: 1,
  evidence_type,
  unit,
  evidence_source,
});
// Only the first assay is "in vitro" AND "uM" AND experimental.
const MEASUREMENTS = [
  m("in vitro", "uM", "Experimental"),
  m("in vitro", "nM", "Experimental"),
  m("in vivo", "uM", "Experimental"),
  m("in vitro", "uM", "predicted"),
];

vi.mock("@/utils/fetching", () => ({
  getBioactivityChemicals: vi.fn(),
  getFoodInferredBioactivities: vi.fn(),
  getChemicalBioactivities: vi.fn(),
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
  getBioactivityEndpointOptions: vi.fn().mockResolvedValue([
    { endpoint: "", unit: "uM", count: 1 },
    { endpoint: "", unit: "nM", count: 1 },
  ]),
  getBioactivityCategoryOptions: vi.fn().mockResolvedValue([]),
  getBioactivitySourceKindCounts: vi
    .fn()
    .mockResolvedValue({ both: 1, experimental: 1, predicted: 1 }),
  getBioactivityEvidenceTypeCounts: vi.fn().mockResolvedValue([
    { evidence_type: "in vitro", count: 1 },
    { evidence_type: "in vivo", count: 1 },
  ]),
  NO_SIDEBAR_FILTERS: {},
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import { PaginationsProvider } from "@/context/paginationsContext";
import {
  getBioactivityChemicals,
  getChemicalBioactivities,
  getFoodInferredBioactivities,
} from "@/utils/fetching";

const mount = (node: ReactElement) =>
  render(<PaginationsProvider>{node}</PaginationsProvider>);

const option = (label: string) =>
  screen
    .getAllByRole("button")
    .concat(screen.queryAllByRole("radio"))
    .find((el) =>
      new RegExp(`^${label}\\s*\\d*$`, "i").test((el.textContent ?? "").trim())
    );

const modalCount = async (): Promise<number> => {
  let n = NaN;
  await waitFor(() => {
    const el = document.body.querySelector("[data-filtered-count]");
    n = Number(el?.getAttribute("data-filtered-count"));
    expect(Number.isNaN(n)).toBe(false);
  });
  return n;
};

describe("table → measurements modal carries the page filters", () => {
  it("bioactivity chemicals table", async () => {
    vi.mocked(getBioactivityChemicals).mockResolvedValue({
      data: [
        {
          id: "c1",
          name: "quercetin",
          measurement_count: 1,
          active_count: 1,
          inactive_count: 0,
          measurements: MEASUREMENTS,
        },
      ],
      metadata: { row_count: 1, total_rows: 1 },
    } as never);
    mount(<BioactivityChemicalsSection commonName="antioxidant" />);
    await screen.findAllByText(/quercetin/i);

    await waitFor(() => expect(option("in vitro")).toBeTruthy());
    fireEvent.click(option("in vitro")!);
    await waitFor(() => expect(option("uM")).toBeTruthy());
    fireEvent.click(option("uM")!);
    fireEvent.click(option("Experimental")!);
    await waitFor(() =>
      expect(vi.mocked(getBioactivityChemicals).mock.lastCall?.[1]).toMatchObject({
        filterEvidenceType: "in vitro",
        filterUnit: "uM",
        filterSourceKind: "experimental",
      })
    );

    fireEvent.click(screen.getAllByRole("button", { name: /1 assay/i })[0]);
    expect(await modalCount()).toBe(1);
  });

  it("food inferred-bioactivities table", async () => {
    vi.mocked(getFoodInferredBioactivities).mockResolvedValue({
      data: [
        {
          bioactivity: "antioxidant",
          bioactivity_id: "b1",
          chemical: "quercetin",
          chemical_id: "c1",
          measurement_count: 1,
          measurements: MEASUREMENTS,
        },
      ],
      metadata: { row_count: 1, total_rows: 1 },
    } as never);
    vi.mocked(getChemicalBioactivities).mockResolvedValue({
      data: [{ id: "b1", measurements: MEASUREMENTS }],
      metadata: { row_count: 1 },
    } as never);
    mount(
      <FoodInferredBioactivitiesSection
        commonName="apple"
        externalSearch=""
        externalEvidenceType="in vitro"
        externalUnit="uM"
        externalSourceKind="experimental"
      />
    );
    fireEvent.click((await screen.findAllByRole("button", { name: /1 assay/i }))[0]);
    expect(await modalCount()).toBe(1);
  });
});

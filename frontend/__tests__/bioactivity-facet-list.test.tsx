// The server-driven facets keep every option and list them alphabetically.
//
// The two surfaces named in the report: on the food page's Bioactivities
// tab, picking an Assay Source made the units that source did not carry
// vanish from the sidebar; on a bioactivity page's Chemicals table the
// Unit list did the same and was busiest-first besides. Both built their
// option list from a faceted counts endpoint, which — being a GROUP BY —
// only returns values with rows under the current filters.
//
// These surfaces are excluded from facet-invariants.test.tsx for their
// COUNTS (server-side, pinned by backend tests). The LIST is client
// wiring, so it is asserted here against a mock that behaves like the
// real endpoints: drops zero groups, returns busiest-first.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));

// One synthetic dataset behind every direction: three units and three
// evidence types, each tagged with the assay source kind that carries it.
// `nM` and `in vivo` are predicted-only, so "Experimental" zeroes them.
const UNITS = [
  { unit: "uM", count: 40, kind: "experimental" },
  { unit: "nM", count: 5, kind: "predicted" },
  { unit: "ug/mL", count: 12, kind: "experimental" },
];
const EVIDENCE = [
  { evidence_type: "in vitro", count: 30, kind: "experimental" },
  { evidence_type: "in vivo", count: 2, kind: "predicted" },
  { evidence_type: "adme/tox", count: 9, kind: "experimental" },
];
const CATEGORIES = [
  { category: "flavonoid", count: 20, kind: "experimental" },
  { category: "alkaloid", count: 3, kind: "predicted" },
];

// Like the real endpoints: apply the source-kind filter, drop what has
// no rows left, busiest first.
const facet = <T extends { count: number; kind: string }>(
  rows: T[],
  filters: { filterSourceKind?: string }
) =>
  rows
    .filter((r) => !filters.filterSourceKind || r.kind === filters.filterSourceKind)
    .sort((a, b) => b.count - a.count)
    .map(({ kind: _k, ...rest }) => rest);

vi.mock("@/utils/fetching", () => ({
  NO_SIDEBAR_FILTERS: {},
  getBioactivityChemicals: vi.fn().mockResolvedValue({
    data: [
      {
        id: "c1",
        name: "quercetin",
        measurement_count: 1,
        active_count: 1,
        inactive_count: 0,
        top_measurement: null,
        measurements: [],
      },
    ],
    metadata: { row_count: 1, total_rows: 1, total_pages: 1, current_page: 1, rows_per_page: 20 },
  }),
  getFoodBioactivities: vi.fn().mockResolvedValue({
    data: [],
    metadata: { row_count: 0, total_rows: 0, total_pages: 0, current_page: 1, rows_per_page: 20 },
  }),
  getFoodInferredBioactivities: vi.fn().mockResolvedValue({
    data: [],
    metadata: { row_count: 0, total_rows: 0, total_pages: 0 },
  }),
  getChemicalBioactivities: vi.fn().mockResolvedValue({ data: [] }),
  getBioactivityEndpointOptions: vi.fn(
    (_n: string, _d: string, f: { filterSourceKind?: string } = {}) =>
      Promise.resolve(
        facet(UNITS, f).map((u) => ({ endpoint: "IC50", ...u }))
      )
  ),
  getBioactivityEvidenceTypeCounts: vi.fn(
    (_n: string, _d: string, f: { filterSourceKind?: string } = {}) =>
      Promise.resolve(facet(EVIDENCE, f))
  ),
  getBioactivityCategoryOptions: vi.fn(
    (_n: string, f: { filterSourceKind?: string } = {}) =>
      Promise.resolve(facet(CATEGORIES, f))
  ),
  getBioactivitySourceKindCounts: vi
    .fn()
    .mockResolvedValue({ both: 57, experimental: 52, predicted: 5 }),
}));

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import FoodBioactivitiesTab from "@/components/entities/bioactivity/FoodBioactivitiesTab";
import { PaginationsProvider } from "@/context/paginationsContext";

const mount = (node: ReactElement) =>
  render(<PaginationsProvider>{node}</PaginationsProvider>);

/** The option labels under a FilterGroup heading, top to bottom. */
const optionsUnder = (heading: string): HTMLButtonElement[] => {
  const label = screen
    .getAllByText(heading, { exact: true })
    .find((el) => el.tagName === "SPAN");
  expect(label, `no filter group headed "${heading}"`).toBeTruthy();
  const group = label!.parentElement!.parentElement!;
  return Array.from(
    group.querySelectorAll<HTMLButtonElement>(
      'button[aria-pressed], button[role="radio"]'
    )
  );
};
const labelsUnder = (heading: string) =>
  optionsUnder(heading).map((b) =>
    (
      Array.from(b.children).find(
        (c) => c.tagName === "SPAN" && !c.hasAttribute("aria-hidden") && !c.className.includes("tabular-nums")
      )?.textContent ?? ""
    ).trim()
  );
const countUnder = (heading: string, label: string): number => {
  const i = labelsUnder(heading).indexOf(label);
  const span = optionsUnder(heading)[i]?.querySelector(".tabular-nums");
  return Number((span?.textContent ?? "").replace(/,/g, ""));
};
const pick = (heading: string, label: string) => {
  const i = labelsUnder(heading).indexOf(label);
  expect(i, `no option "${label}" under ${heading}`).toBeGreaterThanOrEqual(0);
  fireEvent.click(optionsUnder(heading)[i]);
};

describe.each([
  {
    name: "BioactivityChemicalsSection (bioactivity → chemicals)",
    node: <BioactivityChemicalsSection commonName="antioxidant" />,
    groups: ["Unit", "Evidence", "Category"],
    directions: 1,
  },
  {
    name: "FoodBioactivitiesTab (food → bioactivities)",
    node: <FoodBioactivitiesTab commonName="onion" />,
    groups: ["Unit", "Evidence"],
    // The tab's sidebar drives a direct AND an inferred table and sums
    // their counts; the mock answers both directions the same.
    directions: 2,
  },
])("$name", ({ node, groups, directions }) => {
  beforeEach(() => vi.clearAllMocks());

  it("lists options alphabetically, not in the order the server sent", async () => {
    mount(node);
    await waitFor(() => expect(labelsUnder("Unit")).toHaveLength(3));
    // The mock answered uM (40), ug/mL (12), nM (5).
    expect(labelsUnder("Unit")).toEqual(["nM", "ug/mL", "uM"]);
    if (groups.includes("Evidence")) {
      await waitFor(() => expect(labelsUnder("Evidence")).toHaveLength(3));
      expect(labelsUnder("Evidence")).toEqual(["adme/tox", "in vitro", "in vivo"]);
    }
    if (groups.includes("Category")) {
      await waitFor(() => expect(labelsUnder("Category")).toHaveLength(2));
      expect(labelsUnder("Category")).toEqual(["alkaloid", "flavonoid"]);
    }
  });

  it("keeps every option, disabled at zero, after picking an Assay Source", async () => {
    mount(node);
    await waitFor(() => expect(labelsUnder("Unit")).toHaveLength(3));
    const before = Object.fromEntries(groups.map((g) => [g, labelsUnder(g)]));

    pick("Assay Source", "Experimental");

    // nM is predicted-only, so the counts endpoint no longer returns it.
    await waitFor(() => expect(countUnder("Unit", "nM")).toBe(0));
    for (const g of groups) {
      expect(labelsUnder(g), `${g} changed shape`).toEqual(before[g]);
    }
    const nM = optionsUnder("Unit")[labelsUnder("Unit").indexOf("nM")];
    expect(nM).toBeDisabled();
    expect(countUnder("Unit", "uM")).toBe(40 * directions);
    if (groups.includes("Evidence")) {
      await waitFor(() => expect(countUnder("Evidence", "in vivo")).toBe(0));
      const inVivo = optionsUnder("Evidence")[labelsUnder("Evidence").indexOf("in vivo")];
      expect(inVivo).toBeDisabled();
    }
  });

  it("fetches the option set with no filters and the counts with them", async () => {
    const { getBioactivityEndpointOptions } = await import("@/utils/fetching");
    mount(node);
    await waitFor(() => expect(labelsUnder("Unit")).toHaveLength(3));
    pick("Assay Source", "Predicted");
    await waitFor(() => expect(countUnder("Unit", "uM")).toBe(0));

    const filters = vi
      .mocked(getBioactivityEndpointOptions)
      .mock.calls.map(([, , f]) => f?.filterSourceKind ?? "");
    // At least one universe call (no source kind) and one faceted call.
    expect(filters).toContain("");
    expect(filters).toContain("predicted");
  });
});

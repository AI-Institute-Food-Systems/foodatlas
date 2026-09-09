import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

// jsdom has no ResizeObserver; the section observes its table wrapper to
// position the highlight overlay. Without this the whole tree throws and
// the body renders empty — which makes negative assertions pass for the
// wrong reason.
beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/utils/fetching", () => ({
  getFoodCompositionData: vi.fn(),
  getFoodCompositionCounts: vi.fn(),
}));

const reporter = vi.hoisted(() => ({ getRowProps: () => ({}) }));
vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => reporter,
}));
// Stable identities. The section lists setTablePaginations in the
// data-fetch effect's dependency array, so a factory that returns a fresh
// vi.fn() per render re-runs the effect forever ("Maximum update depth
// exceeded") rather than failing an assertion.
const pagination = vi.hoisted(() => ({
  getTablePaginations: () => ({ currentPage: 1, rowsPerPage: 20 }),
  setTablePaginations: vi.fn(),
}));
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => pagination,
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/food/pepper%20(raw)",
  useSearchParams: () => new URLSearchParams(),
}));

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import {
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

// A scaled-down pepper (raw): FDC has nothing, FoodAtlas has most rows,
// PTFI has one that FoodAtlas doesn't — the shape that made
// filter_source=fdc+ptfi return the unfiltered set.
const evidence = (source: "FoodAtlas" | "FDC" | "PTFI") => [
  {
    premise: "",
    extraction: [],
    reference: {
      id: `${source}-1`,
      source_name: source,
      display_name: `${source} record`,
      url: "https://example.test",
    },
  },
];

const ROWS = [
  {
    id: "e1",
    name: "quercetin",
    median_concentration: { unit: "mg", value: 5, converted: false, base_units: [] },
    foodatlas_evidences: evidence("FoodAtlas"),
    fdc_evidences: null,
    ptfi_evidences: null,
    chemical_classification: ["flavonoid"],
  },
  {
    id: "e2",
    name: "kaempferol",
    median_concentration: { unit: "mg", value: 3, converted: false, base_units: [] },
    foodatlas_evidences: evidence("FoodAtlas"),
    fdc_evidences: null,
    ptfi_evidences: null,
    chemical_classification: ["flavonoid"],
  },
  {
    id: "e3",
    name: "capsaicin",
    median_concentration: null,
    foodatlas_evidences: null,
    fdc_evidences: null,
    ptfi_evidences: evidence("PTFI"),
    chemical_classification: ["alkaloid"],
  },
];

const COUNTS = { fdc: 0, foodatlas: 2, ptfi: 1 };

// The mock answers the source filter the way a correct server does: it
// returns only rows with evidence in a selected source, and reports
// metadata that describes THAT set. A component that re-filters what it
// receives, or reads its pager off a different set, contradicts itself
// against this.
const mockServer = () => {
  vi.mocked(getFoodCompositionData).mockImplementation(
    (async (
      _commonName: string,
      _page: number,
      sourceFilters: string[],
    ) => {
      const rows = ROWS.filter((r) =>
        sourceFilters.some(
          (s) => r[`${s}_evidences` as keyof typeof r] !== null,
        ),
      );
      return {
        data: rows,
        metadata: {
          row_count: rows.length,
          rows_per_page: 25,
          current_row: 1,
          current_page: 1,
          total_rows: rows.length,
          total_pages: rows.length > 0 ? 1 : 0,
          highlight_page: null,
        },
      };
    }) as never,
  );
  vi.mocked(getFoodCompositionCounts).mockResolvedValue({
    source_counts: COUNTS,
    classification_counts: { flavonoid: 2, alkaloid: 1 },
    no_concentration_count: 1,
    low_trust_count: 0,
    total_row_count: ROWS.length,
  } as never);
};

// Every section renders a desktop <table> AND a mobile card list, and
// jsdom honours neither `hidden md:block` nor `md:hidden` — so
// getAllByText double-counts. Scope row counting to the real table.
// (Same reason as bioactivity-sections.test.tsx.)
//
// The empty state and the page-height padding are themselves <tr>s, so a
// bare tbody count is never 0 and would make "the table isn't empty"
// vacuously true. Count only rows carrying a chemical cell.
const desktopRowCount = (container: HTMLElement): number =>
  container.querySelectorAll("table tbody tr td:first-child a").length;

const desktopRowNames = (container: HTMLElement): string[] =>
  Array.from(container.querySelectorAll("table tbody tr")).map(
    (tr) => tr.textContent ?? "",
  );

// The filters-active empty state. Its presence alongside a positive
// total_rows is the exact contradiction the reported bug produced.
const emptyStateShown = (): boolean =>
  screen.queryAllByText(/No associations match your filters/i).length > 0;

const mount = async () => {
  const view = render(<FoodCompositionSection commonName="pepper (raw)" />);
  await waitFor(() => expect(desktopRowCount(view.container)).toBe(3));
  return view;
};

// FilterOption renders a <button aria-pressed>, not a real checkbox.
const sourceChip = (name: string): HTMLElement => {
  const matches = screen.getAllByRole("button", {
    name: new RegExp(`^${name}\\b`, "i"),
  });
  // Sidebar + mobile drawer render the same control twice.
  return matches[0];
};

describe("composition source filter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockServer();
  });

  it("shows the PTFI row once FoodAtlas is deselected", async () => {
    const { container } = await mount();
    expect(desktopRowNames(container).join(" ")).toContain("quercetin");

    fireEvent.click(sourceChip("FoodAtlas"));

    await waitFor(() => expect(desktopRowCount(container)).toBe(1));
    expect(desktopRowNames(container).join(" ")).toContain("capsaicin");
    expect(desktopRowNames(container).join(" ")).not.toContain("quercetin");
  });

  it("asks the server for the narrowed source set", async () => {
    await mount();
    fireEvent.click(sourceChip("FoodAtlas"));

    await waitFor(() => {
      const last = vi.mocked(getFoodCompositionData).mock.calls.at(-1);
      // 3rd positional arg is sourceFilters.
      expect(last?.[2]).not.toContain("foodatlas");
    });
    // ...and it is a real narrowing, not an empty selection.
    const last = vi.mocked(getFoodCompositionData).mock.calls.at(-1);
    expect(last?.[2]).toEqual(expect.arrayContaining(["fdc", "ptfi"]));
  });

  it("keeps a zero-count source deselectable while it is still selected", async () => {
    await mount();
    // FDC has count 0 but ships selected, so disabling it on count alone
    // traps the user: "PTFI alone" becomes unreachable by clicking.
    const fdc = sourceChip("FDC");
    expect(fdc).not.toBeDisabled();

    fireEvent.click(fdc);
    // Once deselected there is nothing to come back for — 0 rows — so
    // the usual zero-count disabling applies again.
    await waitFor(() => expect(sourceChip("FDC")).toBeDisabled());
  });

  it("never renders an empty table while the pager reports rows", async () => {
    // The reported bug, as an invariant the component must hold on its
    // own — independently of the server fix.
    //
    // Here the mock plays the BROKEN server: it ignores sourceFilters and
    // answers a two-of-three selection with the unfiltered set, whose
    // page 1 happens to be FoodAtlas-only (on real pepper (raw), pages
    // 1-8 were, because PTFI ships no median to sort by). The reader has
    // just deselected FoodAtlas.
    //
    // With the client-side re-filter in place, all rows on the page were
    // dropped while total_rows/total_pages still came from this same
    // unfiltered response — an empty table under a pager promising 3
    // rows. Deleting the re-filter is what makes that unrepresentable:
    // the table can only ever render what the metadata describes.
    const foodatlasOnlyPage = ROWS.filter((r) => r.foodatlas_evidences);
    vi.mocked(getFoodCompositionData).mockResolvedValue({
      data: foodatlasOnlyPage,
      metadata: {
        row_count: foodatlasOnlyPage.length,
        rows_per_page: 25,
        current_row: 1,
        current_page: 1,
        total_rows: ROWS.length,
        total_pages: 1,
        highlight_page: null,
      },
    } as never);

    const { container } = render(
      <FoodCompositionSection commonName="pepper (raw)" />,
    );
    await waitFor(() => expect(desktopRowCount(container)).toBe(2));

    fireEvent.click(sourceChip("FoodAtlas"));
    await waitFor(() =>
      expect(
        vi.mocked(getFoodCompositionData).mock.calls.at(-1)?.[2],
      ).not.toContain("foodatlas"),
    );

    // total_rows is 3 > 0, so the table must show rows — not the
    // "nothing matches your filters" state under a pager saying 3.
    await waitFor(() => expect(desktopRowCount(container)).toBeGreaterThan(0));
    expect(emptyStateShown()).toBe(false);
  });

  it("reports how many rows the filters hide, and stops when they are cleared", async () => {
    const { container } = await mount();
    // Nothing hidden on an untouched table.
    expect(screen.queryByText(/hidden by filters/i)).toBeNull();

    fireEvent.click(sourceChip("FoodAtlas"));
    await waitFor(() => expect(desktopRowCount(container)).toBe(1));

    // 3 rows total, 1 visible.
    expect(await screen.findByText(/2 hidden by filters/i)).toBeInTheDocument();
    expect(screen.getByText(/3 chemicals/i)).toBeInTheDocument();

    // Scope to the summary line's own control — the sidebar reset and the
    // empty state both also read "clear filters".
    const summary = screen.getByText(/hidden by filters/i).closest("p");
    const clear = summary?.querySelector("button");
    expect(clear).toBeTruthy();
    fireEvent.click(clear as HTMLButtonElement);
    await waitFor(() =>
      expect(screen.queryByText(/hidden by filters/i)).toBeNull(),
    );
  });
});

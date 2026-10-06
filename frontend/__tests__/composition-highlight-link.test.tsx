import { render, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

// Chemical → food links moved the highlight from ?highlight= into the
// fragment so the crawlable link is the bare food URL. Both forms must still
// land on the highlighted row: the fragment for new links, the query for
// links already indexed or bookmarked.

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
// Stable identities, or the fetch effect re-runs forever (see
// composition-source-filter.test.tsx).
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
const nav = vi.hoisted(() => ({
  router: { push: () => {}, replace: () => {}, back: () => {} },
}));
vi.mock("next/navigation", () => ({
  useRouter: () => nav.router,
  usePathname: () => "/food/onion",
}));

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import {
  foodHighlightHref,
  readHighlightHash,
} from "@/components/entities/food/highlightLink";
import {
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

// The last positional argument of getFoodCompositionData is find_chemical.
const findChemicalArgs = () =>
  vi.mocked(getFoodCompositionData).mock.calls.map((c) => c[8]);

beforeEach(() => {
  vi.mocked(getFoodCompositionData).mockResolvedValue({
    data: [],
    metadata: { total_rows: 0, total_pages: 0, highlight_page: null },
  } as never);
  vi.mocked(getFoodCompositionCounts).mockResolvedValue({
    source_counts: {},
    classification_counts: {},
  } as never);
});

afterEach(() => {
  vi.clearAllMocks();
  window.history.replaceState(null, "", "/");
});

describe("highlight link format", () => {
  it("keeps the chemical out of the query string", () => {
    const href = foodHighlightHref("/food/onion", "e73791");
    expect(href).toBe("/food/onion#highlight=e73791");
    expect(new URL(href, "https://x.test").search).toBe("");
  });

  it("round-trips through the fragment", () => {
    expect(readHighlightHash("#highlight=e%2F1")).toBe("e/1");
  });

  it("ignores other fragments and malformed escapes", () => {
    expect(readHighlightHash("#composition")).toBe("");
    expect(readHighlightHash("")).toBe("");
    expect(readHighlightHash("#highlight=%E0%A4%A")).toBe("");
  });
});

describe("food composition highlight", () => {
  it("finds the chemical named in the fragment", async () => {
    window.history.replaceState(null, "", "/food/onion#highlight=E73791");
    render(<FoodCompositionSection commonName="onion" />);
    await waitFor(() => expect(findChemicalArgs()).toContain("e73791"));
  });

  it("still honours a legacy ?highlight= link", async () => {
    window.history.replaceState(null, "", "/food/onion?highlight=e73791");
    render(<FoodCompositionSection commonName="onion" />);
    await waitFor(() => expect(findChemicalArgs()).toContain("e73791"));
  });

  it("does not search for a chemical without either", async () => {
    render(<FoodCompositionSection commonName="onion" />);
    await waitFor(() =>
      expect(vi.mocked(getFoodCompositionData)).toHaveBeenCalled()
    );
    expect(findChemicalArgs().every((a) => !a)).toBe(true);
  });
});

import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

// The food page fetches page 1 of the composition table on the server and
// seeds the client table with it, so the rows (and their chemical links) are
// in the HTML crawlers get. The seed must stand in for the first fetch only
// when it describes what the URL asks for.

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
const pagination = vi.hoisted(() => ({
  page: 1,
  getTablePaginations: () => ({ currentPage: pagination.page, rowsPerPage: 20 }),
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
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

const SEED = {
  data: [
    {
      id: "e1",
      name: "quercetin",
      median_concentration: { unit: "mg", value: 5, converted: false, base_units: [] },
      foodatlas_evidences: [],
      fdc_evidences: null,
      ptfi_evidences: null,
      chemical_classification: ["flavonoid"],
    },
  ],
  metadata: { total_pages: 1, total_rows: 1, highlight_page: null },
};

const chemicalLinks = (container: HTMLElement) =>
  Array.from(container.querySelectorAll("table tbody a")).map((a) =>
    a.getAttribute("href")
  );

beforeEach(() => {
  vi.mocked(getFoodCompositionData).mockResolvedValue(SEED as never);
  vi.mocked(getFoodCompositionCounts).mockResolvedValue({
    source_counts: {},
    classification_counts: {},
  } as never);
});

afterEach(() => {
  window.history.replaceState(null, "", "/");
  vi.clearAllMocks();
  pagination.page = 1;
});

describe("food composition server seed", () => {
  it("renders the seeded rows on first render, without fetching", async () => {
    const { container } = render(
      <FoodCompositionSection commonName="onion" initialData={SEED} />
    );
    // Synchronously present: this is what the server HTML contains.
    expect(chemicalLinks(container)).toContain("/chemical/quercetin");
    // Let effects settle; the counts still load, the rows do not refetch.
    await waitFor(() =>
      expect(vi.mocked(getFoodCompositionCounts)).toHaveBeenCalled()
    );
    expect(vi.mocked(getFoodCompositionData)).not.toHaveBeenCalled();
  });

  it("fetches when the URL asks for a search the seed does not cover", async () => {
    window.history.replaceState(null, "", "/food/onion?search=kaempferol");
    render(<FoodCompositionSection commonName="onion" initialData={SEED} />);
    await waitFor(() =>
      expect(vi.mocked(getFoodCompositionData)).toHaveBeenCalled()
    );
  });

  it("fetches when pagination is past page 1", async () => {
    pagination.page = 3;
    render(<FoodCompositionSection commonName="onion" initialData={SEED} />);
    await waitFor(() =>
      expect(vi.mocked(getFoodCompositionData)).toHaveBeenCalled()
    );
  });

  it("still fetches without a seed", async () => {
    render(<FoodCompositionSection commonName="onion" />);
    await waitFor(() => expect(screen.getAllByText("quercetin").length).toBeGreaterThan(0));
    expect(vi.mocked(getFoodCompositionData)).toHaveBeenCalledTimes(1);
  });
});

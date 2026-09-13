// A filter change on the literature table must send the reader back to
// page 1. Searching "quercetin" from page 3 of inflammation asked the
// server for page 3 of a 3-row result: "No evidence found", no pager,
// the matches unreachable without clearing the search.

import { render, waitFor } from "@testing-library/react";
import { useEffect } from "react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/utils/fetching", () => ({
  getDiseaseData: vi.fn(),
}));
vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import CorrelationTable from "@/components/entities/CorrelationTable";
import { PaginationsProvider, usePaginations } from "@/context/paginationsContext";
import { getDiseaseData } from "@/utils/fetching";

const TABLE_ID = "disease-correlation-table";

// Puts the table on page 3 the way the pager would, then hands control
// back to the test.
const OnPage = ({ page }: { page: number }) => {
  const { setTablePaginations } = usePaginations();
  useEffect(() => {
    setTablePaginations(TABLE_ID, page);
  }, [page, setTablePaginations]);
  return null;
};

// 2nd positional arg of getDiseaseData is the page requested.
const requestedPages = () =>
  vi.mocked(getDiseaseData).mock.calls.map((c) => c[1]);

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getDiseaseData).mockResolvedValue({
    data: { associations: [] },
    metadata: { total_rows: 0, total_pages: 0 },
  });
});

describe("literature table page reset", () => {
  it.each([
    ["search", { search: "quercetin" }],
    ["direction", { direction: "improves" as const }],
  ])("goes back to page 1 when the %s filter changes", async (_label, next) => {
    const { rerender } = render(
      <PaginationsProvider>
        <OnPage page={3} />
        <CorrelationTable commonName="inflammation" tableLocation="disease" />
      </PaginationsProvider>
    );
    await waitFor(() => expect(requestedPages()).toContain(3));

    // Keep the tree shape identical so the table updates in place
    // rather than remounting (a remount would start fresh on page 3).
    rerender(
      <PaginationsProvider>
        <OnPage page={3} />
        <CorrelationTable
          commonName="inflammation"
          tableLocation="disease"
          {...next}
        />
      </PaginationsProvider>
    );
    await waitFor(() => expect(requestedPages().at(-1)).toBe(1));
  });

  it("does not reset on mount, so a deep-linked page survives", async () => {
    render(
      <PaginationsProvider>
        <OnPage page={3} />
        <CorrelationTable
          commonName="inflammation"
          tableLocation="disease"
          search="quercetin"
        />
      </PaginationsProvider>
    );
    await waitFor(() => expect(requestedPages().length).toBeGreaterThan(0));
    expect(requestedPages().at(-1)).toBe(3);
  });
});

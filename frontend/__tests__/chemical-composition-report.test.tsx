// The chemical composition table delegates its <tbody> rows to
// ChemicalCompositionRow, so report-coverage.test.tsx can only allowlist it
// rather than prove the wiring. This test proves it: every rendered row must
// hand getRowProps a context that identifies the food it stands for.

import { render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

import ChemicalCompositionTable from "@/components/entities/chemical/ChemicalCompositionTable";
import { ChemicalCompositionRow } from "@/utils/chemicalComposition";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

const getRowProps = vi.fn(() => ({ "data-reportable": "true" }));

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ isSelectMode: true, getRowProps }),
}));
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => ({
    getTablePaginations: () => ({ currentPage: 1, rowsPerPage: 20 }),
    setTablePaginations: vi.fn(),
  }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn() }),
  usePathname: () => "/chemical/quercetin",
  useSearchParams: () => new URLSearchParams(),
}));

const rows: ChemicalCompositionRow[] = [
  {
    id: "f1",
    name: "onion",
    median_concentration: { value: 10, unit: "mg/100g" },
    evidence_count: 3,
    fdc_count: 3,
  },
];

describe("chemical composition report wiring", () => {
  it("passes a food-identifying report context for every row", () => {
    render(
      <ChemicalCompositionTable
        withConcentrations={rows}
        withoutConcentrations={[]}
        commonName="quercetin"
      chemicalId="c123"
      />
    );

    expect(getRowProps).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "food-composition-row",
        entityType: "chemical",
        entitySlug: "c123",
        chemicalName: "c123",
        foodId: "f1",
        foodName: "onion",
        dataPointCount: 3,
      })
    );
    // and the returned props actually reach the DOM row
    expect(screen.getAllByTestId("concentration-bar").length).toBeGreaterThan(0);
    expect(
      document.querySelectorAll('[data-reportable="true"]').length
    ).toBeGreaterThan(0);
  });
});

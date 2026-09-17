import { render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

// jsdom has no ResizeObserver; the section renders a responsive table that
// observes its wrapper. Without this the whole tree throws and the body
// renders empty — which makes negative assertions pass for the wrong reason.
beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/utils/fetching", () => ({
  getFoodInferredBioactivities: vi.fn(),
  getChemicalBioactivities: vi.fn().mockResolvedValue({ data: [] }),
}));

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}) }),
}));
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => ({
    getTablePaginations: () => ({ currentPage: 1, rowsPerPage: 20 }),
    setTablePaginations: vi.fn(),
  }),
}));

// next/navigation isn't mounted in the vitest jsdom env; Button uses
// useRouter for its built-in <Link> shortcut, so stub it.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import { getFoodInferredBioactivities } from "@/utils/fetching";

const row = (over: Record<string, unknown> = {}) => ({
  bioactivity: "anticancer",
  bioactivity_id: "e1",
  chemical: "hexadecanoic acid",
  chemical_id: "c1",
  median_concentration: null,
  conc_quality_flag: "ok",
  measurement_count: 5,
  n_curves: 3,
  efficacy_fraction: 0.5,
  conc_vs_ac50: "above",
  ...over,
});

const mount = async (rows: Record<string, unknown>[]) => {
  vi.mocked(getFoodInferredBioactivities).mockResolvedValue({
    data: rows,
    metadata: { total_rows: rows.length, total_pages: 1 },
  } as never);
  render(<FoodInferredBioactivitiesSection commonName="apple" />);
  await waitFor(() =>
    expect(vi.mocked(getFoodInferredBioactivities)).toHaveBeenCalled()
  );
};

// The upstream pipeline sets conc_quality_flag='suspect_high' when a
// chemical's concentration exceeds 10% of the food by mass — e.g.
// hexadecanoic acid at 75% of an apple. Those rows sort to the TOP of
// "Highest efficacy" precisely because the inflated concentration
// inflates the efficacy, so the warning has to be visible there.
describe("suspect_high concentration warning", () => {
  it("renders when the flag is set and a concentration IS shown", async () => {
    await mount([
      row({
        conc_quality_flag: "suspect_high",
        median_concentration: { value: 75000, unit: "mg/100g" },
      }),
    ]);
    await waitFor(() =>
      expect(
        screen.getAllByLabelText(/flagged as implausibly high/i).length
      ).toBeGreaterThan(0)
    );
  });

  // Regression: the warning used to live INSIDE the non-null branch of the
  // concentration cell, so rows with a null median_concentration rendered
  // an em-dash and no warning at all. That hid it on 468 of the flagged
  // rows — the ones where the user can least judge the number themselves,
  // because the flag comes from the efficacy row while the displayed value
  // comes from the composition row and the two frequently disagree.
  it("renders when the flag is set and the concentration is NULL", async () => {
    await mount([
      row({ conc_quality_flag: "suspect_high", median_concentration: null }),
    ]);
    await waitFor(() =>
      expect(
        screen.getAllByLabelText(/flagged as implausibly high/i).length
      ).toBeGreaterThan(0)
    );
  });

  it("does not render for an unflagged row", async () => {
    await mount([row({ conc_quality_flag: "ok" })]);
    await waitFor(() =>
      expect(vi.mocked(getFoodInferredBioactivities)).toHaveBeenCalled()
    );
    expect(
      screen.queryByLabelText(/flagged as implausibly high/i)
    ).toBeNull();
  });
});

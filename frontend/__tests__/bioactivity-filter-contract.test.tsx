// The measurements modal against the shared filter contract.
//
// contracts/bioactivity_filters.json holds measurements as the API's
// /bioactivity/measurements returns them, plus the modal total each
// (pair, page filters) case must show. The API permutation tests check
// the "N assays" button against the same numbers on Postgres, so passing
// both suites means button == modal for every case.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});
afterEach(cleanup);

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import { measurementMatches } from "@/components/entities/bioactivity/measurementFilters";

interface ContractFilters {
  evidence: string[];
  unit: string[];
  source: string;
}
interface Contract {
  pairs: { api_measurements: Record<string, unknown>[] }[];
  modal_filters: ContractFilters[];
  modal_cases: [number, number, number][];
}

const CONTRACT: Contract = JSON.parse(
  readFileSync(resolve(__dirname, "../../contracts/bioactivity_filters.json"), "utf8")
);

const cases = CONTRACT.modal_cases.map(([pair, filt, expected]) => ({
  rows: CONTRACT.pairs[pair].api_measurements,
  f: CONTRACT.modal_filters[filt],
  expected,
}));

const mount = (rows: Record<string, unknown>[], f: ContractFilters) =>
  render(
    <BioactivityMeasurementsModal
      isOpen
      onClose={() => {}}
      headLabel="chem"
      tailLabel="bio"
      initialMeasurements={rows as never}
      initialSourceKind={f.source}
      initialEvidenceTypes={f.evidence}
      initialUnits={f.unit}
    />
  );

const shownCount = async (): Promise<number> => {
  let n = NaN;
  await waitFor(() => {
    const el = document.body.querySelector("[data-filtered-count]");
    n = Number(el?.getAttribute("data-filtered-count"));
    expect(Number.isNaN(n)).toBe(false);
  });
  return n;
};

describe("modal filter rule vs the shared contract", () => {
  it(`matches every expected modal total (${cases.length} cases)`, () => {
    const wrong = cases.filter(
      ({ rows, f, expected }) =>
        rows.filter((r) =>
          measurementMatches(r, {
            outcome: "all",
            source: f.source,
            evidence: f.evidence,
            units: f.unit,
            search: "",
          })
        ).length !== expected
    );
    expect(wrong).toEqual([]);
  });
});

describe("modal opens on the page filters", () => {
  const sampled = cases.filter((c, i) => i % 37 === 0 && c.expected > 0);

  it.each(sampled.map((c, i) => [i, c] as const))(
    "case %i shows the expected total",
    async (_i, { rows, f, expected }) => {
      mount(rows, f);
      expect(await shownCount()).toBe(expected);
    }
  );

  it("offers a Unit filter with the page's units selected", async () => {
    const c = cases.find((x) => x.f.unit.length > 0 && x.expected > 0);
    expect(c).toBeTruthy();
    mount(c!.rows, c!.f);
    await shownCount();
    expect(screen.getByText("Unit")).toBeTruthy();
  });

  it("Clear filters shows every assay of the pair", async () => {
    const c = cases.find(
      (x) => x.expected > 0 && x.expected < x.rows.length && x.f.evidence.length > 0
    );
    expect(c).toBeTruthy();
    mount(c!.rows, c!.f);
    expect(await shownCount()).toBe(c!.expected);
    const clear = screen
      .getAllByRole("button")
      .find((b) => /clear filters|reset/i.test(b.textContent ?? ""));
    expect(clear).toBeTruthy();
    fireEvent.click(clear!);
    await waitFor(async () => expect(await shownCount()).toBe(c!.rows.length));
  });
});

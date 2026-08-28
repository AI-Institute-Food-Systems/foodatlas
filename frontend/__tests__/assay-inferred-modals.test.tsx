// Target and Assays cells on the assay-inferred table.
//
// Both used to render two or three items inline plus a "+N" tooltip. A
// tooltip can't be reached on touch and can't be copied out of, and the
// visible slice was arbitrary rather than the important ones — so the
// cell now states the count and the modal holds the full list.
//
// The count sources differ and that matters: targets count their own
// array, but assays count `n_assays`, because the stored assay list is
// capped upstream (ASSAY_CAP = 25) while the count is not. A row backed
// by 300 assays must say 300, and the modal must admit it is showing 25.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
// Modal's close Button reaches for the app router.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import { PaginationsProvider } from "@/context/paginationsContext";

const row = (over: Record<string, unknown> = {}) => ({
  disease_name: "melanoma",
  disease_foodatlas_id: "d1",
  chemical_name: "quercetin",
  chemical_foodatlas_id: "c1",
  n_assays: 3,
  n_active_measurements: 12,
  relationships: [],
  literature_directions: [],
  targets: [
    { id: "NCBIGene: 7157", label: "cellular tumor antigen p53" },
    { id: "UniProt: Q16236", label: "nuclear factor erythroid 2-related factor 2" },
  ],
  assays: ["AID 1234", "AID 5678", "CHEMBL999"],
  ...over,
});

// The real provider rather than a stub: the table pages in memory, so a
// stubbed currentPage would make every paging assertion vacuous.
const mount = async (
  rows: Record<string, unknown>[],
  expectFirst = "melanoma"
) => {
  render(
    <PaginationsProvider>
      <AssayInferredAssociationsTable
        commonName="quercetin"
        peer="disease"
        fetcher={async () => ({
          data: rows as never,
          metadata: { row_count: rows.length },
        })}
      />
    </PaginationsProvider>
  );
  await waitFor(() =>
    expect(screen.getAllByText(expectFirst).length).toBeGreaterThan(0)
  );
};

beforeEach(() => vi.clearAllMocks());

const headerTexts = () =>
  Array.from(document.querySelectorAll("th")).map((th) => th.textContent);

describe("column headers", () => {
  it("names the source-assay column Assays", async () => {
    // It was "Evidence", which read as publications on a page whose
    // other table has a Publications column.
    await mount([row()]);
    // Scoped to <th>: the mobile card list renders an "Assays" label of
    // its own, so a bare getByText matches two nodes.
    expect(headerTexts()).toContain("Assays");
    expect(headerTexts()).not.toContain("Evidence");
  });
});

describe("the assay count", () => {
  it("appears exactly once, on the Assays button", async () => {
    // Three printings of one number used to sit side by side: an Active
    // column (n_active_measurements == n_assays for 347,632/347,632 rows
    // of mv_chemical_disease_bioactivity and 408,118/408,118 of
    // mv_disease_bioactivity — the materializer counts distinct assay ids
    // and distinct measurement ids over evidence with one measurement per
    // assay, so they cannot diverge), a "# Assays" count column, and the
    // button that already says how many.
    await mount([row({ n_assays: 7, n_active_measurements: 7 })]);
    expect(headerTexts()).not.toContain("Active");
    expect(headerTexts()).not.toContain("# Assays");
    expect(screen.getAllByText("See 7 assays").length).toBeGreaterThan(0);
    // And the bare number is gone from the row entirely.
    expect(screen.queryByText("7")).not.toBeInTheDocument();
  });
});

describe("targets", () => {
  it("states the count rather than previewing chips", async () => {
    await mount([row()]);
    expect(screen.getAllByText("See 2 targets").length).toBeGreaterThan(0);
  });

  it("opens a modal with every target's label and id", async () => {
    await mount([row()]);
    fireEvent.click(screen.getAllByText("See 2 targets")[0]);

    await waitFor(() =>
      expect(screen.getByText("Protein targets")).toBeInTheDocument()
    );
    expect(
      screen.getByText("cellular tumor antigen p53")
    ).toBeInTheDocument();
    // The long label is truncated in the cell but must be whole here.
    expect(
      screen.getByText("nuclear factor erythroid 2-related factor 2")
    ).toBeInTheDocument();
    expect(screen.getByText("NCBIGene: 7157")).toBeInTheDocument();
  });

  it("renders no button for a row with no targets", async () => {
    await mount([row({ targets: [] })]);
    expect(screen.queryByText(/See \d+ targets?/)).not.toBeInTheDocument();
  });
});

describe("assays", () => {
  it("counts n_assays, not the capped stored list", async () => {
    await mount([row({ n_assays: 300 })]);
    expect(screen.getAllByText("See 300 assays").length).toBeGreaterThan(0);
  });

  it("admits in the modal when the stored list is capped", async () => {
    await mount([row({ n_assays: 300 })]);
    fireEvent.click(screen.getAllByText("See 300 assays")[0]);

    await waitFor(() =>
      expect(screen.getByText("Source assays")).toBeInTheDocument()
    );
    expect(screen.getByText(/Showing 3 of 300/)).toBeInTheDocument();
  });

  it("says nothing about capping when the list is complete", async () => {
    await mount([row()]);
    fireEvent.click(screen.getAllByText("See 3 assays")[0]);

    await waitFor(() =>
      expect(screen.getByText("Source assays")).toBeInTheDocument()
    );
    expect(screen.queryByText(/Showing/)).not.toBeInTheDocument();
    expect(screen.getByText("AID 1234")).toBeInTheDocument();
    expect(screen.getByText("CHEMBL999")).toBeInTheDocument();
  });
});

describe("modal identity", () => {
  it("opens the clicked row's list, not the first row's", async () => {
    // Keying the open modal off a boolean plus "current row" is the easy
    // bug here; both modals are keyed by peer id instead.
    await mount([
      row(),
      row({
        disease_name: "psoriasis",
        disease_foodatlas_id: "d2",
        targets: [{ id: "NCBIGene: 3569", label: "interleukin-6" }],
        assays: ["AID 4242"],
        n_assays: 1,
      }),
    ]);

    fireEvent.click(screen.getAllByText("See 1 target")[0]);
    await waitFor(() =>
      expect(screen.getByText("Protein targets")).toBeInTheDocument()
    );
    expect(screen.getByText("interleukin-6")).toBeInTheDocument();
    expect(
      screen.queryByText("cellular tumor antigen p53")
    ).not.toBeInTheDocument();
  });
});

describe("pagination", () => {
  // It used to render the first 50 rows and a "Show all" button, alone
  // among the tables on these pages — the literature table directly above
  // it paginates, and so does every bioactivity table.
  const manyRows = (n: number) =>
    Array.from({ length: n }, (_, i) =>
      row({
        disease_name: `disease-${i}`,
        disease_foodatlas_id: `d${i}`,
      })
    );

  it("shows 20 rows a page and pages through the rest", async () => {
    await mount(manyRows(25), "disease-0");
    // Desktop table and mobile cards both render in jsdom, so count rows
    // in the <tbody> rather than by text.
    expect(document.querySelectorAll("tbody tr")).toHaveLength(20);
    expect(screen.queryAllByText("disease-24")).toHaveLength(0);

    fireEvent.click(screen.getByLabelText(/next page/i));
    await waitFor(() =>
      expect(screen.getAllByText("disease-24").length).toBeGreaterThan(0)
    );
    expect(document.querySelectorAll("tbody tr")).toHaveLength(5);
  });

  it("offers no paginator when everything fits on one page", async () => {
    await mount(manyRows(3), "disease-0");
    expect(screen.queryByLabelText(/next page/i)).toBeNull();
  });

  it("has no show-all escape hatch left", async () => {
    await mount(manyRows(25), "disease-0");
    expect(screen.queryByText(/show all/i)).toBeNull();
  });
});

// The two tables on the disease page's Chemicals tab (and the chemical
// page's Diseases tab — same components) sort by their headers.
//
// The literature table is server-paginated, so a header click has to
// become sort_by/sort_dir on the request; the assay table holds every
// row in memory and sorts there. Both were unsortable.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import CorrelationTable from "@/components/entities/CorrelationTable";
import { PaginationsProvider } from "@/context/paginationsContext";
import { getDiseaseData } from "@/utils/fetching";

const literatureRow = (name: string, n: number) => ({
  id: `c-${name}`,
  name,
  relationship_ids: ["r4"],
  source_chemical_name: name,
  source_chemical_foodatlas_id: `c-${name}`,
  sources: [],
  improves_evidences: Array.from({ length: n }, (_, i) => ({
    pmid: { id: `${name}-${i}`, url: "https://example.test" },
  })),
  worsens_evidences: null,
  ambiguity_siblings: [],
});

const clickHeader = (name: RegExp) =>
  fireEvent.click(screen.getByRole("button", { name }));

const lastSort = () =>
  vi.mocked(getDiseaseData).mock.calls.at(-1)?.[5];

beforeEach(() => vi.clearAllMocks());

describe("literature table (CorrelationTable)", () => {
  const mount = async () => {
    vi.mocked(getDiseaseData).mockResolvedValue({
      data: { associations: [literatureRow("caffeine", 3), literatureRow("aspirin", 9)] },
      metadata: { total_rows: 2, total_pages: 1 },
    });
    render(
      <PaginationsProvider>
        <CorrelationTable commonName="diabetes" tableLocation="disease" />
      </PaginationsProvider>
    );
    await waitFor(() =>
      expect(screen.getAllByText("caffeine").length).toBeGreaterThan(0)
    );
  };

  it("asks the server for most publications first by default", async () => {
    await mount();
    expect(lastSort()).toEqual({ by: "evidence_count", dir: "desc" });
    expect(screen.getByRole("columnheader", { name: /publications/i })).toHaveAttribute(
      "aria-sort",
      "descending"
    );
  });

  it("sorts by chemical name, A→Z first, on a header click", async () => {
    await mount();
    clickHeader(/^chemical$/i);
    await waitFor(() =>
      expect(lastSort()).toEqual({ by: "name", dir: "asc" })
    );
    clickHeader(/^chemical$/i);
    await waitFor(() =>
      expect(lastSort()).toEqual({ by: "name", dir: "desc" })
    );
  });

  it("flips publications to fewest-first on a second click", async () => {
    await mount();
    clickHeader(/^publications$/i);
    await waitFor(() =>
      expect(lastSort()).toEqual({ by: "evidence_count", dir: "asc" })
    );
  });

  it("does not offer Direction as a sort — it is the sidebar's filter", async () => {
    await mount();
    expect(screen.queryByRole("button", { name: /^direction$/i })).toBeNull();
  });
});

describe("assay table (AssayInferredAssociationsTable)", () => {
  const assay = (chemical: string, n_assays: number) => ({
    chemical_name: chemical,
    chemical_foodatlas_id: `c-${chemical}`,
    disease_name: "diabetes",
    disease_foodatlas_id: "d1",
    n_assays,
    n_active_measurements: n_assays,
    relationships: ["therapeutic"],
    target_genes: [],
    targets: [],
    assays: [],
    bioactivities: ["anticancer"],
  });
  // Alphabetical order and assay-count order disagree on purpose, so a
  // sort that silently fell back to the other would not pass.
  const ROWS = [assay("caffeine", 9), assay("aspirin", 3), assay("berberine", 5)];

  const names = () =>
    Array.from(document.querySelectorAll("tbody tr")).map((tr) =>
      (tr.querySelector("td")?.textContent ?? "").trim()
    );

  const mount = async () => {
    render(
      <PaginationsProvider>
        <AssayInferredAssociationsTable
          commonName="diabetes"
          peer="chemical"
          fetcher={async () => ({ data: ROWS as never, metadata: { row_count: 3 } })}
        />
      </PaginationsProvider>
    );
    await waitFor(() =>
      expect(screen.getAllByText("caffeine").length).toBeGreaterThan(0)
    );
  };

  it("shows most assays first by default", async () => {
    await mount();
    expect(names()).toEqual(["caffeine", "berberine", "aspirin"]);
  });

  it("sorts by chemical A→Z, then Z→A, on header clicks", async () => {
    await mount();
    clickHeader(/^chemical$/i);
    await waitFor(() =>
      expect(names()).toEqual(["aspirin", "berberine", "caffeine"])
    );
    clickHeader(/^chemical$/i);
    await waitFor(() =>
      expect(names()).toEqual(["caffeine", "berberine", "aspirin"])
    );
  });

  it("flips assays to fewest-first on a second click", async () => {
    await mount();
    clickHeader(/^assays$/i);
    await waitFor(() =>
      expect(names()).toEqual(["aspirin", "berberine", "caffeine"])
    );
  });

  it("leaves the list-valued columns unsortable", async () => {
    await mount();
    for (const label of [/^signal$/i, /^activities$/i, /^target$/i]) {
      expect(screen.queryByRole("button", { name: label })).toBeNull();
    }
  });
});

describe("literature table column widths", () => {
  // Direction is one badge and one word; giving it an equal third of the
  // table (the previous colgroup) pushed the chemical names into a
  // narrower column than a single word got.
  const widths = () =>
    Array.from(document.querySelectorAll("colgroup col")).map((c) =>
      (c.getAttribute("class") ?? "").match(/w-\[(\d+)%\]/)?.[1]
    );

  it("gives the name column the most room and Direction the least", async () => {
    vi.mocked(getDiseaseData).mockResolvedValue({
      data: { associations: [literatureRow("caffeine", 3)] },
      metadata: { total_rows: 1, total_pages: 1 },
    });
    render(
      <PaginationsProvider>
        <CorrelationTable commonName="diabetes" tableLocation="disease" />
      </PaginationsProvider>
    );
    await waitFor(() =>
      expect(screen.getAllByText("caffeine").length).toBeGreaterThan(0)
    );
    const [direction, name, publications] = widths().map(Number);
    expect(direction).toBeLessThan(publications);
    expect(publications).toBeLessThan(name);
    expect(direction + name + publications).toBe(100);
  });
});

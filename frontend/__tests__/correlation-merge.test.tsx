// The two things the Diseases/Chemicals tab merge had to get right.
//
// 1. The source-chemical column. It looks redundant on a chemical page —
//    every row naming the chemical you're already looking at — and the
//    obvious fix is to delete it. But `mv_chemical_disease_correlation`
//    emits one row per (chemical, source_chemical, disease, rel) so a
//    ChEBI class page can surface its descendants' edges: 82% of rows in
//    the local snapshot have source != chemical, and "polyphenol" rolls
//    up 124 distinct source chemicals. Deleting the column would blank
//    that attribution on ~19% of pages. So it is hidden per PAGE, not
//    removed — present only when some row actually attributes elsewhere.
//
// 2. The direction. Improves/Worsens used to be two separately fetched
//    tables under two headings; they are now one table whose rows carry
//    `relationship_id` (r4 improves, r3 worsens). A row that renders the
//    wrong direction inverts the claim, so it is pinned here.

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
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => ({ getTablePaginations: () => ({ currentPage: 1 }) }),
}));
vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}) }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import CorrelationTable from "@/components/entities/CorrelationTable";
import {
  hasDistinctSource,
  rowDirection,
} from "@/components/entities/shared/CorrelationRow";
import { getDiseaseData } from "@/utils/fetching";

const row = (over: Record<string, unknown> = {}) => ({
  id: "d1",
  name: "inflammation",
  relationship_id: "r4",
  source_chemical_name: "caffeine",
  source_chemical_foodatlas_id: "e1",
  sources: [],
  evidences: [{ pmid: { id: "123", url: "https://example.org/123" } }],
  ambiguity_siblings: [],
  ...over,
});

const mount = async (rows: Record<string, unknown>[], commonName: string) => {
  vi.mocked(getDiseaseData).mockResolvedValue({
    data: { associations: rows },
    metadata: { total_rows: rows.length, total_pages: 1 },
  });
  render(
    <CorrelationTable commonName={commonName} tableLocation="chemical" />
  );
  await waitFor(() =>
    expect(screen.getAllByText(/inflammation|diabetes/i).length).toBeGreaterThan(0)
  );
};

beforeEach(() => vi.clearAllMocks());

describe("source-chemical column", () => {
  it("is hidden on a leaf page, where every row names the page itself", async () => {
    await mount([row(), row({ id: "d2", name: "diabetes" })], "caffeine");
    expect(screen.queryByText("Via Chemical")).not.toBeInTheDocument();
  });

  it("ignores case when deciding a row names the page itself", async () => {
    // The view stores lowercased names; the slug can arrive title-cased.
    await mount([row({ source_chemical_name: "Caffeine" })], "caffeine");
    expect(screen.queryByText("Via Chemical")).not.toBeInTheDocument();
  });

  it("appears on a class page and names the descendant", async () => {
    await mount(
      [
        row({ source_chemical_name: "trans-resveratrol" }),
        row({ id: "d2", name: "diabetes", source_chemical_name: "rotenone" }),
      ],
      "polyphenol"
    );
    expect(screen.getByText("Via Chemical")).toBeInTheDocument();
    expect(screen.getAllByText("trans-resveratrol").length).toBeGreaterThan(0);
    expect(screen.getAllByText("rotenone").length).toBeGreaterThan(0);
  });

  it("appears when only some rows attribute elsewhere", async () => {
    // 49 pages in the snapshot are mixed. The column is a property of the
    // page, so one differing row is enough to earn it.
    await mount(
      [
        row({ source_chemical_name: "vitamin c" }),
        row({ id: "d2", name: "diabetes", source_chemical_name: "l-ascorbic acid" }),
      ],
      "vitamin c"
    );
    expect(screen.getByText("Via Chemical")).toBeInTheDocument();
  });
});

describe("direction", () => {
  it("renders both directions in one table, from relationship_id", async () => {
    await mount(
      [row({ relationship_id: "r4" }), row({ id: "d2", name: "diabetes", relationship_id: "r3" })],
      "caffeine"
    );
    expect(screen.getAllByText("Improves").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Worsens").length).toBeGreaterThan(0);
  });

  it("maps r3 to worsens and r4 to improves", () => {
    expect(rowDirection({ relationship_id: "r3" })).toBe("negative");
    expect(rowDirection({ relationship_id: "r4" })).toBe("positive");
  });
});

describe("hasDistinctSource", () => {
  it("is false when the source is missing entirely", () => {
    // Older API responses predate the column; a row without it is the
    // page's own chemical by definition, not an unnamed third one.
    expect(hasDistinctSource({}, "caffeine")).toBe(false);
    expect(hasDistinctSource({ source_chemical_name: "" }, "caffeine")).toBe(
      false
    );
  });

  it("is true only for a genuinely different chemical", () => {
    expect(
      hasDistinctSource({ source_chemical_name: "rotenone" }, "polyphenol")
    ).toBe(true);
    expect(
      hasDistinctSource({ source_chemical_name: "caffeine" }, "caffeine")
    ).toBe(false);
  });
});

describe("publications", () => {
  it("names the real total instead of previewing three ids", async () => {
    // The old cell rendered evidences[0..2] inline plus an "N more..."
    // chip, so a row with 200 PMIDs advertised "197 more" next to three
    // arbitrary ones. The count is the only part a reader can act on.
    const evidences = Array.from({ length: 200 }, (_, i) => ({
      pmid: { id: String(i), url: `https://example.org/${i}` },
    }));
    await mount([row({ evidences })], "caffeine");
    expect(screen.getAllByText("See 200 publications").length).toBeGreaterThan(0);
    expect(screen.queryByText(/more\.\.\./)).not.toBeInTheDocument();
  });

  it("singularises a lone publication", async () => {
    await mount([row()], "caffeine");
    expect(screen.getAllByText("See 1 publication").length).toBeGreaterThan(0);
  });

  it("opens a modal listing every publication, not just the visible few", async () => {
    const evidences = Array.from({ length: 5 }, (_, i) => ({
      pmid: { id: `pmid-${i}`, url: `https://example.org/${i}` },
    }));
    await mount([row({ evidences })], "caffeine");

    fireEvent.click(screen.getAllByText("See 5 publications")[0]);

    await waitFor(() =>
      expect(screen.getByText("Publications (PMIDs)")).toBeInTheDocument()
    );
    // Every id, including the ones the old cell hid behind "+N".
    for (let i = 0; i < 5; i++) {
      expect(screen.getByText(`pmid-${i}`)).toBeInTheDocument();
    }
  });

  it("disables the button when a row somehow has no evidence", async () => {
    await mount([row({ evidences: [] })], "caffeine");
    const buttons = screen.getAllByRole("button", {
      name: /See 0 publications/,
    });
    expect(buttons[0]).toBeDisabled();
  });
});

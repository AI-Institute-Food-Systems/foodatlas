// Health tab: every Signal x Activity combination on the assay-inferred
// table, and every Direction on the literature table. Buttons, row
// filtering, facet counts and modals must agree with an oracle written
// from the rule: one assay must match every active filter.

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({
  getDiseaseData: vi.fn(),
}));

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import CorrelationEvidenceModal from "@/components/entities/CorrelationEvidenceModal";
import CorrelationTable from "@/components/entities/CorrelationTable";
import { assayItemCount } from "@/components/entities/shared/AssayFilteredModal";
import {
  activityFacet,
  rowMatches,
  signalFacet,
  type AssayFilters,
} from "@/components/entities/shared/assayGroups";
import { publicationsFor } from "@/components/entities/shared/CorrelationRow";
import { PaginationsProvider } from "@/context/paginationsContext";
import type { AssayGroup, AssayInferredAssociation } from "@/types/AssayInferred";
import { getDiseaseData } from "@/utils/fetching";

const T = "therapeutic";
const M = "marker/mechanism";
const SIGNALS = [T, M];
const ACTIVITIES = ["anticancer", "antiviral", "anti-inflammatory"];

const group = (
  activities: string[],
  relationships: string[],
  n: number,
  genes: string[]
): AssayGroup => ({
  activities,
  relationships,
  n_assays: n,
  assays: Array.from({ length: Math.min(n, 25) }, (_, i) => `AID ${activities.join("")}${relationships.join("")}${i}`),
  target_genes: genes,
  targets: genes.map((g) => ({ id: g, label: g })),
});

const makeRow = (name: string, groups: AssayGroup[]): AssayInferredAssociation => {
  const n = groups.reduce((s, g) => s + g.n_assays, 0);
  const genes = Array.from(new Set(groups.flatMap((g) => g.target_genes)));
  return {
    chemical_name: "quercetin",
    chemical_foodatlas_id: "c1",
    disease_name: name,
    disease_foodatlas_id: `d-${name}`,
    n_assays: n,
    n_active_measurements: n,
    relationships: Array.from(new Set(groups.flatMap((g) => g.relationships))),
    target_genes: genes,
    targets: genes.map((g) => ({ id: g, label: g })),
    assays: groups.flatMap((g) => g.assays).slice(0, 25),
    bioactivities: Array.from(new Set(groups.flatMap((g) => g.activities))),
    literature_directions: [],
    assay_groups: groups,
  };
};

// "split" has therapeutic assays for anticancer and marker assays for
// antiviral, but no assay that is both therapeutic and antiviral — the
// case a per-dimension row filter got wrong.
const ROWS = [
  makeRow("split", [
    group(["anticancer"], [T], 4, ["G1"]),
    group(["antiviral"], [M], 2, ["G2"]),
  ]),
  makeRow("overlap", [
    group(["anticancer", "antiviral"], [T, M], 3, ["G1", "G3"]),
    group([], [T], 30, ["G4"]),
  ]),
  makeRow("inflamed", [group(["anti-inflammatory"], [M], 1, ["G5"])]),
];

const subsets = <V,>(values: V[]): V[][] =>
  Array.from({ length: 1 << values.length }, (_, mask) =>
    values.filter((_, i) => mask & (1 << i))
  );
const CASES: AssayFilters[] = subsets(SIGNALS).flatMap((signals) =>
  subsets(ACTIVITIES).map((activities) => ({ signals, activities }))
);

// --- oracle, from the rule, not from the helpers -----------------------
const hits = (g: AssayGroup, f: AssayFilters) =>
  (f.signals.length === 0 || g.relationships.some((r) => f.signals.includes(r))) &&
  (f.activities.length === 0 || g.activities.some((a) => f.activities.includes(a)));
const open = (f: AssayFilters) => f.signals.length === 0 && f.activities.length === 0;
const oracle = (row: AssayInferredAssociation, f: AssayFilters) => {
  const gs = (row.assay_groups ?? []).filter((g) => hits(g, f));
  return {
    listed: open(f) || gs.length > 0,
    assays: open(f) ? row.n_assays : gs.reduce((n, g) => n + g.n_assays, 0),
    targets: open(f)
      ? row.targets.length
      : new Set(gs.flatMap((g) => g.target_genes)).size,
    activities: open(f)
      ? (row.bioactivities ?? []).length
      : new Set(
          gs
            .flatMap((g) => g.activities)
            .filter((a) => f.activities.length === 0 || f.activities.includes(a))
        ).size,
  };
};

describe(`assay groups vs oracle (${CASES.length} filter cases)`, () => {
  it("row filter and every button count", () => {
    for (const f of CASES) {
      for (const row of ROWS) {
        const want = oracle(row, f);
        const label = `${row.disease_name} ${JSON.stringify(f)}`;
        expect(rowMatches(row, f), label).toBe(want.listed);
        expect(assayItemCount("assays", row, f), label).toBe(want.assays);
        expect(assayItemCount("targets", row, f), label).toBe(want.targets);
        expect(assayItemCount("activities", row, f), label).toBe(want.activities);
      }
    }
  });

  it("facet counts equal the rows that selection lists", () => {
    for (const f of CASES) {
      const sig = signalFacet(ROWS, f, SIGNALS);
      for (const s of SIGNALS) {
        const listed = ROWS.filter((r) => oracle(r, { ...f, signals: [s] }).listed);
        expect(sig[s], `${s} ${JSON.stringify(f)}`).toBe(listed.length);
      }
      const act = activityFacet(ROWS, f, ACTIVITIES);
      for (const a of ACTIVITIES) {
        const listed = ROWS.filter((r) => oracle(r, { ...f, activities: [a] }).listed);
        expect(act[a], `${a} ${JSON.stringify(f)}`).toBe(listed.length);
      }
    }
  });
});

const mountAssays = (f: AssayFilters) =>
  render(
    <PaginationsProvider>
      <AssayInferredAssociationsTable
        commonName="quercetin"
        peer="disease"
        externalSignals={f.signals}
        externalActivities={f.activities}
        fetcher={async () => ({ data: ROWS, metadata: { row_count: ROWS.length } })}
      />
    </PaginationsProvider>
  );

const shownCount = async (): Promise<number> => {
  let n = NaN;
  await waitFor(() => {
    n = Number(
      document.body.querySelector("[data-filtered-count]")?.getAttribute("data-filtered-count")
    );
    expect(Number.isNaN(n)).toBe(false);
  });
  return n;
};

const NOUN = { assays: ["assay", "assays"], targets: ["target", "targets"], activities: ["activity", "activities"] } as const;

describe("assay table → modals open on the page filters", () => {
  const sampled = CASES.filter((_, i) => i % 3 === 0);
  it.each(sampled.map((f) => [JSON.stringify(f), f] as const))(
    "filters %s",
    async (_label, f) => {
      mountAssays(f);
      const listed = ROWS.filter((r) => oracle(r, f).listed);
      if (listed.length === 0) {
        expect(await screen.findByText(/match these filters/i)).toBeTruthy();
        return;
      }
      await screen.findAllByText(listed[0].disease_name);
      const row = listed[0];
      for (const kind of ["assays", "targets", "activities"] as const) {
        const n = oracle(row, f)[kind];
        if (n === 0) continue;
        const [one, many] = NOUN[kind];
        const name = new RegExp(`^See ${n.toLocaleString()} ${n === 1 ? one : many}$`, "i");
        fireEvent.click(screen.getAllByRole("button", { name })[0]);
        expect(await shownCount(), `${kind}`).toBe(n);
        const dialog = screen.getByRole("dialog");
        expect(within(dialog).getByText("Signal")).toBeTruthy();
        fireEvent.keyDown(dialog, { key: "Escape" });
        await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
      }
    }
  );
});

describe("literature table → publications modal opens on the page Direction", () => {
  const paper = (id: number) => ({ pmid: { id, url: `https://pubmed.test/${id}` } });
  const improves = [paper(1), paper(2), paper(3)];
  const worsens = [paper(3), paper(4)];
  const row = {
    improves_evidences: improves,
    worsens_evidences: worsens,
    evidences: [paper(1), paper(2), paper(3), paper(4)],
  };

  it.each([
    ["all", 4],
    ["positive", 3],
    ["negative", 2],
  ] as const)("direction %s → %i publications", async (direction, want) => {
    expect(publicationsFor(row as never, direction).length).toBe(want);
    render(
      <CorrelationEvidenceModal
        entityType="chemical"
        chemicalName="quercetin"
        diseaseName="asthma"
        improvesEvidences={improves as never}
        worsensEvidences={worsens as never}
        initialDirection={direction}
        isOpen
        onClose={() => {}}
      />
    );
    expect(await shownCount()).toBe(want);
  });
});

describe("literature table → publications modal, per page Direction", () => {
  const paper = (id: string) => ({ pmid: { id, url: `https://pubmed.test/${id}` } });
  const mixed = {
    id: "d1",
    name: "asthma",
    relationship_ids: ["r3", "r4"],
    source_chemical_name: "quercetin",
    source_chemical_foodatlas_id: "c1",
    sources: [],
    improves_evidences: [paper("1"), paper("2"), paper("3")],
    worsens_evidences: [paper("3"), paper("4")],
    evidences: [paper("1"), paper("2"), paper("3"), paper("4")],
    ambiguity_siblings: [],
  };

  it.each([
    ["all", 4],
    ["positive", 3],
    ["negative", 2],
  ] as const)("direction %s: button = modal total = %i", async (direction, want) => {
    vi.mocked(getDiseaseData).mockResolvedValue({
      data: { associations: [mixed] },
      metadata: { total_rows: 1, total_pages: 1 },
    } as never);
    render(
      <PaginationsProvider>
        <CorrelationTable
          commonName="quercetin"
          tableLocation="chemical"
          direction={direction}
        />
      </PaginationsProvider>
    );
    const label = `See ${want} publication${want === 1 ? "" : "s"}`;
    fireEvent.click((await screen.findAllByText(label))[0]);
    expect(await shownCount()).toBe(want);
  });
});

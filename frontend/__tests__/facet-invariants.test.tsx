// Every filtering surface, held to the same three invariants.
//
// Two bugs in a row came from the same promise being broken in different
// places: the number beside a filter option is what you get if you click
// it.
//
//   - composition Source: the pager said 309, the table showed nothing
//   - assays Outcome: the table shrank, the Evidence counts sat frozen
//
// Each was fixed with a test aimed at that one surface, which does
// nothing for the next surface to break. So the assertions live in
// support/facetInvariants.tsx and every surface is registered here.
//
// The registry is enforced: `test_every_filtering_surface_is_registered`
// scans components/ for anything rendering <FilterGroup and fails if it
// is neither registered below nor in EXCLUDED with a reason. Adding a
// filter panel without covering it breaks the build.

import { cleanup, render, screen, waitFor } from "@testing-library/react";
import fs from "node:fs";
import path from "node:path";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import {
  assertFacetCountsMatchRows,
  assertFacetsRespondToOtherFilters,
  assertOptionCountsHoldUnderSwitches,
  assertNoEmptyTableUnderPositiveCount,
  assertOptionSetStableAcrossFilters,
  assertOptionsAlphabetical,
  countTableRows,
  type FacetSurface,
} from "./support/facetInvariants";

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
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
// Stable identities: FoodCompositionSection lists setTablePaginations in
// its fetch effect's deps, so a factory returning a fresh vi.fn() per
// render re-runs the effect forever.
const pagination = vi.hoisted(() => ({
  getTablePaginations: () => ({ currentPage: 1, rowsPerPage: 20 }),
  setTablePaginations: vi.fn(),
}));
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => pagination,
}));

vi.mock("@/utils/fetching", () => ({
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
  getChemicalCompositionEvidence: vi.fn().mockResolvedValue([]),
  getFoodCompositionData: vi.fn(),
  getFoodCompositionCounts: vi.fn(),
  getCorrelationDirectionCounts: vi
    .fn()
    .mockResolvedValue({ improves: 2, worsens: 1, both: 3 }),
  getDiseaseData: vi.fn(),
  getChemicalDiseaseAssociations: vi.fn(),
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import ChemicalCompositionTable from "@/components/entities/chemical/ChemicalCompositionTable";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";
import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import {
  getChemicalDiseaseAssociations,
  getDiseaseData,
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

afterEach(cleanup);

// --- surfaces ---------------------------------------------------------

const measurement = (
  outcome: string,
  evidence_type: string,
  evidence_source: string,
  assay: string
) => ({
  assay,
  endpoint: "IC50",
  outcome,
  evidence_type,
  evidence_source,
  unit: "uM",
  value: 1,
});

const MEASUREMENTS = [
  measurement("active", "in vitro", "experimental", "a1"),
  measurement("active", "in vitro", "experimental", "a2"),
  measurement("active", "molecular-level", "experimental", "a3"),
  measurement("inactive", "molecular-level", "predicted", "a4"),
  measurement("inconclusive", "in vitro", "experimental", "a5"),
  measurement("unspecified", "adme/tox", "predicted", "a6"),
];

const evidence = (
  source: "FoodAtlas" | "FDC" | "PTFI",
  chemical: string,
  id: string
) => ({
  premise: "",
  extraction: [
    {
      attestation_id: id,
      extracted_chemical_name: chemical,
      extracted_food_name: "pepper",
      extracted_concentration: "1 mg",
      method: "HPLC",
    },
  ],
  reference: {
    id,
    source_name: source,
    display_name: `${source} ${id}`,
    url: "https://example.test",
  },
});

const EVIDENCES = [
  evidence("FoodAtlas", "quercetin", "e1"),
  evidence("FoodAtlas", "kaempferol", "e2"),
  evidence("FDC", "quercetin", "e3"),
  evidence("PTFI", "capsaicin", "e4"),
];

// A faithful stand-in for /food/composition and its counts endpoint.
//
// FoodCompositionSection is server-driven, so this mock necessarily
// reimplements the server's filter semantics — and a test can only be as
// honest as its mock. What it is FOR is the client wiring the server
// tests cannot see: whether the component refetches counts when a filter
// changes, hands the right arguments over, and renders counts against the
// rows it actually received. The predicates themselves are pinned
// server-side by backend/api tests/test_filter_predicates_pg.py against
// real Postgres.
type CRow = {
  name: string;
  sources: string[];
  classes: string[];
  hasConc: boolean;
  fullyLowTrust: boolean;
};

const COMPOSITION_ROWS: CRow[] = [
  { name: "quercetin", sources: ["foodatlas"], classes: ["flavonoid"], hasConc: true, fullyLowTrust: false },
  { name: "kaempferol", sources: ["foodatlas"], classes: ["flavonoid"], hasConc: false, fullyLowTrust: false },
  { name: "capsaicin", sources: ["ptfi"], classes: ["alkaloid"], hasConc: false, fullyLowTrust: false },
  { name: "rutin", sources: ["foodatlas", "ptfi"], classes: ["flavonoid"], hasConc: true, fullyLowTrust: false },
  { name: "solanine", sources: ["ptfi"], classes: [], hasConc: true, fullyLowTrust: true },
  { name: "lutein", sources: ["fdc"], classes: ["alkaloid"], hasConc: false, fullyLowTrust: false },
];

interface CFilters {
  sources: string[];
  classes: string[];
  showAllConc: boolean;
  showLowTrust: boolean;
}

const selectRows = (f: CFilters, skip?: keyof CFilters): CRow[] =>
  COMPOSITION_ROWS.filter((r) => {
    if (skip !== "sources" && !f.sources.some((s) => r.sources.includes(s))) {
      return false;
    }
    if (skip !== "classes" && f.classes.length > 0) {
      const wantsNa = f.classes.includes("n/a");
      const hit =
        (wantsNa && r.classes.length === 0) ||
        r.classes.some((c) => f.classes.includes(c));
      if (!hit) return false;
    }
    if (skip !== "showAllConc" && !f.showAllConc && !r.hasConc) return false;
    if (skip !== "showLowTrust" && !f.showLowTrust && r.fullyLowTrust) {
      return false;
    }
    return true;
  });

const installCompositionServer = () => {
  vi.mocked(getFoodCompositionData).mockImplementation((async (
    _n: string,
    _page: number,
    sources: string[],
    _search: string,
    _sort: unknown,
    showAllConc: boolean,
    classes: string[] = [],
    trust = "default"
  ) => {
    const rows = selectRows({
      sources,
      classes,
      showAllConc,
      showLowTrust: trust === "show_all",
    });
    return {
      data: rows.map((r) => ({
        id: r.name,
        name: r.name,
        median_concentration: r.hasConc
          ? { unit: "mg", value: 1, converted: false, base_units: [] }
          : null,
        chemical_classification: r.classes,
        fdc_evidences: r.sources.includes("fdc") ? [] : null,
        foodatlas_evidences: r.sources.includes("foodatlas") ? [] : null,
        ptfi_evidences: r.sources.includes("ptfi") ? [] : null,
      })),
      metadata: {
        row_count: rows.length,
        rows_per_page: 25,
        current_row: 1,
        current_page: 1,
        total_rows: rows.length,
        total_pages: rows.length > 0 ? 1 : 0,
        highlight_page: null,
      },
    };
  }) as never);

  vi.mocked(getFoodCompositionCounts).mockImplementation((async (
    _n: string,
    filters: {
      sourceFilters?: string[];
      classificationFilters?: string[];
      showAllConcentrations?: boolean;
      showLowTrust?: boolean;
    } = {}
  ) => {
    const f: CFilters = {
      sources: filters.sourceFilters ?? ["fdc", "foodatlas", "ptfi"],
      classes: filters.classificationFilters ?? [],
      showAllConc: filters.showAllConcentrations !== false,
      showLowTrust: Boolean(filters.showLowTrust),
    };
    // Each facet excludes its own dimension — the same rule the API follows.
    const source_counts: Record<string, number> = {};
    for (const s of ["fdc", "foodatlas", "ptfi"]) {
      source_counts[s] = selectRows(f, "sources").filter((r) =>
        r.sources.includes(s)
      ).length;
    }
    const classification_counts: Record<string, number> = {};
    for (const r of selectRows(f, "classes")) {
      const keys = r.classes.length ? r.classes : ["n/a"];
      for (const k of keys) {
        classification_counts[k] = (classification_counts[k] ?? 0) + 1;
      }
    }
    return {
      source_counts,
      classification_counts,
      no_concentration_count: selectRows(f, "showAllConc").filter(
        (r) => !r.hasConc
      ).length,
      low_trust_count: selectRows(f, "showLowTrust").filter(
        (r) => r.fullyLowTrust
      ).length,
    };
  }) as never);
};

// What a filter panel promises, as two separable properties:
//   counts — the number beside an option is the rows you get by picking it
//   list   — the options offered and their order do not depend on the
//            current filters; a zero is disabled in place, not hidden
// Separable because a surface can legitimately sit out ONE of them: the
// correlation tab's counts each govern a different table, so "count ==
// rows on screen" is false by construction — but its option lists are
// plain client wiring and must hold like everyone else's. The first
// version of this registry excluded whole surfaces with a single reason,
// and a reason that was true for the counts silently waived the list.
type Property = "counts" | "list";
const PROPERTIES: Property[] = ["counts", "list"];

interface RegisteredSurface extends FacetSurface {
  // Defaults to every property.
  properties?: Property[];
}

const LITERATURE_ROWS = [
  {
    id: "d1",
    name: "inflammation",
    relationship_ids: ["r4"],
    source_chemical_name: "caffeine",
    source_chemical_foodatlas_id: "e1",
    sources: [],
    improves_evidences: [{ pmid: { id: "1", url: "https://example.test/1" } }],
    worsens_evidences: null,
    ambiguity_siblings: [],
  },
  {
    id: "d2",
    name: "diabetes",
    relationship_ids: ["r3"],
    source_chemical_name: "caffeine",
    source_chemical_foodatlas_id: "e1",
    sources: [],
    improves_evidences: null,
    worsens_evidences: [{ pmid: { id: "2", url: "https://example.test/2" } }],
    ambiguity_siblings: [],
  },
];

const assay = (
  disease: string,
  relationships: string[],
  bioactivities: string[]
) => ({
  chemical_name: "caffeine",
  chemical_foodatlas_id: "e1",
  disease_name: disease,
  disease_foodatlas_id: `d-${disease}`,
  n_assays: 3,
  n_active_measurements: 2,
  relationships,
  target_genes: [],
  targets: [],
  assays: [],
  bioactivities,
});

const ASSAY_ROWS = [
  assay("inflammation", ["therapeutic"], ["anticancer", "antiviral"]),
  assay("diabetes", ["marker/mechanism"], ["antiviral"]),
  assay("asthma", ["therapeutic", "marker/mechanism"], ["anti-inflammatory"]),
];

const COMPOSITION_FOODS = [
  { id: "f1", name: "onion", median_concentration: { value: 1000, unit: "mg/100g" }, evidence_count: 12, fdc_count: 12 },
  { id: "f2", name: "apple", median_concentration: { value: 250, unit: "mg/100g" }, evidence_count: 1, foodatlas_count: 1 },
  { id: "f3", name: "parsley", median_concentration: { value: 1, unit: "mg/100g" }, evidence_count: 4, ptfi_count: 4 },
];
const COMPOSITION_UNMEASURED = [
  { id: "f4", name: "kale", median_concentration: null, evidence_count: 7, fdc_count: 7 },
];

const SURFACES: Record<string, RegisteredSurface> = {
  // Keyed by the file that renders the <FilterGroup — the toolbar — while
  // mounting the table that owns its state.
  "ChemicalCompositionToolbar": {
    mount: async () => {
      render(
        <ChemicalCompositionTable
          withConcentrations={COMPOSITION_FOODS as never}
          withoutConcentrations={COMPOSITION_UNMEASURED as never}
          commonName="quercetin"
          chemicalId="c1"
        />
      );
      await waitFor(() => expect(countTableRows()).toBeGreaterThan(0));
    },
    countRows: countTableRows,
  },
  "CorrelationEvidenceTab": {
    mount: async () => {
      vi.mocked(getDiseaseData).mockResolvedValue({
        data: { associations: LITERATURE_ROWS },
        metadata: { total_rows: LITERATURE_ROWS.length, total_pages: 1 },
      } as never);
      vi.mocked(getChemicalDiseaseAssociations).mockResolvedValue({
        data: ASSAY_ROWS,
        metadata: { row_count: ASSAY_ROWS.length },
      } as never);
      render(<CorrelationEvidenceTab commonName="caffeine" anchor="chemical" />);
      await waitFor(() => expect(countTableRows()).toBeGreaterThan(0));
    },
    countRows: countTableRows,
    // Counts sit out — see its EXCLUDED entry for why and where they live.
    properties: ["list"],
  },
  "BioactivityMeasurementsModal": {
    mount: async () => {
      render(
        <BioactivityMeasurementsModal
          isOpen
          onClose={() => {}}
          headLabel="antioxidant"
          tailLabel="quercetin"
          initialMeasurements={MEASUREMENTS as never}
        />
      );
      await waitFor(() => expect(countTableRows()).toBeGreaterThan(0));
    },
    countRows: countTableRows,
  },
  "FoodCompositionEvidenceModal": {
    mount: async () => {
      render(
        <FoodCompositionEvidenceModal
          foodName="pepper"
          chemicalName="quercetin"
          evidences={EVIDENCES as never}
          isOpen
          onClose={() => {}}
        />
      );
      await waitFor(() => expect(countTableRows()).toBeGreaterThan(0));
    },
    countRows: countTableRows,
  },
  "FoodCompositionSection": {
    mount: async () => {
      installCompositionServer();
      render(<FoodCompositionSection commonName="pepper (raw)" />);
      await waitFor(() => expect(countTableRows()).toBeGreaterThan(0));
    },
    countRows: countTableRows,
    // "n/a" is a real classification option, not a reset.
    skipLabels: ["all", "any"],
  },
};

// A (surface, property) pair not driven through this harness, with the
// reason and WHERE it is covered instead. `coveredBy` is checked, not
// read: a test file that must exist, or `surface:<Name>` — a registered
// surface that renders this component and carries the property. The
// registry test fails if any filtering component lacks coverage for any
// property, so nothing drops out silently and a reason for one property
// cannot waive another.
interface Exclusion {
  why: string;
  coveredBy: string;
}
const EXCLUDED: Record<string, Partial<Record<Property, Exclusion>>> = {
  "components/entities/shared/filters/ActivityFilterGroup.tsx": {
    counts: {
      why: "counts are countActivities(), a pure function; the group only renders them",
      coveredBy: "__tests__/activity-facet.test.tsx",
    },
    list: {
      why: "rendered inside CorrelationEvidenceTab",
      coveredBy: "surface:CorrelationEvidenceTab",
    },
  },
  "components/entities/shared/filters/SignalFilterGroup.tsx": {
    counts: {
      why: "counts are countSignals(), a pure function; the group only renders them",
      coveredBy: "__tests__/signal-filter.test.tsx",
    },
    list: {
      why: "rendered inside CorrelationEvidenceTab",
      coveredBy: "surface:CorrelationEvidenceTab",
    },
  },
  "components/entities/shared/CorrelationEvidenceTab.tsx": {
    counts: {
      why:
        "two tables under one panel — Direction filters the literature " +
        "table, Signal/Activity the assay table — so no count is 'rows on " +
        "screen'. Direction is pinned per table there; Signal and Activity " +
        "by signal-filter.test.tsx / activity-facet.test.tsx.",
      coveredBy: "__tests__/correlation-merge.test.tsx",
    },
  },
  "components/entities/bioactivity/BioactivityTable.tsx": {
    counts: {
      why:
        "server-driven; the facet predicates are executed against real " +
        "Postgres by backend test_filter_predicates_pg.py",
      coveredBy: "__tests__/bioactivity-facet-list.test.tsx",
    },
    list: {
      why: "asserted against a GROUP-BY-shaped mock of the counts endpoints",
      coveredBy: "__tests__/bioactivity-facet-list.test.tsx",
    },
  },
  "components/entities/bioactivity/FoodBioactivitiesTab.tsx": {
    counts: {
      why: "sums two BioactivityTables' server-side counts; see BioactivityTable",
      coveredBy: "__tests__/bioactivity-facet-list.test.tsx",
    },
    list: {
      why: "asserted against a GROUP-BY-shaped mock of the counts endpoints",
      coveredBy: "__tests__/bioactivity-facet-list.test.tsx",
    },
  },
};

// --- the invariants ---------------------------------------------------

const carries = (surface: RegisteredSurface, p: Property): boolean =>
  (surface.properties ?? PROPERTIES).includes(p);

describe.each(Object.entries(SURFACES))("%s", (_name, surface) => {
  const counts = carries(surface, "counts") ? it : it.skip;
  const list = carries(surface, "list") ? it : it.skip;

  counts("advertises counts that equal the rows the option yields", async () => {
    await assertFacetCountsMatchRows(surface, cleanup);
  });

  counts("never promises rows over an empty table", async () => {
    await assertNoEmptyTableUnderPositiveCount(surface, cleanup);
  });

  counts("recomputes its facets when another filter narrows the set", async () => {
    await assertFacetsRespondToOtherFilters(surface, cleanup);
  });

  counts("keeps option counts truthful while a toggle switch is on", async () => {
    await assertOptionCountsHoldUnderSwitches(surface, cleanup);
  });

  list("lists every group's options alphabetically", async () => {
    await assertOptionsAlphabetical(surface, cleanup);
  });

  list("keeps the same options, in the same order, under any filter", async () => {
    await assertOptionSetStableAcrossFilters(surface, cleanup);
  });
});

// --- the registry guard -----------------------------------------------

describe("filtering surface registry", () => {
  const COMPONENTS = path.resolve(__dirname, "..", "components");

  const walk = (dir: string): string[] =>
    fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) return walk(full);
      return e.isFile() && full.endsWith(".tsx") ? [full] : [];
    });

  const filteringSurfaces = (): string[] =>
    walk(COMPONENTS)
      .filter((f) => fs.readFileSync(f, "utf8").includes("<FilterGroup"))
      .map((f) => path.relative(path.resolve(__dirname, ".."), f))
      .sort();

  it("finds the filtering surfaces at all", () => {
    // A broken scan would make the assertion below vacuously pass.
    expect(filteringSurfaces().length).toBeGreaterThanOrEqual(5);
  });

  const registeredFor = (name: string, p: Property): boolean => {
    const surface = SURFACES[name];
    return surface !== undefined && carries(surface, p);
  };

  it("covers every filtering surface for every property", () => {
    const unaccounted: string[] = [];
    for (const rel of filteringSurfaces()) {
      const name = path.basename(rel, ".tsx");
      for (const p of PROPERTIES) {
        if (registeredFor(name, p)) continue;
        if (EXCLUDED[rel]?.[p]) continue;
        unaccounted.push(`${rel} [${p}]`);
      }
    }
    expect(unaccounted,
      `these render <FilterGroup but are neither registered in SURFACES for ` +
        `the property nor excluded from it with a reason: ` +
        `${unaccounted.join(", ")}. A filter panel with no invariant test is ` +
        `how the composition and assays-modal count bugs shipped; a surface ` +
        `excluded for its COUNTS with nothing said about its LIST is how the ` +
        `hidden-option and reshuffle bugs shipped past this file.`
    ).toEqual([]);
  });

  it("points every exclusion at coverage that exists", () => {
    // A reason is prose; `coveredBy` is checked. Either a test file on
    // disk, or a registered surface carrying that property.
    const missing: string[] = [];
    for (const [rel, byProperty] of Object.entries(EXCLUDED)) {
      for (const [p, ex] of Object.entries(byProperty)) {
        const ref = ex.coveredBy;
        const ok = ref.startsWith("surface:")
          ? registeredFor(ref.slice("surface:".length), p as Property)
          : fs.existsSync(path.resolve(__dirname, "..", ref));
        if (!ok) missing.push(`${rel} [${p}] → ${ref}`);
      }
    }
    expect(missing, `exclusions pointing at coverage that does not exist: ${missing}`).toEqual([]);
  });

  it("does not exclude a property the surface is also registered for", () => {
    // Belt and braces: an exclusion that duplicates a registration is
    // stale the moment the registration changes.
    const both: string[] = [];
    for (const [rel, byProperty] of Object.entries(EXCLUDED)) {
      const name = path.basename(rel, ".tsx");
      for (const p of Object.keys(byProperty) as Property[]) {
        if (registeredFor(name, p)) both.push(`${rel} [${p}]`);
      }
    }
    expect(both).toEqual([]);
  });

  it("has no stale exclusions", () => {
    const present = new Set(filteringSurfaces());
    const stale = Object.keys(EXCLUDED).filter((f) => !present.has(f));
    expect(stale, `EXCLUDED lists files that no longer filter: ${stale}`).toEqual(
      []
    );
  });
});

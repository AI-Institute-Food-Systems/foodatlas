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
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";
import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import {
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

const SURFACES: Record<string, FacetSurface> = {
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

// Surfaces not driven through this harness, each with a reason. The
// registry test below fails if a filtering component is missing from BOTH
// SURFACES and this list, so nothing drops out silently.
const EXCLUDED: Record<string, string> = {
  "components/entities/shared/filters/ActivityFilterGroup.tsx":
    "a filter group rendered inside other surfaces; covered through them",
  "components/entities/shared/filters/SignalFilterGroup.tsx":
    "a filter group rendered inside other surfaces; covered through them",
  "components/entities/bioactivity/BioactivityTable.tsx":
    "server-driven, same reasoning; the facet endpoints are covered by " +
    "backend TestBioactivityCountsAgreeWithRows.",
  "components/entities/bioactivity/FoodBioactivitiesTab.tsx":
    "hosts shared chrome for two BioactivityTables rather than owning its " +
    "own counts; the tables' facets are server-side and covered above.",
  "components/entities/chemical/ChemicalCompositionToolbar.tsx":
    "toolbar over a fully client-side table whose counts are derived in " +
    "chemical-composition-helpers.ts and unit-tested there.",
  "components/entities/shared/CorrelationEvidenceTab.tsx":
    "direction counts come from /correlation/direction-counts; covered by " +
    "correlation-merge.test.tsx.",
};

// --- the invariants ---------------------------------------------------

describe.each(Object.entries(SURFACES))("%s", (_name, surface) => {
  it("advertises counts that equal the rows the option yields", async () => {
    await assertFacetCountsMatchRows(surface, cleanup);
  });

  it("never promises rows over an empty table", async () => {
    await assertNoEmptyTableUnderPositiveCount(surface, cleanup);
  });

  it("recomputes its facets when another filter narrows the set", async () => {
    await assertFacetsRespondToOtherFilters(surface, cleanup);
  });

  it("keeps option counts truthful while a toggle switch is on", async () => {
    await assertOptionCountsHoldUnderSwitches(surface, cleanup);
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

  it("covers or explicitly excludes every filtering surface", () => {
    const registered = new Set(
      Object.keys(SURFACES).map((n) => n.replace(/^.*\//, ""))
    );
    const unaccounted = filteringSurfaces().filter((rel) => {
      if (rel in EXCLUDED) return false;
      return !registered.has(path.basename(rel, ".tsx"));
    });
    expect(unaccounted,
      `these render <FilterGroup but are neither registered in SURFACES nor ` +
        `listed in EXCLUDED with a reason: ${unaccounted.join(", ")}. A ` +
        `filter panel with no invariant test is how both the composition and ` +
        `assays-modal count bugs shipped.`
    ).toEqual([]);
  });

  it("has no stale exclusions", () => {
    const present = new Set(filteringSurfaces());
    const stale = Object.keys(EXCLUDED).filter((f) => !present.has(f));
    expect(stale, `EXCLUDED lists files that no longer filter: ${stale}`).toEqual(
      []
    );
  });
});

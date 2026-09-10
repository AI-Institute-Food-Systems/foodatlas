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
vi.mock("@/utils/fetching", () => ({
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
  getChemicalCompositionEvidence: vi.fn().mockResolvedValue([]),
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";

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
};

// Surfaces not driven through this harness, each with a reason. The
// registry test below fails if a filtering component is missing from BOTH
// SURFACES and this list, so nothing drops out silently.
const EXCLUDED: Record<string, string> = {
  "components/entities/shared/filters/ActivityFilterGroup.tsx":
    "a filter group rendered inside other surfaces; covered through them",
  "components/entities/shared/filters/SignalFilterGroup.tsx":
    "a filter group rendered inside other surfaces; covered through them",
  "components/entities/food/FoodCompositionSection.tsx":
    "server-driven: counts come from /food/composition/counts and rows from " +
    "/food/composition, so the invariant is only as good as the mock. Held " +
    "instead by the server-side pair (backend test_filter_predicates_pg.py " +
    "asserts facet == rows against real Postgres) plus " +
    "composition-source-filter.test.tsx for the contradiction case.",
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

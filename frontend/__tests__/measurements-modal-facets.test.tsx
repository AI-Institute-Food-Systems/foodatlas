// Faceted counts in the assays modal must describe the rows you'd get.
//
// Every sidebar count in this app follows one rule: a facet applies every
// OTHER active filter and excludes its own, so the number answers "what
// would I get if I clicked this?". Break that and the count describes a
// row set nobody is looking at — which is exactly the composition bug,
// where the pager promised 309 rows above an empty table.
//
// The assays modal broke it client-side: `evidenceTypeOptions` counted the
// raw row set with a `[rows]` dependency, so picking Outcome
// "inconclusive" shrank the table while every Evidence number sat frozen.
// The Postgres harness cannot see this — these filters never reach the
// API, they narrow an already-fetched array in the browser — and the
// route-param meta-test cannot either, since there is no `filter_*` query
// param behind them.
//
// So the invariant is asserted here directly, per dimension, by reading
// the rendered counts and the rendered row count.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

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
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";

const m = (
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

// Deliberately lopsided: "inconclusive" exists only for in-vitro, so a
// frozen Evidence count is numerically distinguishable from a faceted one.
const MEASUREMENTS = [
  m("active", "in vitro", "experimental", "a1"),
  m("active", "in vitro", "experimental", "a2"),
  m("active", "molecular-level", "experimental", "a3"),
  m("inactive", "molecular-level", "predicted", "a4"),
  m("inconclusive", "in vitro", "experimental", "a5"),
  m("unspecified", "adme/tox", "predicted", "a6"),
];

// The modal renders through a HeadlessUI portal, so `render().container`
// is empty — every query has to go through document.body.
const mount = async () => {
  render(
    <BioactivityMeasurementsModal
      isOpen
      onClose={() => {}}
      headLabel="antioxidant"
      tailLabel="quercetin"
      initialMeasurements={MEASUREMENTS as never}
    />
  );
  // The table body is committed one paint after open.
  await waitFor(() => expect(renderedRows()).toBeGreaterThan(0));
};

// FilterOption renders single-select groups (Outcome, Source) as
// role="radio" and multi-select ones (Evidence) as plain buttons, so a
// single-role query silently misses half the filter surface.
const findOption = (label: string): HTMLElement | undefined => {
  const candidates = [
    ...screen.queryAllByRole("radio"),
    ...screen.queryAllByRole("button"),
  ];
  // The label and its count are adjacent text nodes ("in vitro3"), so a
  // \b anchor never matches — there is no word boundary between "o" and
  // "3". Match the label followed by an optional count and nothing else.
  const escaped = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`^${escaped}\\s*\\d*$`, "i");
  return candidates.find((el) => re.test((el.textContent ?? "").trim()));
};

/** Rendered count next to a filter option, by its visible label. */
const facetCount = (label: string): number => {
  const option = findOption(label);
  expect(option, `no filter option labelled ${label}`).toBeTruthy();
  // Label and count are adjacent text nodes: "in vitro3".
  const match = (option?.textContent ?? "").trim().match(/(\d+)\s*$/);
  return match ? Number(match[1]) : 0;
};

const clickFacet = (label: string) => {
  const option = findOption(label);
  expect(option, `no filter option labelled ${label}`).toBeTruthy();
  fireEvent.click(option as HTMLElement);
};

/** Rows actually rendered, excluding the padding rows the shell adds. */
const renderedRows = (): number =>
  Array.from(document.body.querySelectorAll("tbody tr")).filter((tr) =>
    (tr.textContent ?? "").trim()
  ).length;

describe("assays modal faceted counts", () => {
  it("updates Evidence counts when Outcome changes", async () => {
    await mount();

    // Unfiltered: in vitro covers a1, a2, a5.
    expect(facetCount("in vitro")).toBe(3);

    clickFacet("inconclusive");

    // Only a5 is inconclusive, and it is in vitro — so in vitro must
    // drop to 1 and the other buckets to 0. Frozen counts stay at 3.
    await waitFor(() => expect(facetCount("in vitro")).toBe(1));
    expect(facetCount("molecular-level")).toBe(0);
    expect(facetCount("adme/tox")).toBe(0);
    expect(renderedRows()).toBe(1);
  });

  it("keeps every Evidence count equal to the rows that selection yields", async () => {
    await mount();

    for (const outcome of ["active", "inactive", "unspecified", "inconclusive"]) {
      clickFacet(outcome);
      await waitFor(() => expect(renderedRows()).toBeGreaterThanOrEqual(0));

      for (const et of ["in vitro", "molecular-level", "adme/tox"]) {
        const promised = facetCount(et);
        // A zero is disabled rather than hidden, so it stays in place but
        // cannot be picked — there is no row set for it to promise.
        if (promised === 0) {
          expect(findOption(et)).toHaveAttribute("aria-disabled", "true");
          continue;
        }
        clickFacet(et);
        await waitFor(() =>
          expect(renderedRows()).toBe(promised)
        );
        clickFacet(et); // deselect, back to the outcome-only view
      }
      clickFacet("all"); // reset outcome
    }
  });

  it("updates Outcome and Source counts when Evidence changes", async () => {
    // The sibling half of the same defect: Evidence was added after the
    // other facets and never wired into them, so Outcome and Source
    // counted as though no Evidence filter existed.
    await mount();

    // Unfiltered: 2 active in vitro, 1 active molecular-level.
    expect(facetCount("active")).toBe(3);

    clickFacet("molecular-level");

    // Restricted to molecular-level: a3 (active) and a4 (inactive).
    await waitFor(() => expect(facetCount("active")).toBe(1));
    expect(facetCount("inactive")).toBe(1);
    expect(facetCount("inconclusive")).toBe(0);
    expect(renderedRows()).toBe(2);
  });

  it("does not let a count promise rows the table cannot show", async () => {
    // The contradiction invariant, per dimension: a positive facet count
    // must yield at least one row when selected.
    await mount();
    clickFacet("inconclusive");
    await waitFor(() => expect(renderedRows()).toBe(1));

    for (const et of ["in vitro", "molecular-level", "adme/tox"]) {
      const promised = facetCount(et);
      if (promised === 0) continue;
      clickFacet(et);
      await waitFor(() => expect(renderedRows()).toBeGreaterThan(0));
      clickFacet(et);
    }
  });
});

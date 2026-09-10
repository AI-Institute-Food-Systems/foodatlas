// One invariant, asserted the same way on every filtering surface.
//
// Every filter sidebar and modal in the app promises the same thing: the
// number beside an option is what you get if you click it. Break that and
// the UI contradicts itself — a pager over an empty table (the
// composition source bug), or counts frozen while the table shrinks (the
// assays modal Outcome bug). Both shipped because the promise was only
// ever checked by hand, one surface at a time.
//
// Rather than a bespoke test per surface, this crawls whatever is
// rendered. It works because every surface builds its options from the
// same `FilterOption` in components/entities/shared/filters/
// FilterControls.tsx, so the DOM shape is uniform:
//
//   <button aria-pressed|role=radio>
//     <span aria-hidden>          indicator
//     <span>label</span>
//     <span class="tabular-nums">count</span>   (optional)
//   </button>
//
// A surface therefore only has to say how to mount itself and how to
// count its rows; the assertions come from here.

import { waitFor } from "@testing-library/react";
import { expect } from "vitest";

export interface Facet {
  label: string;
  /** Identifies the option's group (Source, Outcome, ...) by container. */
  group: Element | null;
  /** undefined when the option renders no count (counts still loading). */
  count?: number;
  selected: boolean;
  disabled: boolean;
  el: HTMLElement;
}

/** Every filter option currently rendered, wherever it is in the document. */
export function readFacets(): Facet[] {
  const buttons = Array.from(
    document.body.querySelectorAll<HTMLButtonElement>(
      'button[aria-pressed], button[role="radio"]'
    )
  );
  return buttons.map((el) => {
    // DIRECT children only. A selected option nests a second <span> inside
    // the indicator (the radio dot / check glyph) which carries no
    // aria-hidden of its own, so a descendant query returns it as the
    // label and every selected option reads as "". That silently broke the
    // skip-list here: an option was excluded before it was clicked and
    // included after, which made a sum comparison shift by that option's
    // count and look like real narrowing.
    const spans = Array.from(el.children).filter(
      (c): c is HTMLElement => c.tagName === "SPAN"
    );
    const countSpan = spans.find((s) => s.className.includes("tabular-nums"));
    const labelSpan = spans.find(
      (s) => !s.hasAttribute("aria-hidden") && s !== countSpan
    );
    const raw = countSpan?.textContent?.trim() ?? "";
    const parsed = raw === "" ? Number.NaN : Number(raw.replace(/,/g, ""));
    return {
      label: (labelSpan?.textContent ?? "").trim(),
      // Options in one FilterGroup share a parent (the FilterOptionList
      // container), which is enough to tell dimensions apart without
      // coupling to class names.
      group: el.parentElement,
      count: Number.isNaN(parsed) ? undefined : parsed,
      selected:
        el.getAttribute("aria-pressed") === "true" ||
        el.getAttribute("aria-checked") === "true",
      disabled: el.disabled || el.getAttribute("aria-disabled") === "true",
      el,
    };
  });
}

export interface FacetSurface {
  /** Render fresh. Must resolve once rows are on screen. */
  mount: () => Promise<void>;
  /** Rows the table/list is actually showing (exclude padding + empty state). */
  countRows: () => number;
  /**
   * Options that are not row filters and must be skipped — typically the
   * "All"/"Any" reset of a radio group, whose count is the unfiltered
   * total by definition.
   */
  skipLabels?: string[];
  /** Guard the guard: fail if fewer than this many options are found. */
  minFacets?: number;
}

const isSkipped = (f: Facet, skip: string[]): boolean =>
  skip.some((s) => s.toLowerCase() === f.label.toLowerCase());

/**
 * For every filter option on the surface: the count it advertises must
 * equal the rows selecting it yields.
 *
 * Remounts per option rather than toggling in place — radio groups cannot
 * be un-clicked, and leaving one dimension set while testing the next
 * turns an ordering accident into a passing test.
 */
export async function assertFacetCountsMatchRows(
  surface: FacetSurface,
  cleanup: () => void
): Promise<void> {
  const skip = surface.skipLabels ?? ["all", "any"];

  await surface.mount();
  const discovered = readFacets().filter((f) => !isSkipped(f, skip));
  const min = surface.minFacets ?? 2;
  expect(
    discovered.length,
    `only ${discovered.length} filter options found; the crawler is probably ` +
      "not seeing this surface's options and every assertion below would be " +
      "vacuous"
  ).toBeGreaterThanOrEqual(min);
  const labels = discovered
    .filter((f) => typeof f.count === "number")
    .map((f) => f.label);
  cleanup();

  for (const label of labels) {
    await surface.mount();
    const facet = readFacets().find((f) => f.label === label);
    if (!facet || facet.disabled || typeof facet.count !== "number") {
      cleanup();
      continue;
    }
    const promised = facet.count;
    facet.el.click();
    await waitFor(() => {
      expect(
        surface.countRows(),
        `option "${label}" advertises ${promised} rows but the table renders ` +
          `${surface.countRows()}`
      ).toBe(promised);
    });
    cleanup();
  }
}

/**
 * A positive count must never yield an empty table.
 *
 * Weaker than the equality above and worth asserting separately: it is
 * the exact contradiction users report ("it says 309, I see nothing"),
 * and it still holds for surfaces whose counts are deliberately
 * approximate.
 */
export async function assertNoEmptyTableUnderPositiveCount(
  surface: FacetSurface,
  cleanup: () => void
): Promise<void> {
  const skip = surface.skipLabels ?? ["all", "any"];

  await surface.mount();
  const labels = readFacets()
    .filter((f) => !isSkipped(f, skip) && !f.disabled && (f.count ?? 0) > 0)
    .map((f) => f.label);
  cleanup();

  for (const label of labels) {
    await surface.mount();
    const facet = readFacets().find((f) => f.label === label);
    if (!facet || facet.disabled) {
      cleanup();
      continue;
    }
    facet.el.click();
    await waitFor(() => {
      expect(
        surface.countRows(),
        `option "${label}" advertises ${facet.count} rows but the table is empty`
      ).toBeGreaterThan(0);
    });
    cleanup();
  }
}

/**
 * Selecting one dimension must move the other dimensions' counts.
 *
 * This is the assays-modal bug: `evidenceTypeOptions` was computed from
 * the raw rows, so Outcome changed the table and left every Evidence
 * number frozen. Expressed generically: after narrowing to some option,
 * the *sum* of the counts in every other group must not exceed the
 * unfiltered sum — a facet that ignores the active filter keeps its old,
 * larger numbers.
 */
export async function assertFacetsRespondToOtherFilters(
  surface: FacetSurface,
  cleanup: () => void
): Promise<void> {
  const skip = surface.skipLabels ?? ["all", "any"];
  const sumOfOtherGroups = (facets: Facet[], group: Element | null): number =>
    facets
      .filter((f) => f.group !== group && !isSkipped(f, skip))
      .reduce((n, f) => n + (f.count ?? 0), 0);

  await surface.mount();
  const before = readFacets();
  const groups = Array.from(new Set(before.map((f) => f.group)));
  if (groups.length < 2) {
    // Only one dimension on screen. Narrowing within a group correctly
    // leaves that group's own counts alone (each option excludes itself),
    // so there is nothing cross-dimensional to assert here.
    cleanup();
    return;
  }
  // One narrowing per group, not just the single most restrictive option
  // overall. A facet that ignores exactly one sibling dimension is
  // invisible unless you narrow using THAT dimension — the assays modal
  // had counts blind to the Evidence filter specifically, and narrowing
  // by Outcome sailed straight past it.
  const plan = groups
    .map((group) => {
      const option = before
        .filter(
          (f) =>
            f.group === group &&
            !isSkipped(f, skip) &&
            !f.disabled &&
            (f.count ?? 0) > 0
        )
        .sort((a, b) => (a.count ?? 0) - (b.count ?? 0))[0];
      return option
        ? {
            label: option.label,
            count: option.count ?? 0,
            others: sumOfOtherGroups(before, group),
          }
        : null;
    })
    .filter((p): p is NonNullable<typeof p> => p !== null);
  const unfilteredRows = surface.countRows();
  cleanup();

  for (const step of plan) {
    if (step.count >= unfilteredRows) continue; // does not actually narrow
    await surface.mount();
    const target = readFacets().find((f) => f.label === step.label);
    expect(target, `lost track of option "${step.label}"`).toBeTruthy();
    const targetGroup = target?.group ?? null;
    target?.el.click();
    await waitFor(() => expect(surface.countRows()).toBe(step.count));

    const after = sumOfOtherGroups(readFacets(), targetGroup);
    expect(
      after,
      `selecting "${step.label}" narrowed the table to ${step.count} rows, ` +
        `but the OTHER filter groups' counts still sum to ${after} (was ` +
        `${step.others}). At least one group is counting the unfiltered ` +
        "set — its facet is not applying the active filter from this dimension."
    ).toBeLessThan(step.others);
    cleanup();
  }
}

/** Rows on screen, excluding the empty-state row and padding rows. */
export const countTableRows = (): number =>
  Array.from(document.body.querySelectorAll("tbody tr")).filter((tr) => {
    const text = (tr.textContent ?? "").trim();
    if (!text) return false;
    // The shared empty state renders as a single full-width cell.
    if (/no (associations|results|data|measurements)/i.test(text)) return false;
    return true;
  }).length;

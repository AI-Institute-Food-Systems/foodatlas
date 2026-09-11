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
  /** "option" = FilterOption checkbox/radio; "switch" = ToggleSwitch. */
  kind: "option" | "switch";
  /** Radio groups are single-select; check groups can start all-selected. */
  mode: "radio" | "check" | "switch";
  /** Identifies the option's group (Source, Outcome, ...) by container. */
  group: Element | null;
  /** undefined when the option renders no count (counts still loading). */
  count?: number;
  selected: boolean;
  disabled: boolean;
  el: HTMLElement;
}

/** Every filter control currently rendered, wherever it is in the document. */
export function readFacets(): Facet[] {
  return [...readOptions(), ...readSwitches()];
}

/** FilterOption checkboxes and radios. */
function readOptions(): Facet[] {
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
    return {
      label: (labelSpan?.textContent ?? "").trim(),
      kind: "option" as const,
      mode: (el.getAttribute("role") === "radio" ? "radio" : "check") as
        | "radio"
        | "check",
      count: parseCount(countSpan),
      // Options in one FilterGroup share a parent (the FilterOptionList
      // container), which is enough to tell dimensions apart without
      // coupling to class names.
      group: el.parentElement,
      selected:
        el.getAttribute("aria-pressed") === "true" ||
        el.getAttribute("aria-checked") === "true",
      disabled: el.disabled || el.getAttribute("aria-disabled") === "true",
      el,
    };
  });
}

/**
 * ToggleSwitch rows.
 *
 * A different shape and a different meaning, both of which matter. Shape:
 * HeadlessUI renders `button[role=switch]` and the label and count are
 * siblings of that button inside the enclosing <label>, not children of
 * it — so the option crawler above sees nothing. Meaning: a switch's
 * count is not "rows you will get". "Without concentration" counts the
 * rows it governs, and "Low-trust data points" counts chemicals holding a
 * hidden point while the row total does not move at all. So switches are
 * deliberately NOT fed to the count-equals-rows assertion; they are here
 * for the cross-dimension one, which does hold: flipping a switch changes
 * the row set, so every other group's counts must be recomputed.
 */
function readSwitches(): Facet[] {
  const switches = Array.from(
    document.body.querySelectorAll<HTMLButtonElement>('button[role="switch"]')
  );
  return switches.map((el) => {
    const container = el.closest("label");
    const spans = Array.from(container?.children ?? []).filter(
      (c): c is HTMLElement => c.tagName === "SPAN"
    );
    const countSpan = spans.find((s) => s.className.includes("tabular-nums"));
    const labelSpan = spans.find((s) => s !== countSpan);
    return {
      label: (labelSpan?.textContent ?? "").trim(),
      kind: "switch" as const,
      mode: "switch" as const,
      count: parseCount(countSpan),
      group: container?.parentElement ?? null,
      selected: el.getAttribute("aria-checked") === "true",
      disabled: el.disabled || el.getAttribute("aria-disabled") === "true",
      el,
    };
  });
}

function parseCount(span?: HTMLElement): number | undefined {
  const raw = span?.textContent?.trim() ?? "";
  if (raw === "") return undefined;
  const n = Number(raw.replace(/,/g, ""));
  return Number.isNaN(n) ? undefined : n;
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


/**
 * Put the surface into the state option `label` advertises.
 *
 * Not simply "click it". A check-mode group can start with everything
 * selected — Source on the composition table does — and there a click
 * DEselects, leaving the complement of what the count describes. Clicking
 * "FDC" on a fresh panel drops FDC and shows the other four rows, so a
 * naive harness reads that as the count lying.
 *
 * Isolating means: deselect every other option in the group, then ensure
 * this one is on. Re-reads between clicks because each one re-renders.
 */
async function isolateOption(
  label: string,
  countRows: () => number
): Promise<void> {
  const target = readFacets().find(
    (f) => f.kind === "option" && f.label === label
  );
  if (!target) return;
  if (target.mode === "radio") {
    target.el.click();
    await waitFor(() => expect(countRows()).toBeGreaterThanOrEqual(0));
    return;
  }
  const group = target.group;
  // Deselect the siblings one at a time.
  for (;;) {
    const sibling = readFacets().find(
      (f) =>
        f.kind === "option" &&
        f.group === group &&
        f.label !== label &&
        f.selected &&
        !f.disabled
    );
    if (!sibling) break;
    sibling.el.click();
    await waitFor(() => expect(countRows()).toBeGreaterThanOrEqual(0));
  }
  const now = readFacets().find(
    (f) => f.kind === "option" && f.label === label
  );
  if (now && !now.selected && !now.disabled) {
    now.el.click();
    await waitFor(() => expect(countRows()).toBeGreaterThanOrEqual(0));
  }
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
  const discovered = readFacets().filter(
    (f) => f.kind === "option" && !isSkipped(f, skip)
  );
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
    await isolateOption(label, surface.countRows);
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
    .filter(
      (f) =>
        f.kind === "option" &&
        !isSkipped(f, skip) &&
        !f.disabled &&
        (f.count ?? 0) > 0
    )
    .map((f) => f.label);
  cleanup();

  for (const label of labels) {
    await surface.mount();
    const facet = readFacets().find((f) => f.label === label);
    if (!facet || facet.disabled) {
      cleanup();
      continue;
    }
    await isolateOption(label, surface.countRows);
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
      .filter(
        (f) => f.kind === "option" && f.group !== group && !isSkipped(f, skip)
      )
      .reduce((n, f) => n + (f.count ?? 0), 0);

  await surface.mount();
  // Options only. Switches are a filter dimension too, but they are not
  // isolatable the same way and their counts are not row counts, so they
  // have their own assertion (assertOptionCountsHoldUnderSwitches).
  const before = readFacets().filter((f) => f.kind === "option");
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
    await isolateOption(step.label, surface.countRows);
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

/**
 * Option counts must stay truthful while a switch is on.
 *
 * A switch is just another active filter, and the facets around it have
 * to respect it exactly as they respect each other. Its own count is NOT
 * a row count — "Without concentration" counts the rows it governs and
 * "Low-trust data points" counts chemicals holding a hidden point while
 * the row total does not move — so the switch is never asserted against
 * rows. What is asserted is that after flipping it, every other group's
 * most-restrictive option still advertises the rows it yields.
 *
 * Bounded to one option per group per switch: the point is whether the
 * switch is plumbed into the facet computation at all, and the first
 * option answers that as well as the twentieth.
 */
export async function assertOptionCountsHoldUnderSwitches(
  surface: FacetSurface,
  cleanup: () => void
): Promise<void> {
  const skip = surface.skipLabels ?? ["all", "any"];

  await surface.mount();
  const switchLabels = readFacets()
    .filter((f) => f.kind === "switch" && !f.disabled)
    .map((f) => f.label);
  cleanup();
  if (switchLabels.length === 0) return;

  for (const switchLabel of switchLabels) {
    await surface.mount();
    const groups = Array.from(
      new Set(
        readFacets()
          .filter((f) => f.kind === "option")
          .map((f) => f.group)
      )
    );
    cleanup();

    for (let groupIndex = 0; groupIndex < groups.length; groupIndex += 1) {
      await surface.mount();
      const sw = readFacets().find(
        (f) => f.kind === "switch" && f.label === switchLabel
      );
      expect(sw, `lost the "${switchLabel}" switch`).toBeTruthy();
      sw?.el.click();
      await waitFor(() => expect(surface.countRows()).toBeGreaterThanOrEqual(0));

      const optionGroups = Array.from(
        new Set(
          readFacets()
            .filter((f) => f.kind === "option")
            .map((f) => f.group)
        )
      );
      const group = optionGroups[groupIndex];
      const option = readFacets()
        .filter(
          (f) =>
            f.kind === "option" &&
            f.group === group &&
            !isSkipped(f, skip) &&
            !f.disabled &&
            (f.count ?? 0) > 0
        )
        .sort((a, b) => (a.count ?? 0) - (b.count ?? 0))[0];
      if (!option) {
        cleanup();
        continue;
      }
      const promised = option.count ?? 0;
      await isolateOption(option.label, surface.countRows);
      await waitFor(() => {
        expect(
          surface.countRows(),
          `with "${switchLabel}" flipped, option "${option.label}" advertises ` +
            `${promised} rows but the table renders ${surface.countRows()} — ` +
            "that facet is not applying the switch."
        ).toBe(promised);
      });
      cleanup();
    }
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

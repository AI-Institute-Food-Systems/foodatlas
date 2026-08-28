// The Signal facet: CTD's two DirectEvidence values as a filter.
//
// The rule that makes it not a radio: a row can carry BOTH values — a
// chemical that is therapeutic for a disease and also marks it — so the
// options overlap, selecting both is not the same as selecting neither,
// and a row matches if it carries ANY selected value.

import { describe, expect, it } from "vitest";

import {
  SIGNALS,
  countSignals,
  matchesSignals,
} from "@/components/entities/shared/filters/SignalFilterGroup";
import { MARKER, THERAPEUTIC } from "@/components/entities/shared/SignalChips";

const row = (...relationships: string[]) => ({ relationships });

describe("the option list", () => {
  it("offers exactly CTD's two DirectEvidence values", () => {
    // Verified against the snapshot: mv_disease_bioactivity holds only
    // these two, 341,981 marker/mechanism to 85,537 therapeutic.
    expect(SIGNALS.map((s) => s.key)).toEqual([THERAPEUTIC, MARKER]);
  });
});

describe("matchesSignals", () => {
  it("keeps everything when nothing is selected", () => {
    expect(matchesSignals([MARKER], [])).toBe(true);
    expect(matchesSignals(undefined, [])).toBe(true);
  });

  it("keeps a row carrying any selected value", () => {
    expect(matchesSignals([MARKER, THERAPEUTIC], [THERAPEUTIC])).toBe(true);
    expect(matchesSignals([MARKER], [THERAPEUTIC])).toBe(false);
  });

  it("drops a row with no relationships once a filter is on", () => {
    expect(matchesSignals(undefined, [THERAPEUTIC])).toBe(false);
    expect(matchesSignals([], [MARKER])).toBe(false);
  });
});

describe("countSignals", () => {
  it("counts rows per value, not values", () => {
    const counts = countSignals([
      row(THERAPEUTIC),
      row(MARKER),
      row(MARKER, THERAPEUTIC),
    ]);
    // The both-ways row counts once under each — which is why the two
    // counts can sum past the number of rows.
    expect(counts[THERAPEUTIC]).toBe(2);
    expect(counts[MARKER]).toBe(2);
  });

  it("counts a repeated value once per row", () => {
    expect(countSignals([row(MARKER, MARKER)])[MARKER]).toBe(1);
  });

  it("ignores rows with no relationships", () => {
    expect(countSignals([{ relationships: undefined }])).toEqual({});
  });
});

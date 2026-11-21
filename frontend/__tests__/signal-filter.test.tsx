// The Signal facet: CTD's two DirectEvidence values as a filter.
//
// The rule that makes it not a radio: a row can carry BOTH values — a
// chemical that is therapeutic for a disease and also marks it — so the
// options overlap, selecting both is not the same as selecting neither,
// and a row matches if it carries ANY selected value.

import { describe, expect, it } from "vitest";

import {
  SIGNALS,
} from "@/components/entities/shared/filters/SignalFilterGroup";
import { MARKER, THERAPEUTIC } from "@/components/entities/shared/SignalChips";

const row = (...relationships: string[]) => ({ relationships });

describe("the option list", () => {
  it("offers exactly CTD's two DirectEvidence values", () => {
    // Verified against the snapshot: mv_disease_bioactivity holds only
    // these two, 341,981 marker/mechanism to 85,537 therapeutic.
    expect(SIGNALS.map((s) => s.key)).toEqual([MARKER, THERAPEUTIC]);
  });

  it("lists them alphabetically by label, like every facet", () => {
    const labels = SIGNALS.map((s) => s.label);
    expect(labels).toEqual(
      [...labels].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }))
    );
  });
});

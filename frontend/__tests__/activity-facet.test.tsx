// The Activity facet and cell, which replaced the disease page's separate
// Bioactivities tab.
//
// That tab read mv_disease_bioactivity: the same (chemical, disease) pairs
// as the lab-assay table — 347,632 either way, set difference 0 — split one
// row per activity instead of one per chemical. So the dimension is now a
// facet plus a per-row cell, and the grain stays one row per chemical.

import { describe, expect, it } from "vitest";

import {
  countActivities,
  matchesActivities,
} from "@/components/entities/shared/filters/ActivityFilterGroup";

const row = (...bioactivities: string[]) => ({ bioactivities });

describe("matchesActivities", () => {
  it("keeps everything when nothing is selected", () => {
    expect(matchesActivities(["anticancer"], [])).toBe(true);
    expect(matchesActivities(undefined, [])).toBe(true);
  });

  it("keeps a row carrying any selected activity", () => {
    // 27% of pairs carry more than one, so this is an ANY match, not an
    // equality test — the same rule the Signal facet uses.
    expect(matchesActivities(["anticancer", "antiviral"], ["antiviral"])).toBe(
      true
    );
    expect(matchesActivities(["anticancer"], ["antiviral"])).toBe(false);
  });

  it("drops a row with no activities once a filter is on", () => {
    expect(matchesActivities(undefined, ["anticancer"])).toBe(false);
    expect(matchesActivities([], ["anticancer"])).toBe(false);
  });
});

describe("countActivities", () => {
  it("counts rows per activity, not activities", () => {
    const counts = countActivities([
      row("anticancer"),
      row("anticancer", "antiviral"),
      row("antiviral"),
    ]);
    expect(counts.anticancer).toBe(2);
    expect(counts.antiviral).toBe(2);
  });

  it("counts a repeated activity once per row", () => {
    expect(countActivities([row("anticancer", "anticancer")]).anticancer).toBe(
      1
    );
  });

  it("ignores rows with no activities", () => {
    expect(countActivities([{ bioactivities: undefined }])).toEqual({});
  });
});

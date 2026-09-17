// The Activity facet and cell, which replaced the disease page's separate
// Bioactivities tab.
//
// That tab read mv_disease_bioactivity: the same (chemical, disease) pairs
// as the lab-assay table — 347,632 either way, set difference 0 — split one
// row per activity instead of one per chemical. So the dimension is now a
// facet plus a per-row cell, and the grain stays one row per chemical.

import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import ActivityFilterGroup, {
  activitiesOf,
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

describe("countActivities with a universe", () => {
  it("returns every universe key, zero when no filtered row carries it", () => {
    // The keys used to come from the FILTERED rows, so a search that
    // excluded an activity dropped it from the sidebar instead of
    // greying it out.
    const counts = countActivities([row("anticancer")], [
      "anticancer",
      "antiviral",
    ]);
    expect(counts).toEqual({ anticancer: 1, antiviral: 0 });
  });

  it("ignores activities outside the universe", () => {
    expect(countActivities([row("stale")], ["anticancer"])).toEqual({
      anticancer: 0,
    });
  });
});

describe("activitiesOf", () => {
  it("collects the distinct activities across rows", () => {
    expect(
      activitiesOf([row("anticancer", "antiviral"), row("antiviral")])
    ).toEqual(["anticancer", "antiviral"]);
  });
});

describe("ActivityFilterGroup's list", () => {
  it("is alphabetical, whatever the counts", () => {
    // Busiest-first was the rule here, on the argument that the tail is
    // long. But the counts move with every other filter, so the options
    // reshuffled on every click.
    const { container } = render(
      <ActivityFilterGroup
        selected={[]}
        counts={{ antiviral: 9, anticancer: 1, "anti-inflammatory": 5 }}
        onToggle={() => {}}
        onClear={() => {}}
      />
    );
    const labels = Array.from(
      container.querySelectorAll("button[aria-pressed]")
    ).map((b) =>
      (
        Array.from(b.children).find(
          (c) => c.tagName === "SPAN" && !c.hasAttribute("aria-hidden") && !c.className.includes("tabular-nums")
        )?.textContent ?? ""
      ).trim()
    );
    expect(labels).toEqual(["anti-inflammatory", "anticancer", "antiviral"]);
  });

  it("keeps a zero in place, disabled", () => {
    const { container } = render(
      <ActivityFilterGroup
        selected={[]}
        counts={{ anticancer: 0, antiviral: 2 }}
        onToggle={() => {}}
        onClear={() => {}}
      />
    );
    const buttons = container.querySelectorAll<HTMLButtonElement>(
      "button[aria-pressed]"
    );
    expect(buttons).toHaveLength(2);
    expect(buttons[0]).toBeDisabled();
    expect(buttons[1]).toBeEnabled();
  });
});

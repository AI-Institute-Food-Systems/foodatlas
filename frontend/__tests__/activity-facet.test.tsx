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
} from "@/components/entities/shared/filters/ActivityFilterGroup";

const row = (...bioactivities: string[]) => ({ bioactivities });

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

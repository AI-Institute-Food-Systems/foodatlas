import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import EntityLoadingShell from "@/components/entities/EntityLoadingShell";
import {
  DEFAULT_TAB_ID,
  ENTITY_TABS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

const ENTITY_TYPES = Object.keys(ENTITY_TABS) as EntityType[];

// The desktop chip strip, which is the thing that used to visibly grow.
const chips = (container: HTMLElement) =>
  Array.from(container.querySelectorAll("div.hidden.sm\\:flex > div"));

describe("EntityLoadingShell", () => {
  it.each(ENTITY_TYPES)("renders every %s tab chip", (entity) => {
    // Direct regression test for the tabCount mismatch: chemical rendered
    // 4 chips against a real 5, disease 2 against 4, bioactivity 3
    // against 4. Deriving both from the config makes that unrepresentable
    // — this asserts the derivation actually happens.
    const { container } = render(<EntityLoadingShell entityType={entity} />);
    expect(chips(container)).toHaveLength(ENTITY_TABS[entity].length);
  });

  it.each(ENTITY_TYPES)("labels %s chips for real", (entity) => {
    const { container } = render(<EntityLoadingShell entityType={entity} />);
    const labels = chips(container).map((c) => c.textContent?.trim());

    expect(labels).toEqual(ENTITY_TABS[entity].map((t) => t.label));
  });

  it.each(ENTITY_TYPES)("marks the %s default tab as selected", (entity) => {
    // The SSR shell paints the default tab as the cream selected chip, so
    // the loading shell has to as well or the handoff recolours a chip.
    const { container } = render(<EntityLoadingShell entityType={entity} />);
    const index = ENTITY_TABS[entity].findIndex(
      (t) => t.id === DEFAULT_TAB_ID[entity]
    );

    expect(chips(container)[index]?.className).toContain("bg-light-200");
  });

  it.each(ENTITY_TYPES)("reserves %s count badges while pending", (entity) => {
    // Scoped to the chip strip: the header renders a real Badge that is
    // also rounded-full and is not a placeholder.
    const { container } = render(<EntityLoadingShell entityType={entity} />);
    const reserved = chips(container).filter(
      (chip) => chip.querySelector(".rounded-full") !== null
    );
    const expected = ENTITY_TABS[entity].filter((t) => t.hasCount).length;

    expect(reserved).toHaveLength(expected);
  });
});

import type { ReactNode } from "react";

import type { TabSpec } from "@/components/entities/EntityTabs";
import {
  ENTITY_TABS,
  type EntityType,
  type TabIdOf,
} from "@/components/entities/entityTabs.config";

// The per-tab pieces a page supplies: everything that isn't already
// declared in the config. Ids, labels and ordering come from the config
// so a page can't invent, drop or reorder one.
type TabParts = {
  count?: number | null;
  content: ReactNode;
};

// Assembles a page's tabs from the shared config.
//
// `Record<TabIdOf<E>, TabParts>` is what makes the old `tabCount` drift
// unrepresentable: a missing tab fails the index signature and an
// unknown one fails excess-property checking, both at compile time. No
// page writes a tab count any more, so `loading.tsx` and `page.tsx`
// cannot disagree about how many chips to render.
export const buildTabs = <E extends EntityType>(
  entityType: E,
  parts: Record<TabIdOf<E>, TabParts>
): TabSpec[] =>
  ENTITY_TABS[entityType].map((def) => ({
    id: def.id,
    label: def.label,
    hasCount: def.hasCount,
    ...parts[def.id as TabIdOf<E>],
  }));

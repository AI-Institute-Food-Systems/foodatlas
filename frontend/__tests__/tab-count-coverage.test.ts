// Every tab declared `hasCount: true` must receive a count from its page.
//
// Tabs mount lazily, so a tab publishes its own count only once opened.
// Until then the badge falls back to whatever `page.tsx` prefetched — and
// if nothing did, the placeholder pulses for the life of the page. The
// disease page shipped with none of its three counted tabs fetched, so
// all three pulsed; chemical was missing two.
//
// Static, because these are async Server Components: rendering one here
// would mean standing up the whole data layer to assert a prop is passed.

import { readFileSync } from "fs";
import { join } from "path";
import { describe, expect, it } from "vitest";

import {
  ENTITY_TABS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

const PAGES: Record<EntityType, string> = {
  food: "app/(everything-else)/food/[slug]/page.tsx",
  chemical: "app/(everything-else)/chemical/[slug]/page.tsx",
  disease: "app/(everything-else)/disease/[slug]/page.tsx",
  bioactivity: "app/(everything-else)/bioactivity/[slug]/page.tsx",
};

const source = (entity: EntityType) =>
  readFileSync(join(__dirname, "..", PAGES[entity]), "utf8");

// The buildTabs literal, sliced per tab so a `count:` belonging to a
// neighbouring tab can't satisfy the wrong one.
const tabBlock = (src: string, id: string): string | null => {
  const key = new RegExp(`^\\s*"?${id}"?:\\s*\\{`, "m");
  const start = src.search(key);
  if (start === -1) return null;
  const rest = src.slice(start);
  // Up to the next sibling key at the same nesting, or the block's end.
  const next = rest.slice(1).search(/^\s{10}"?[a-z-]+"?:\s*\{/m);
  return next === -1 ? rest : rest.slice(0, next + 1);
};

describe("tab count coverage", () => {
  for (const entity of Object.keys(ENTITY_TABS) as EntityType[]) {
    const counted = ENTITY_TABS[entity].filter((t) => t.hasCount);

    for (const tab of counted) {
      it(`${entity} page prefetches a count for "${tab.id}"`, () => {
        const block = tabBlock(source(entity), tab.id);
        expect(block, `${entity}: no "${tab.id}" entry in buildTabs`).not.toBe(
          null
        );
        expect(
          /\bcount:/.test(block!),
          `${entity} page passes no count for "${tab.id}" — its badge will ` +
            `pulse until the tab is opened`
        ).toBe(true);
      });
    }

    it(`${entity} page passes no count for uncounted tabs`, () => {
      // The mirror: a count on a hasCount:false tab renders nothing and is
      // a wasted request.
      const src = source(entity);
      for (const tab of ENTITY_TABS[entity].filter((t) => !t.hasCount)) {
        const block = tabBlock(src, tab.id);
        if (!block) continue;
        expect(
          /\bcount:/.test(block),
          `${entity}: "${tab.id}" is hasCount:false but is given a count`
        ).toBe(false);
      }
    });
  }
});

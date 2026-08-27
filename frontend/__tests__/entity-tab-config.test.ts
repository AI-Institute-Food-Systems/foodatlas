import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  DEFAULT_TAB_ID,
  ENTITY_TABS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

const ENTITY_TYPES = Object.keys(ENTITY_TABS) as EntityType[];
const ROUTES_DIR = join(process.cwd(), "app", "(everything-else)");

describe("entityTabs.config", () => {
  it("covers every entity type", () => {
    expect(ENTITY_TYPES.sort()).toEqual([
      "bioactivity",
      "chemical",
      "disease",
      "food",
    ]);
  });

  it.each(ENTITY_TYPES)("gives %s a default tab that exists", (entity) => {
    const ids = ENTITY_TABS[entity].map((t) => t.id);
    expect(ids).toContain(DEFAULT_TAB_ID[entity]);
  });

  it.each(ENTITY_TYPES)("gives %s unique tab ids", (entity) => {
    const ids = ENTITY_TABS[entity].map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it.each(ENTITY_TYPES)("gives %s exactly one uncounted tab", (entity) => {
    // Overview is metadata, not a collection, so it's the only tab that
    // never carries a badge. Everything else publishes a count, which is
    // what entitles it to a width-reserving placeholder while pending.
    const uncounted = ENTITY_TABS[entity].filter((t) => !t.hasCount);
    expect(uncounted.map((t) => t.id)).toEqual(["overview"]);
  });
});

describe("route loading shells", () => {
  const loadingFiles = readdirSync(ROUTES_DIR, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => join(ROUTES_DIR, d.name, "[slug]", "loading.tsx"))
    .filter((p) => {
      try {
        readFileSync(p);
        return true;
      } catch {
        return false;
      }
    });

  it("finds one per entity route", () => {
    expect(loadingFiles).toHaveLength(ENTITY_TYPES.length);
  });

  it.each(loadingFiles)("%s hard-codes no tab count", (file) => {
    // The original defect: loading.tsx guessed a tabCount that disagreed
    // with its page.tsx on three routes out of four, so the tab strip
    // visibly grew when the skeleton handed off to the SSR shell. Tab
    // counts must be derived from the config, never written down.
    expect(readFileSync(file, "utf8")).not.toMatch(/tabCount/);
  });
});

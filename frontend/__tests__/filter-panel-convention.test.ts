import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Source-scanning guard for the filter panels, in the same style as
// skeleton-convention.test.ts.
//
// This exists because "make the filter panels consistent" was requested
// three separate times and drifted back every time. At the point it was
// finally consolidated there were SEVEN implementations of the same facet
// row — FilterListItem, CheckRow, RadioRow, UnitRow, SourceKindRow, Chip
// pills, and one inlined directly in FoodBioactivitiesTab — plus four
// copies of the sidebar's positioning classes and four of the drawer.
//
// No unit test catches a NEW panel hand-rolling its own. This does.

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);

// Where the filter system itself is allowed to live.
const FILTERS_DIR = join(ROOT, "components", "entities", "shared", "filters");

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return SOURCE_EXT.has(extname(entry.name)) ? [full] : [];
  });

const SOURCES = SCAN_DIRS.flatMap(walk);
const rel = (f: string) => relative(ROOT, f);

// Modal owns an equivalent sidebar slot of its own, and has to: HeadlessUI
// dismisses on clicks outside the DialogPanel, so a modal's sidebar must be
// `absolute right-full` from INSIDE the panel rather than a flex sibling.
// FilterPanel's page sidebar cannot serve that case.
const MODAL_PRIMITIVE = join(ROOT, "components", "basic", "Modal.tsx");

const isShared = (f: string) =>
  f.startsWith(FILTERS_DIR) || f === MODAL_PRIMITIVE;

// Rules are about what the code *does*; prose must not trip them, since
// several of these files describe the convention in a comment.
const stripComments = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");

const code = (f: string) => stripComments(readFileSync(f, "utf8"));

describe("filter panel conventions", () => {
  it("finds the source tree", () => {
    expect(SOURCES.length).toBeGreaterThan(50);
  });

  it("only the shared module positions a filter sidebar", () => {
    // `absolute right-full` is the sidebar. Copying the class string was
    // never enough to copy the behaviour — it resolves against the nearest
    // positioned ancestor, so the chemical sidebar landed somewhere else
    // entirely while carrying byte-identical classes. FilterPanel owns the
    // wrapper precisely so a call site cannot get that half wrong.
    const offenders = SOURCES.filter(
      (f) => !isShared(f) && code(f).includes("absolute right-full")
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("only the shared module builds the filter drawer", () => {
    const offenders = SOURCES.filter(
      (f) =>
        !isShared(f) &&
        /w-\[85vw\]\s+max-w-sm/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("only the shared module renders a facet row", () => {
    // The row's distinctive shape: a 3.5-unit square/circle affordance
    // next to a full-width text button. Every one of the seven copies had
    // this exact geometry.
    const offenders = SOURCES.filter(
      (f) => !isShared(f) && /w-3\.5 h-3\.5[^"]*border/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("only the shared module renders a filter group label", () => {
    // FilterGroup's label. Panels that inlined this also tended to inline
    // their own clear link next to it.
    const offenders = SOURCES.filter(
      (f) =>
        !isShared(f) &&
        /uppercase tracking-wider text-light-400/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("clears filters through the shared control, never a bare link", () => {
    // A literal >clear< / >Clear filters< text node outside the shared
    // module means someone rebuilt the affordance by hand.
    const offenders = SOURCES.filter((f) => {
      if (isShared(f) || f.endsWith("ResetFiltersButton.tsx")) return false;
      return />\s*[Cc]lear(\s+filters)?\s*<\/(button|span)>/.test(code(f));
    });
    expect(offenders.map(rel)).toEqual([]);
  });

  it("every surface that filters offers a way to clear", () => {
    // A panel with filter groups but no reset is the specific defect that
    // shipped twice. FilterPanelBody renders the reset itself, so using
    // either it or FilterPanel satisfies this.
    const offenders = SOURCES.filter((f) => {
      if (isShared(f)) return false;
      const src = code(f);
      if (!src.includes("<FilterGroup")) return false;
      return !(
        src.includes("FilterPanel") || src.includes("FilterPanelBody")
      );
    });
    expect(offenders.map(rel)).toEqual([]);
  });
});

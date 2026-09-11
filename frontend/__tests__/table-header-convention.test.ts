import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Source-scanning guard for table headers, in the style of
// filter-panel-convention.test.ts: the header cell is the shared Th, and
// the sort affordance lives only there.
//
// Four tables had hand-rolled sortable headers and two had none, which
// is how the disease page's Chemicals tab shipped with nothing to click.
// The copies had also drifted: one table's arrows pointed the opposite
// way, and its first click on a number went smallest-first.

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);
const SHARED = join(ROOT, "components", "entities", "shared", "EvidenceTable.tsx");

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return SOURCE_EXT.has(extname(entry.name)) ? [full] : [];
  });

const SOURCES = SCAN_DIRS.flatMap(walk);
const rel = (f: string) => relative(ROOT, f);
const stripComments = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
const code = (f: string) => stripComments(readFileSync(f, "utf8"));

describe("table header conventions", () => {
  it("finds the source tree", () => {
    expect(SOURCES.length).toBeGreaterThan(50);
  });

  it("only the shared Th renders the sort affordance", () => {
    // MdUnfoldMore is the "sortable, not active" glyph. Anywhere else it
    // is a header being rebuilt by hand.
    const offenders = SOURCES.filter(
      (f) => f !== SHARED && /\bMdUnfoldMore\b/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("only the shared module builds the mobile sort control", () => {
    // The "sort" caption beside a SortListbox. Every card list that
    // sorts gets it from MobileSort, so it sits in the same place with
    // the same words on every table.
    const offenders = SOURCES.filter(
      (f) => f !== SHARED && /<SortListbox\b/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("does not put a bare <th> in an entity table", () => {
    // Entity tables render <Th>; a literal <th> in one is a header cell
    // that will not carry help or sort the way the others do. Scoped to
    // components/entities — the modals' small detail tables are a
    // different thing and keep their own markup.
    const offenders = SOURCES.filter((f) => {
      if (f === SHARED || !f.includes(join("components", "entities"))) {
        return false;
      }
      const src = code(f);
      return /<th[\s>]/.test(src) && /<Th\b/.test(src);
    });
    expect(offenders.map(rel)).toEqual([]);
  });
});

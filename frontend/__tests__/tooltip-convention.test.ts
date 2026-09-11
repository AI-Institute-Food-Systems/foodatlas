import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Source-scanning guard for hover info boxes, in the style of
// filter-panel-convention.test.ts: there is ONE tooltip, and every
// explanation goes through it. The alternatives it catches are the ones
// that actually shipped — a native `title` on an icon (the browser's box,
// not the app's), and a hand-assembled "i" that drifts from InfoTip.

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);
const TOOLTIP = join(ROOT, "components", "basic", "Tooltip.tsx");

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

describe("tooltip conventions", () => {
  it("finds the source tree", () => {
    expect(SOURCES.length).toBeGreaterThan(50);
  });

  it("only Tooltip.tsx renders a tooltip", () => {
    const offenders = SOURCES.filter(
      (f) => f !== TOOLTIP && /role=["']tooltip["']/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("never puts a native title on an icon", () => {
    // An icon with `title=` is a hover explanation rendered by the
    // browser — a different box, differently placed, from the one every
    // other "i" in the app opens. Wrap it in <Tooltip> instead.
    // (Native `title` on TEXT that truncates is fine and stays: revealing
    // clipped text is what the browser's tooltip is for.)
    const offenders = SOURCES.filter((f) =>
      /<Md[A-Z]\w*\b(?:(?!\/>)[\s\S])*?\btitle=/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });

  it("uses InfoTip for the 'i', never a hand-assembled one", () => {
    // <Tooltip><MdInfoOutline …/></Tooltip> outside Tooltip.tsx is InfoTip
    // rebuilt by hand, and the copies drift (size, colour, aria-label).
    const offenders = SOURCES.filter(
      (f) =>
        f !== TOOLTIP &&
        /<Tooltip\b[\s\S]*?<MdInfoOutline\b/.test(code(f)) &&
        // Only flag when the icon is the DIRECT child of the Tooltip, i.e.
        // within the same JSX expression with nothing but whitespace/props.
        /<Tooltip\b[^>]*>\s*<MdInfoOutline\b/.test(code(f))
    );
    expect(offenders.map(rel)).toEqual([]);
  });
});

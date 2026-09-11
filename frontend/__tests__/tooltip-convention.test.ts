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

  it("uses a native title only to reveal clipped text", () => {
    // Every `title=` in the tree is one of exactly three things:
    //   - a <Modal title=…> — a heading prop, not a hover;
    //   - on an element whose tag carries `truncate` — the browser's
    //     tooltip revealing text the layout clipped, which is its job;
    //   - FilterOption's row title in FilterControls.tsx — the same
    //     reveal, on the row so the tick and count show it too.
    // Anything else is an explanation rendered by the browser: a
    // different box, differently placed, from the one every "i" in the
    // app opens. Wrap it in <Tooltip> instead. Structural rather than a
    // list of files, so a new `title="What this column means"` on a
    // <th> or an icon fails here rather than shipping.
    const FILTER_CONTROLS = join(
      ROOT, "components", "entities", "shared", "filters", "FilterControls.tsx"
    );
    const offenders: string[] = [];
    for (const f of SOURCES) {
      if (f === TOOLTIP || f === FILTER_CONTROLS) continue;
      const src = code(f);
      const re = /\btitle=/g;
      let m: RegExpExecArray | null;
      while ((m = re.exec(src)) !== null) {
        const tagStart = src.lastIndexOf("<", m.index);
        const tag = src.slice(tagStart, m.index);
        const name = /^<([A-Za-z][\w.]*)/.exec(tag)?.[1] ?? "";
        if (name === "Modal" || /\btruncate\b/.test(tag)) continue;
        const line = src.slice(0, m.index).split("\n").length;
        offenders.push(`${rel(f)}:${line} <${name}>`);
      }
    }
    expect(offenders).toEqual([]);
  });

  it("never gives tooltip content a width of its own", () => {
    // The bubble sets the width. A `w-…` on the content is either
    // narrower than the bubble (pointless) or wider (runs past the
    // background, which the Efficacy help did at w-[28rem]). Scans the
    // value of every `content={…}` / `help={…}` prop.
    const offenders: string[] = [];
    for (const f of SOURCES) {
      if (f === TOOLTIP) continue;
      const src = code(f);
      const re = /\b(content|help)=\{/g;
      let m: RegExpExecArray | null;
      while ((m = re.exec(src)) !== null) {
        // The prop value: balance the braces from the opening one.
        let depth = 0;
        let i = m.index + m[0].length - 1;
        for (; i < src.length; i += 1) {
          if (src[i] === "{") depth += 1;
          else if (src[i] === "}" && --depth === 0) break;
        }
        const value = src.slice(m.index, i + 1);
        // The width class may be first in the list or after whitespace.
        if (/className="(?:[^"]*\s)?(?:min-)?w-(?:\[|\d|px|full|screen)/.test(value)) {
          const line = src.slice(0, m.index).split("\n").length;
          offenders.push(`${rel(f)}:${line}`);
        }
      }
    }
    expect(offenders).toEqual([]);
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

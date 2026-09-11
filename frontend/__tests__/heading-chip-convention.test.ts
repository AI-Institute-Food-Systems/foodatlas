import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Source-scanning guard for section labels, in the style of the other
// convention tests: on entity pages and in modals, a chip-styled heading
// is <Heading variant="chip"> and nothing else.
//
// The same label read three ways: four sections used the Heading, four
// had its classes copied into a <span>, and every modal headline was a
// larger, capitalised "boxed" variant that nothing else used. The
// landing page's card-catalog chips are decorative labels above a real
// <h3> and are deliberately out of scope — a heading element there
// would be a second heading per card.

const ROOT = process.cwd();
const HEADING = join(ROOT, "components", "basic", "Heading.tsx");
const SCOPE = [
  join(ROOT, "components", "entities"),
  join(ROOT, "components", "basic", "Modal.tsx"),
];

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return [".ts", ".tsx"].includes(extname(entry.name)) ? [full] : [];
  });
const strip = (s: string) =>
  s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
const sources = SCOPE.flatMap((p) => (p.endsWith(".tsx") ? [p] : walk(p)));

describe("heading chip conventions", () => {
  it("finds the scope", () => {
    expect(sources.length).toBeGreaterThan(20);
  });

  it("only Heading renders the chip", () => {
    // The chip's distinctive recipe: the cream fill with the inset glow.
    const offenders = sources.filter(
      (f) =>
        f !== HEADING &&
        /bg-light-200\s+shadow-inner\s+shadow-light-50/.test(
          strip(readFileSync(f, "utf8"))
        )
    );
    expect(offenders.map((f) => relative(ROOT, f))).toEqual([]);
  });

  it("has no second chip variant to drift to", () => {
    // "boxed" was the modal's private cousin. If a variant comes back,
    // it comes back on purpose and with a test edit.
    expect(strip(readFileSync(HEADING, "utf8"))).not.toMatch(/\bboxed\b/);
  });
});

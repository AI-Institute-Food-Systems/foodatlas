import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// Source-scanning guard. The whole point of this branch was that
// skeletons had no shared contract and every call site invented its own
// fill, radius and animation — so the thing most worth locking down is
// that they can't diverge again. Unit tests can't catch a NEW component
// hand-rolling its own placeholder; this can.
//
// Same technique as report-coverage.test.tsx, which is the only mechanism
// in this repo that has actually held a convention in place.

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);

// Where the skeleton system itself is allowed to live.
const TOKENS_FILE = join(ROOT, "components", "basic", "skeletonTokens.ts");

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return SOURCE_EXT.has(extname(entry.name)) ? [full] : [];
  });

const SOURCES = SCAN_DIRS.flatMap(walk);
const rel = (f: string) => relative(ROOT, f);

// These rules are about what the code *does*, so prose must not trip
// them — several of these files explain the convention in a comment and
// would otherwise report themselves.
const stripComments = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");

const code = (f: string) => stripComments(readFileSync(f, "utf8"));

describe("skeleton conventions", () => {
  it("finds the source tree", () => {
    expect(SOURCES.length).toBeGreaterThan(50);
  });

  it("declares the pulse animation in exactly one place", () => {
    // A second `animate-pulse` somewhere is how the four-fills problem
    // started. Note this bans the opacity-based Tailwind default
    // outright: on the light-800 resting fill it bottoms out near 1.06:1
    // against a Card, i.e. invisible for half of every cycle.
    const offenders = SOURCES.filter(
      (f) => f !== TOKENS_FILE && code(f).includes("animate-pulse")
    );

    expect(offenders.map(rel)).toEqual([]);
  });

  it("routes every placeholder through the Skeleton primitive", () => {
    // LoadingCard was the old passthrough; it's gone, and nothing should
    // reintroduce an import of it.
    const offenders = SOURCES.filter((f) => code(f).includes("basic/LoadingCard"));

    expect(offenders.map(rel)).toEqual([]);
  });

  it("keeps skeleton fills out of call sites", () => {
    // Tone belongs to skeletonTokens. A literal fill next to an
    // animation is a hand-rolled placeholder by another name.
    const offenders = SOURCES.filter(
      (f) =>
        f !== TOKENS_FILE &&
        /bg-light-(200|700|800|950)\/\d+[^"'`]*animate-/.test(code(f))
    );

    expect(offenders.map(rel)).toEqual([]);
  });
});

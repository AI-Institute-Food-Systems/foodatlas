// HeaderSection and HeaderSectionSuspense must lay out identically.
//
// The skeleton is what every entity route renders first — both as the
// Suspense fallback in page.tsx and, via EntityLoadingShell, as the route
// loading.tsx. If it disagrees with the real header about the header's
// height, the page moves when one replaces the other, and everything below
// (tab strip, tables) moves with it.
//
// This has gone wrong three times. The ambiguity banner hung below the band
// in the real header only, so ambiguous pages inserted a block on handoff.
// The name placeholder was h-9/h-10 against an H1 measured at 1.875rem
// below md. And md:h-9 was 4px SHORT from md up, because `leading-none` is
// unprefixed and the responsive md:text-4xl's own line-height wins there —
// so the class list said 36px and the browser rendered 40.
//
// There are TWO such pairs, not one, and covering only the header is how
// a 16px shift shipped: EntityDetailLayout moved from mt-6 to mt-10 while
// EntityDetailLayoutSuspense stayed behind, so every entity route stepped
// down when the loading shell handed off. Both pairs are checked here.
//
// Static, because HeaderSection is an async Server Component: rendering it
// would mean standing up the data layer to compare two boxes.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = process.cwd();
const read = (f: string) =>
  readFileSync(join(ROOT, "components", "entities", f), "utf8");

const REAL = read("HeaderSection.tsx");
const SKELETON = read("HeaderSectionSuspense.tsx");

const stripComments = (src: string) =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");

// Every className literal in the file, in source order.
const classNames = (src: string): string[] =>
  Array.from(stripComments(src).matchAll(/className="([^"]+)"/g)).map(
    (m) => m[1]
  );

// The wrapper each pair opens with. These set where everything below them
// starts, so a mismatch moves the entire page, not just its own box.
const OUTER_MARGIN = /<div className="(m[tbxy]-[^"]*)"/;

describe("layout/skeleton pairs agree on their outer margin", () => {
  const PAIRS: [string, string][] = [
    ["HeaderSection.tsx", "HeaderSectionSuspense.tsx"],
    ["EntityDetailLayout.tsx", "EntityDetailLayoutSuspense.tsx"],
  ];

  for (const [real, skeleton] of PAIRS) {
    it(`${real} matches ${skeleton}`, () => {
      const margin = (f: string) =>
        stripComments(read(f)).match(OUTER_MARGIN)?.[1];
      expect(margin(real), `${real} has no outer margin to compare`).toBeDefined();
      expect(margin(real)).toBe(margin(skeleton));
    });
  }
});

// The rows that set the header's height: the outer split, the badge line
// the ambiguity affordance rides on, and the name line. Declared once — a
// second copy of these literals is how the "no third block" check below
// silently stopped matching when the name row's margin changed.
const OUTER_ROW = "flex items-center justify-between";
const NAME_COLUMN = "flex flex-col gap-0";
const BADGE_ROW = "flex items-center gap-2";
const NAME_ROW = "mt-3 flex items-center gap-3 flex-wrap";
const ID_ROW = "flex items-baseline gap-1.5 whitespace-nowrap";
const RIGHT_STACK = "flex flex-col items-end gap-2";

describe("header skeleton parity", () => {
  it("uses the same row structure in both", () => {
    for (const layout of [
      OUTER_ROW,
      NAME_COLUMN,
      BADGE_ROW,
      NAME_ROW,
      ID_ROW,
      RIGHT_STACK,
    ]) {
      expect(classNames(REAL), `real header missing: ${layout}`).toContain(
        layout
      );
      expect(classNames(SKELETON), `skeleton missing: ${layout}`).toContain(
        layout
      );
    }
  });

  it("sizes the entity badge the same in both", () => {
    // The badge sets the top row's height, so a size that differs between
    // the two moves the page on handoff just as a wrong name placeholder
    // would — and it is a one-word edit that looks harmless.
    const badgeSize = (src: string) =>
      stripComments(src).match(/size="(xs|sm|md|lg)"/)?.[1];
    expect(badgeSize(REAL)).toBe(badgeSize(SKELETON));
  });

  it("reserves exactly the H1's height for the name", () => {
    // Measured in the browser, not derived from the class names: the H1
    // is 30px below md and 40px from md up. `leading-none` is unprefixed,
    // so at md the responsive `md:text-4xl` and its line-height: 2.5rem
    // override it — the "2.25rem at leading-none" this test used to
    // assume never renders, and reserving it left the strip 4px low.
    const h1 = classNames(REAL).find((c) => c.includes("text-3xl"));
    expect(h1).toContain("md:text-4xl");
    expect(h1).toContain("leading-none");

    const placeholder = classNames(SKELETON).find((c) => c.startsWith("h-["));
    expect(placeholder).toBe("h-[1.875rem] md:h-10 w-56");
  });

  it("puts the ambiguity affordance on the badge's line", () => {
    // The skeleton paints before the metadata that says whether this
    // entity is ambiguous, so it cannot reserve a box for the affordance.
    // The affordance is therefore only free if it rides a line something
    // else already sizes — the Badge's. Anywhere else and ambiguous pages
    // grow at handoff, which is the bug this whole pair exists to stop.
    const body = stripComments(REAL);
    const badgeLine = body.indexOf(`className="${BADGE_ROW}"`);
    const nameLine = body.indexOf(`className="${NAME_ROW}"`);
    const affordance = body.indexOf("<EntityAmbiguityBadge");
    expect(badgeLine, "badge line not found").toBeGreaterThan(-1);
    expect(affordance, "ambiguity affordance not rendered").toBeGreaterThan(
      badgeLine
    );
    expect(affordance, "affordance escaped the badge line").toBeLessThan(
      nameLine
    );
    // And the skeleton must not try to reserve one anyway — a placeholder
    // where the real header has inline text is its own mismatch.
    expect(stripComments(SKELETON)).not.toMatch(/min-h-\[/);
  });

  it("keeps the real header free of blocks the skeleton has no room for", () => {
    // Both files are two lines in a column beside the id. Anything that
    // renders as a third sibling adds height the skeleton does not have.
    const body = stripComments(REAL);
    const rows = classNames(REAL).filter(
      (c) => c === BADGE_ROW || c === NAME_ROW
    );
    expect(rows).toHaveLength(2);
    expect(body).not.toMatch(/role="note"/);
  });
});

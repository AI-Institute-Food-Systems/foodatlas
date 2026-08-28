// HeaderSection and HeaderSectionSuspense must lay out identically.
//
// The skeleton is what every entity route renders first — both as the
// Suspense fallback in page.tsx and, via EntityLoadingShell, as the route
// loading.tsx. If it disagrees with the real header about the header's
// height, the page moves when one replaces the other, and everything below
// (tab strip, tables) moves with it.
//
// This has gone wrong twice. The ambiguity banner hung below the band in
// the real header only, so ambiguous pages inserted a block on handoff.
// And the name placeholder was h-9/h-10 against an H1 that renders
// 1.875rem/2.25rem at leading-none — 6px too tall at both breakpoints.
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

// The two rows that set the header's height: the badge/id line and the
// name line. Declared once — a second copy of these literals is how the
// "no third block" check below silently stopped matching when the name
// row's margin changed.
const BADGE_ROW = "flex items-start justify-between gap-4";
const NAME_ROW = "mt-3 flex items-center gap-3 flex-wrap";
const ID_ROW = "flex items-baseline gap-1.5 whitespace-nowrap";
// The right-hand stack, and the always-present slot the ambiguity chip
// sits in. The slot is the reason an ambiguous page is the same height as
// a plain one.
const RIGHT_STACK = "flex flex-col items-end gap-1";
const CHIP_SLOT = "min-h-[1.25rem] flex items-center";

describe("header skeleton parity", () => {
  it("uses the same row structure in both", () => {
    for (const layout of [BADGE_ROW, NAME_ROW, ID_ROW, RIGHT_STACK, CHIP_SLOT]) {
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
    // text-3xl/text-4xl at leading-none is 1.875rem then 2.25rem. The
    // placeholder has to be those numbers, not the nearest h-* step.
    const h1 = classNames(REAL).find((c) => c.includes("text-3xl"));
    expect(h1).toContain("md:text-4xl");
    expect(h1).toContain("leading-none");

    const placeholder = classNames(SKELETON).find((c) => c.startsWith("h-["));
    expect(placeholder).toBe("h-[1.875rem] md:h-9 w-56");
  });

  it("reserves the ambiguity chip's line whether or not there is one", () => {
    // The skeleton paints before the metadata that says whether this
    // entity is ambiguous, so the slot cannot be conditional — a chip
    // that appears only on some pages would grow the top row on exactly
    // those pages, at handoff, which is what moving it here risked.
    const slots = (src: string) =>
      classNames(src).filter((c) => c === CHIP_SLOT).length;
    expect(slots(REAL)).toBe(1);
    expect(slots(SKELETON)).toBe(1);
  });

  it("keeps the real header free of blocks the skeleton has no room for", () => {
    // The ambiguity affordance is a chip inside the name row, which the
    // skeleton already sizes. Anything that renders as a sibling of the
    // two rows would add height the skeleton does not reserve.
    const body = stripComments(REAL);
    const rows = classNames(REAL).filter(
      (c) => c === BADGE_ROW || c === NAME_ROW
    );
    expect(rows).toHaveLength(2);
    expect(body).not.toMatch(/role="note"/);
  });
});

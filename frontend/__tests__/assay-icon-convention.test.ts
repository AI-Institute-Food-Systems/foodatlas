// Source guard: every assay button wears the assay icon.
//
// In the same style as skeleton-convention and filter-panel-convention,
// and for the same reason — this drifted once already. Assay buttons live
// in five files across three tabs, and three of them had settled on
// MdDescription, the document icon that belongs to publications and
// composition data points. No unit test catches that: each chip renders
// fine, the label is right, and the wrong glyph only reads as wrong when
// you see two of them side by side. On the merged evidence tab you do —
// "See 12 publications" and "See 3 assays" sit one column apart, and
// giving them the same icon undoes the distinction the tab exists to make.
//
// Two rules:
//   1. Nothing outside AssayIcon.tsx imports MdBiotech directly.
//   2. No Chip whose label mentions assays uses a different icon.

import { readdirSync, readFileSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);
const ICON_FILE = join(ROOT, "components", "icons", "AssayIcon.tsx");

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return SOURCE_EXT.has(extname(entry.name)) ? [full] : [];
  });

const SOURCES = SCAN_DIRS.flatMap(walk);
const rel = (f: string) => relative(ROOT, f);

// Prose must not trip the rules — several of these files describe the
// convention in a comment.
const stripComments = (src: string): string =>
  src
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/^\s*\/\/.*$/gm, "");

describe("assay icon convention", () => {
  it("only AssayIcon.tsx imports MdBiotech", () => {
    const offenders = SOURCES.filter(
      (f) =>
        f !== ICON_FILE && /\bMdBiotech\b/.test(stripComments(readFileSync(f, "utf8")))
    ).map(rel);

    expect(
      offenders,
      `Import AssayIcon from "@/components/icons/AssayIcon" instead of ` +
        `reaching for MdBiotech:\n${offenders.join("\n")}`
    ).toEqual([]);
  });

  it("every Chip labelled with assays uses AssayIcon", () => {
    // Chip renders `icon={...}` then `label={...}` within a few lines of
    // each other; pair them up and check any label mentioning assays.
    const offenders: string[] = [];

    for (const file of SOURCES) {
      const src = stripComments(readFileSync(file, "utf8"));
      // Array.from, not direct iteration: the tsconfig target predates
      // iterating a RegExpStringIterator.
      const chips = Array.from(
        src.matchAll(
          /icon=\{([\s\S]{0,120}?)\}\s*\n\s*label=\{?([\s\S]{0,200}?)(?:\}\s*\n|\n)/g
        )
      );
      for (const chip of chips) {
        const [, icon, label] = chip;
        if (!/assay/i.test(label)) continue;
        if (/AssayIcon/.test(icon)) continue;
        offenders.push(`${rel(file)} — icon={${icon.trim()}}`);
      }
    }

    expect(
      offenders,
      `These buttons count or open assays but wear another icon:\n${offenders.join("\n")}`
    ).toEqual([]);
  });

  it("finds the assay buttons it is meant to be guarding", () => {
    // A regex guard that matches nothing passes forever. Pin that the
    // sweep actually reaches the known assay buttons.
    const users = SOURCES.filter((f) =>
      /AssayIcon/.test(readFileSync(f, "utf8"))
    ).map(rel);

    expect(users).toContain("components/entities/bioactivity/BioactivityTable.tsx");
    expect(users).toContain(
      "components/entities/bioactivity/FoodInferredBioactivitiesSection.tsx"
    );
    expect(users).toContain(
      "components/entities/shared/AssayDetailModals.tsx"
    );
  });
});

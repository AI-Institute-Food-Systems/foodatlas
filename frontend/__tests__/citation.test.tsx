import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import About from "@/app/(everything-else)/about/page";
import Developers from "@/app/(everything-else)/developers/page";
import Citation from "@/components/basic/Citation";
import {
  CANONICAL_PUBLICATION,
  PUBLICATIONS,
  doiUrl,
} from "@/utils/publications";

// Two guards. The render tests prove the pages agree today; the
// source scan proves a fourth page can't quietly hand-roll copy #4.
// The npj reference used to be retyped in three files, and all three
// had drifted from the published record in the same two ways.

const ROOT = process.cwd();
const SCAN_DIRS = [join(ROOT, "components"), join(ROOT, "app")];
const SOURCE_EXT = new Set([".ts", ".tsx"]);

const walk = (dir: string): string[] =>
  readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return SOURCE_EXT.has(extname(entry.name)) ? [full] : [];
  });

const SOURCES = SCAN_DIRS.flatMap(walk);
const rel = (f: string) => relative(ROOT, f);

// Prose must not trip the rules — Citation.tsx explains the convention
// in a comment and would otherwise report itself.
const stripComments = (src: string): string =>
  src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");

const code = (f: string) => stripComments(readFileSync(f, "utf8"));

describe("citation conventions", () => {
  it("finds the source tree", () => {
    expect(SOURCES.length).toBeGreaterThan(50);
  });

  it("hardcodes no DOI outside utils/publications.ts", () => {
    const offenders = SOURCES.filter((f) => /doi\.org\/10\./.test(code(f)));
    expect(offenders.map(rel)).toEqual([]);
  });

  it("hardcodes no author list outside utils/publications.ts", () => {
    const offenders = SOURCES.filter((f) => /Tagkopoulos, I\./.test(code(f)));
    expect(offenders.map(rel)).toEqual([]);
  });
});

describe("Citation", () => {
  it("renders the corrected npj record in full", () => {
    render(<Citation publication={CANONICAL_PUBLICATION} />);

    // Gunning and Ni were dropped by the old "..." elision, and the
    // volume/issue/article number were missing entirely.
    const text = document.body.textContent ?? "";
    expect(text).toContain("Gunning, M.");
    expect(text).toContain("Ni, K.");
    expect(text).toContain("(2026)");
    expect(text).toContain("npj Science of Food, 10(1), 33.");
    expect(text).not.toContain("...");
  });

  it("prefixes proceedings venues with In", () => {
    const workshop = PUBLICATIONS.find((p) => p.kind === "proceedings");
    expect(workshop).toBeDefined();
    render(<Citation publication={workshop!} />);

    expect(document.body.textContent).toContain(
      "In 2nd AAAI Workshop on AI for Agriculture and Food Systems.",
    );
  });

  it("omits the issue when the record has none", () => {
    const cbm = PUBLICATIONS.find((p) => p.doi.startsWith("10.1016/"));
    expect(cbm).toBeDefined();
    render(<Citation publication={cbm!} />);

    expect(document.body.textContent).toContain(
      "Computers in Biology and Medicine, 181, 109072.",
    );
  });
});

describe("citation surfaces", () => {
  const canonicalHref = doiUrl(CANONICAL_PUBLICATION.doi);

  it("cites the canonical paper on /developers", () => {
    render(<Developers />);

    expect(screen.getByRole("heading", { name: /how to cite/i })).toBeTruthy();
    expect(
      screen.getAllByRole("link", { name: new RegExp(canonicalHref) }).length,
    ).toBeGreaterThan(0);
  });

  it("lists every publication on /about, canonical first", () => {
    render(<About />);

    const items = screen.getAllByRole("listitem");
    const cited = items.filter((li) =>
      li.textContent?.includes("https://doi.org/"),
    );
    expect(cited).toHaveLength(PUBLICATIONS.length);
    expect(cited[0].textContent).toContain(canonicalHref);

    for (const publication of PUBLICATIONS) {
      expect(
        cited.some((li) => li.textContent?.includes(doiUrl(publication.doi))),
      ).toBe(true);
    }
  });
});

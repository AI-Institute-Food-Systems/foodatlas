// Structural guard: every table that renders data rows must be reportable.
//
// "Report an issue" is wired per-component, so a new table simply forgets it —
// which is what happened to the assay-inferred tables and the disease
// Bioactivities table. Nothing failed; the rows just weren't reportable, and
// only manual testing caught it.
//
// This scans source rather than rendering, because rendering each table needs
// its own fetch mocks, providers and fixtures — the kind of setup that gets
// skipped for a new component, i.e. exactly when this check has to fire.

import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

const ENTITIES_DIR = path.join(process.cwd(), "components", "entities");

// Renders rows from a collection but is not itself the row surface: the
// wrapper delegates to a child table that carries the reporting.
const DELEGATES_TO_CHILD = new Set<string>([
  // Renders <DiseaseBioactivityTable/>, which is wired.
  "disease/DiseaseBioactivitiesSection.tsx",
  // Owns the <tbody> but builds each <tr> from
  // <ChemicalCompositionRow/>, which spreads the getRowProps this
  // component computes per row.
  "chemical/ChemicalCompositionTable.tsx",
]);

// Loading skeletons render placeholder <tbody> rows that hold no data, so
// there is nothing to report. The repo names them *Suspense.tsx by convention.
const isSkeleton = (rel: string) => rel.endsWith("Suspense.tsx");

const walk = (dir: string): string[] =>
  fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return entry.isFile() && entry.name.endsWith(".tsx") ? [full] : [];
  });

describe("report-an-issue coverage", () => {
  const files = walk(ENTITIES_DIR);

  it("finds component files to check", () => {
    expect(files.length).toBeGreaterThan(0);
  });

  it.each(
    files.map((file) => [path.relative(ENTITIES_DIR, file), file] as const),
  )("%s renders rows reportably", (rel, file) => {
    const source = fs.readFileSync(file, "utf8");

    // Only components that render their own row bodies are in scope.
    const rendersRows = source.includes("<tbody");
    if (!rendersRows || DELEGATES_TO_CHILD.has(rel) || isSkeleton(rel)) return;

    expect(
      source.includes("getRowProps"),
      `${rel} renders <tbody> rows but never calls getRowProps, so its rows ` +
        "cannot be reported. Wire useReportRows() and spread " +
        "reporter.getRowProps({...}) onto each row, or add the file to " +
        "DELEGATES_TO_CHILD if a child component carries the reporting.",
    ).toBe(true);
  });
});

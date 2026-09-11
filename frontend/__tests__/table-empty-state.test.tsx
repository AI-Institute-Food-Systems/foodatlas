// "Nothing here" looks the same on every entity table.
//
// Five tables rendered the icon-and-sentence block; three returned a bare
// italic paragraph — so "olefinic compound is not recorded in any food"
// on a chemical's Foods tab sat in a different typeface, colour and place
// from "No evidence found" on the same page's Diseases tab.

import { readFileSync, readdirSync } from "node:fs";
import { extname, join, relative } from "node:path";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import TableEmptyState from "@/components/entities/shared/TableEmptyState";

vi.mock("@/utils/fetching", () => ({
  getChemicalCompositionData: vi.fn().mockResolvedValue({
    with_concentrations: [],
    without_concentrations: [],
  }),
  getMetaData: vi.fn().mockResolvedValue({ id: "c1" }),
}));

describe("TableEmptyState", () => {
  it("is the icon-and-sentence block, table-height", () => {
    const { container } = render(<TableEmptyState>No evidence found</TableEmptyState>);
    const block = container.firstElementChild as HTMLElement;
    expect(block.className).toContain("h-[10rem]");
    expect(block.querySelector("svg")).toBeTruthy();
    expect(screen.getByText("No evidence found")).toBeInTheDocument();
    expect(screen.queryByText(/clear filters/i)).toBeNull();
  });

  it("offers the way out when the filters are what hid the rows", () => {
    const onClear = vi.fn();
    render(
      <TableEmptyState onClearFilters={onClear}>
        No foods match these filters
      </TableEmptyState>
    );
    screen.getByText(/clear filters/i).click();
    expect(onClear).toHaveBeenCalled();
  });

  it("reads as an error when asked to", () => {
    const { container } = render(
      <TableEmptyState error>An error occurred</TableEmptyState>
    );
    expect(container.querySelector(".text-red-400")).toBeTruthy();
  });
});

describe("the chemical Foods tab with no rows", () => {
  it("renders the shared block, not a bare paragraph", async () => {
    const { default: ChemicalCompositionSection } = await import(
      "@/components/entities/chemical/ChemicalCompositionSection"
    );
    // An async server component: await the element it resolves to.
    const element = await ChemicalCompositionSection({
      commonName: "olefinic compound",
    });
    const { container } = render(element);
    expect(container.querySelector("p.italic")).toBeNull();
    const block = container.firstElementChild as HTMLElement;
    expect(block.className).toContain("h-[10rem]");
    expect(block.textContent).toMatch(/not recorded in any food/i);
  });
});

// Source-scanning guard, in the style of the other convention tests.
describe("empty-state conventions", () => {
  const ROOT = process.cwd();
  const SHARED = join(ROOT, "components", "entities", "shared", "TableEmptyState.tsx");
  const walk = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
      const full = join(dir, e.name);
      if (e.isDirectory()) return walk(full);
      return [".ts", ".tsx"].includes(extname(e.name)) ? [full] : [];
    });
  const strip = (s: string) =>
    s.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
  const sources = walk(join(ROOT, "components", "entities")).filter(
    (f) => f !== SHARED
  );

  it("only the shared component renders the empty block", () => {
    // Its distinctive geometry: a 10rem-tall centred flex box.
    const offenders = sources.filter((f) =>
      /h-\[10rem\]\s+flex\s+items-center\s+justify-center/.test(
        strip(readFileSync(f, "utf8"))
      )
    );
    expect(offenders.map((f) => relative(ROOT, f))).toEqual([]);
  });

  it("never says 'nothing here' as a bare italic paragraph", () => {
    // The exact shape that shipped three times.
    const offenders = sources.filter((f) =>
      /<p\b[^>]*\bitalic\b[^>]*>\s*(?:\{[^}]*\}\s*)?(?:<[^>]+>[^<]*<\/[^>]+>\s*)?(?:No |is not recorded)/.test(
        strip(readFileSync(f, "utf8"))
      )
    );
    expect(offenders.map((f) => relative(ROOT, f))).toEqual([]);
  });
});

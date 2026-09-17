import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  TableSkeleton,
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import {
  TABLE_SKELETON_ROWS,
  TABLE_SKELETON_ROWS_MOBILE,
  type SkeletonColumn,
} from "@/components/basic/skeletonTokens";

const COLUMNS: SkeletonColumn[] = [
  { key: "name", width: "w-[40%]" },
  { key: "count", width: "w-[20%]", align: "right" },
  { key: "value", width: "w-[20%]", align: "right" },
  { key: "action", width: "w-[20%]" },
];

const renderRows = (ui: React.ReactElement) =>
  render(
    <table>
      <tbody>{ui}</tbody>
    </table>
  );

describe("TableSkeletonRows", () => {
  it("renders one placeholder per column, never a colSpan bar", () => {
    // The core of defect E: a single full-width colSpan slab has no
    // relationship to the real grid, so cells visibly re-slice on resolve.
    const { container } = renderRows(
      <TableSkeletonRows columns={COLUMNS} rows={3} />
    );

    expect(container.querySelectorAll("tr")).toHaveLength(3);
    expect(container.querySelectorAll("td")).toHaveLength(3 * COLUMNS.length);
    expect(container.querySelector("td[colspan]")).toBeNull();
  });

  it("defaults to the real first page's row count", () => {
    // Defect D: CorrelationTable used 10 while the shell used 20, so the
    // Health Impacts tab visibly shrank on handoff.
    const { container } = renderRows(<TableSkeletonRows columns={COLUMNS} />);
    expect(container.querySelectorAll("tr")).toHaveLength(TABLE_SKELETON_ROWS);
  });

  it("pins right-aligned columns to the right", () => {
    const { container } = renderRows(
      <TableSkeletonRows columns={COLUMNS} rows={1} />
    );
    const wrappers = Array.from(container.querySelectorAll("td > div"));

    expect(wrappers.map((w) => w.className.includes("justify-end"))).toEqual([
      false,
      true,
      true,
      false,
    ]);
  });

  it("mirrors the real cells' edge-flush padding rule", () => {
    // Real rows pad outer edges flush and interior columns symmetrically.
    // A uniform px-4 would sit the placeholder grid 16px off the real one.
    const { container } = renderRows(
      <TableSkeletonRows columns={COLUMNS} rows={1} />
    );
    const cells = Array.from(container.querySelectorAll("td"));

    expect(cells[0]).toHaveClass("pr-4");
    expect(cells[1]).toHaveClass("px-4");
    expect(cells[2]).toHaveClass("px-4");
    expect(cells[3]).toHaveClass("pl-4");
  });

  it("keeps row pitch identical to a loaded row", () => {
    const { container } = renderRows(
      <TableSkeletonRows columns={COLUMNS} rows={1} />
    );
    expect(container.querySelector("td")).toHaveClass("py-1.5");
    expect(container.querySelector("td > div")).toHaveClass("min-h-9");
  });
});

describe("TableSkeletonCards", () => {
  it("renders the mobile row count with a label:value pair per extra column", () => {
    const { container } = render(<TableSkeletonCards columns={COLUMNS} />);
    const cards = Array.from(container.firstElementChild?.children ?? []);

    expect(cards).toHaveLength(TABLE_SKELETON_ROWS_MOBILE);
    // Primary line + one label:value row per remaining column.
    expect(cards[0]?.children).toHaveLength(COLUMNS.length);
  });
});

describe("TableSkeleton", () => {
  it("announces loading once for the whole table", () => {
    const { getByRole } = render(<TableSkeleton columns={COLUMNS} rows={2} />);
    expect(getByRole("status")).toHaveAttribute("aria-busy", "true");
  });

  it("renders real header labels when the caller knows them", () => {
    // Keeping the header rule and column geometry stable across the
    // handoff is the whole point of passing them.
    const { getByText } = render(
      <TableSkeleton
        columns={COLUMNS}
        rows={1}
        headerLabels={["Name", "Count", "Value", ""]}
      />
    );
    expect(getByText("Name")).toBeInTheDocument();
    expect(getByText("Count")).toBeInTheDocument();
  });

  it("declares column widths so the grid does not reflow on resolve", () => {
    const { container } = render(<TableSkeleton columns={COLUMNS} rows={1} />);
    const cols = Array.from(container.querySelectorAll("col"));

    expect(cols.map((c) => c.className)).toEqual([
      "w-[40%]",
      "w-[20%]",
      "w-[20%]",
      "w-[20%]",
    ]);
  });
});

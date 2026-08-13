import { twMerge } from "tailwind-merge";

import Skeleton from "@/components/basic/Skeleton";
import {
  TABLE_SKELETON_ROWS,
  TABLE_SKELETON_ROWS_MOBILE,
  cellPadding,
  cellWidth,
  type SkeletonColumn,
} from "@/components/basic/skeletonTokens";

// Table loading states, generated from the same `columns[]` spec that
// renders the real table. Two rules are baked in here rather than left to
// each call site:
//
//   1. One placeholder PER COLUMN, never a `colSpan` bar. A full-width
//      slab has no relationship to the real grid, so the cells visibly
//      re-slice the moment data lands.
//   2. The skeleton replaces the <tbody> ONLY. Headings, descriptions,
//      filter chrome and <thead> are not derived from the fetch, so they
//      stay rendered and the section keeps its shape.

interface RowsProps {
  columns: SkeletonColumn[];
  rows?: number;
  // Overrides the default cell padding for tables that use their own
  // scheme (the measurements modal is tighter than the entity tables).
  // Merged last, so it wins over the default.
  cellClassName?: string;
}

// <tr> fragment for tables that already own their <table>/<colgroup>/
// <thead> — i.e. every real table in the app. Geometry mirrors
// BioactivityTableRow exactly (py-1.5, edge-flush outer padding, a
// min-h-9 flex wrapper) so row pitch is identical loading vs loaded.
export const TableSkeletonRows = ({
  columns,
  rows = TABLE_SKELETON_ROWS,
  cellClassName,
}: RowsProps) => (
  <>
    {Array.from({ length: rows }).map((_, r) => (
      <tr key={`skeleton-${r}`}>
        {columns.map((c, i) => (
          <td
            key={c.key}
            className={twMerge(
              "py-1.5",
              cellPadding(i, columns.length),
              cellClassName
            )}
          >
            <div
              className={twMerge(
                "flex min-h-9 items-center",
                c.align === "right" && "justify-end"
              )}
            >
              <Skeleton className={twMerge("h-5", cellWidth(r, i))} />
            </div>
          </td>
        ))}
      </tr>
    ))}
  </>
);

interface CardsProps {
  columns: SkeletonColumn[];
  rows?: number;
  className?: string;
}

// Mobile card list. Mirrors the real card shape the tables render at this
// breakpoint: a primary line standing in for columns[0] (typically the
// name link), then one label:value row per remaining column.
export const TableSkeletonCards = ({
  columns,
  rows = TABLE_SKELETON_ROWS_MOBILE,
  className,
}: CardsProps) => (
  <div
    className={twMerge(
      "md:hidden w-full flex flex-col divide-y divide-light-800",
      className
    )}
  >
    {Array.from({ length: rows }).map((_, r) => (
      <div key={`skeleton-${r}`} className="w-full py-3 flex flex-col gap-2">
        <Skeleton className="h-5 w-2/3" />
        {columns.slice(1).map((c) => (
          <div
            key={c.key}
            className="w-full flex items-center justify-between gap-2"
          >
            <Skeleton className="h-3 w-16" />
            <Skeleton className="h-4 w-24" />
          </div>
        ))}
      </div>
    ))}
  </div>
);

interface TableSkeletonProps {
  columns: SkeletonColumn[];
  rows?: number;
  mobileRows?: number;
  // Real header labels when they're known statically. Passing them keeps
  // the header rule and column geometry stable across the handoff; the
  // fallback bars are for callers that genuinely don't know their headers
  // yet (the pre-SSR route shell).
  headerLabels?: string[];
}

// Standalone: full table chrome + rows + the mobile card list. For the
// route loading shell and for sections that have no table of their own to
// hang <tr>s off.
export const TableSkeleton = ({
  columns,
  rows,
  mobileRows,
  headerLabels,
}: TableSkeletonProps) => (
  <div role="status" aria-busy="true">
    <span className="sr-only">Loading table</span>
    <div className="hidden md:block overflow-x-auto">
      <table className="w-full table-fixed">
        <colgroup>
          {columns.map((c) => (
            <col key={c.key} className={c.width} />
          ))}
        </colgroup>
        <thead className="text-light-400">
          <tr>
            {columns.map((c, i) => (
              <th
                key={c.key}
                className={twMerge(
                  "h-9 border-b border-light-700 py-1.5 uppercase text-xs font-medium",
                  cellPadding(i, columns.length),
                  c.align === "right" ? "text-right" : "text-left"
                )}
              >
                {headerLabels?.[i] ?? <Skeleton className="h-3 w-20" />}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          <TableSkeletonRows columns={columns} rows={rows} />
        </tbody>
      </table>
    </div>
    <TableSkeletonCards columns={columns} rows={mobileRows} />
  </div>
);

TableSkeleton.displayName = "TableSkeleton";

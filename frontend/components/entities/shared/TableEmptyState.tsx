"use client";

// THE "nothing here" block for every entity table.
//
// Three states, one look: no data for this entity at all; data that the
// current filters hide, with the way out beside it; or a fetch that
// failed. Five tables rendered the icon-and-sentence block and three
// returned a bare italic paragraph, so "olefinic compound is not
// recorded in any food" sat in a different typeface, colour and place
// from "No evidence found" two tabs over.
//
// The block is the table's height on an empty page (h-[10rem]) whether
// it sits in a <td colSpan> or stands in for the table entirely, so the
// page does not collapse and re-expand as data arrives.

import type { ReactNode } from "react";
import { MdErrorOutline, MdInfoOutline } from "react-icons/md";

import { ClearFiltersLink } from "@/components/entities/shared/filters/FilterControls";

export const TableEmptyState = ({
  children,
  onClearFilters,
  error = false,
}: {
  children: ReactNode;
  // Present ⇒ the rows exist and the filters hid them; offers the way out.
  onClearFilters?: () => void;
  // The fetch failed, rather than returned nothing.
  error?: boolean;
}) => (
  <div className="h-[10rem] flex items-center justify-center">
    {error ? (
      <div className="flex items-center gap-2 text-red-400 text-sm">
        <MdErrorOutline className="shrink-0" />
        <span>{children}</span>
      </div>
    ) : onClearFilters ? (
      <div className="flex flex-col items-center gap-2 text-light-300">
        <div className="flex items-center gap-2 text-sm">
          <MdInfoOutline className="shrink-0" />
          <span>{children}</span>
        </div>
        <ClearFiltersLink onClick={onClearFilters} />
      </div>
    ) : (
      <div className="flex items-center gap-2 text-light-300 text-sm">
        <MdInfoOutline className="shrink-0" />
        <span>{children}</span>
      </div>
    )}
  </div>
);

TableEmptyState.displayName = "TableEmptyState";
export default TableEmptyState;

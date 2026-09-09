"use client";

// "310 chemicals · 204 hidden by filters — Clear", above the composition table.
//
// The sidebar already shows a per-source count, but nothing on the page
// said how much of the food the current view is leaving out. That mattered
// once source filtering started actually working: deselecting FoodAtlas on
// pepper (raw) legitimately drops the table from 309 rows to 106, and
// without a line saying so the drop reads as missing data rather than as
// the filter doing its job.
//
// One combined line rather than a badge per dimension — the user's question
// is "am I seeing everything?", which is a single number.

import { ClearFiltersLink } from "@/components/entities/shared/filters/FilterControls";

interface Props {
  // Rows this food has before any filter (counts endpoint's total_row_count).
  totalRowCount?: number;
  // Rows the current filters leave (the table's metadata.total_rows).
  visibleRowCount: number;
  onClear: () => void;
}

const CompositionFilterSummary = ({
  totalRowCount,
  visibleRowCount,
  onClear,
}: Props) => {
  // Undefined while the counts request is in flight, or against an older
  // API build that doesn't send the field — render nothing rather than
  // flashing a wrong delta computed from a missing total.
  if (totalRowCount === undefined) return null;

  const hidden = totalRowCount - visibleRowCount;
  // Nothing hidden means nothing to say. Also guards the case where the
  // two numbers disagree in the other direction (a counts response that
  // raced ahead of the rows response), which would otherwise render a
  // nonsense negative.
  if (hidden <= 0) return null;

  return (
    <p className="pb-2 text-xs text-light-500">
      <span className="font-medium text-light-700">
        {totalRowCount.toLocaleString()} chemicals
      </span>
      {" · "}
      {hidden.toLocaleString()} hidden by filters
      {" — "}
      {/* The shared affordance, not a hand-rolled link: every other
        * clear-filters control in the app is this one, and
        * filter-panel-convention.test.ts enforces it. */}
      <ClearFiltersLink onClick={onClear} />
    </p>
  );
};

CompositionFilterSummary.displayName = "CompositionFilterSummary";
export default CompositionFilterSummary;

"use client";

// Shared "clear filters" affordance for every filter sidebar.
//
// Previously each sidebar inlined its own 11px italic text link, sitting
// ABOVE the filter groups — easy to miss, and in the wrong place: you go
// looking for it after working through the filters, not before them. This
// renders as a real bordered control at the BOTTOM of the panel instead,
// built on Chip so it speaks the same vocabulary as every other action.
//
// Still conditional on the filters being dirty: a permanently visible
// "clear" on an untouched panel is noise.

import { MdFilterAltOff } from "react-icons/md";

import Chip from "@/components/basic/Chip";

interface Props {
  // Render nothing when the view already matches a fresh page load.
  isDirty: boolean;
  onReset: () => void;
}

const ResetFiltersButton = ({ isDirty, onReset }: Props) => {
  if (!isDirty) return null;
  return (
    <div className="flex flex-col gap-2 pt-1">
      {/* Hairline above so the control reads as panel-level, not as part
       * of whichever filter group happens to sit last. */}
      <div className="border-t border-light-800" />
      <Chip
        icon={<MdFilterAltOff className="size-3" />}
        label="Clear filters"
        tone="outline"
        size="md"
        onClick={onReset}
        className="w-full justify-center"
        aria-label="Clear all filters"
      />
    </div>
  );
};

ResetFiltersButton.displayName = "ResetFiltersButton";
export default ResetFiltersButton;

"use client";

import { useState } from "react";
import { MdOutlineFlag } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import ReportIssueModal from "@/components/basic/ReportIssueModal";
import type { ReportContext } from "@/types/Report";

interface ReportIssueButtonProps {
  context: ReportContext;
  // Positioning hook — the button lives inline with other row actions,
  // so callers own margin/gap. Nothing else should be styled here.
  className?: string;
  // Accessible label override. Default is "Report an issue with this
  // data point" — most call sites want that; the exception is grouped
  // rows where the row label is already in the aria tree.
  ariaLabel?: string;
}

// One-primitive report trigger. Renders as an xs outline Chip carrying
// only a flag icon (no visible label — the tooltip carries the intent).
// Non-intrusive: ~18 px pill that sits inline with existing row
// affordances (View Paper, PMID chips, "N Assays" buttons).
const ReportIssueButton = ({
  context,
  className,
  ariaLabel = "Report an issue with this data point",
}: ReportIssueButtonProps) => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <>
      <Chip
        icon={<MdOutlineFlag className="size-3" aria-hidden />}
        label=""
        tone="outline"
        size="xs"
        onClick={(e) => {
          // Row-level triggers often sit inside <button> or clickable
          // rows; stop propagation so the click doesn't also open the
          // parent row's default action (e.g. the Assays modal).
          e.stopPropagation();
          setIsOpen(true);
        }}
        title="Report an issue"
        aria-label={ariaLabel}
        className={className}
      />
      <ReportIssueModal
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
        context={context}
      />
    </>
  );
};

ReportIssueButton.displayName = "ReportIssueButton";
export default ReportIssueButton;

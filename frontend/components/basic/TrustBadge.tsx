"use client";

import { MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Chip from "@/components/basic/Chip";
import Tooltip from "@/components/basic/Tooltip";

// Pill that surfaces low LLM-plausibility data points on a row in the
// composition table. Mirrors AmbiguityBadge: hidden when the count is zero,
// click-through opens the evidence modal pre-filtered to low-trust items.
interface TrustBadgeProps {
  lowTrustCount: number;
  totalCount: number;
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void;
  className?: string;
}

export const TrustBadge = ({
  lowTrustCount,
  totalCount,
  onClick,
  className,
}: TrustBadgeProps) => {
  if (lowTrustCount <= 0) return null;
  const allLow = lowTrustCount === totalCount;
  return (
    <Tooltip
      content={
        <span className="whitespace-normal">
          {allLow
            ? `All ${totalCount} data point${
                totalCount === 1 ? " has" : "s have"
              } low LLM-plausibility.`
            : `${lowTrustCount} of ${totalCount} data points ${
                lowTrustCount === 1 ? "has" : "have"
              } low LLM-plausibility.`}{" "}
          Click to review.
        </span>
      }
    >
      <Chip
        tone="rose"
        size="sm"
        icon={<MdWarningAmber className="size-3" />}
        label={lowTrustCount}
        onClick={onClick}
        className={twMerge(allLow && "bg-rose-500/20", className)}
        aria-label={`${lowTrustCount} low-trust data points`}
      />
    </Tooltip>
  );
};

TrustBadge.displayName = "TrustBadge";

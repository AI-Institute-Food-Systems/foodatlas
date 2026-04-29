"use client";

import { MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

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
      <button
        type="button"
        onClick={onClick}
        className={twMerge(
          "inline-flex items-center gap-1 rounded-full border px-1.5 py-[0.1rem] text-[0.6rem] font-mono font-medium transition-colors",
          allLow
            ? "text-rose-400 border-rose-500 bg-rose-500/15 hover:bg-rose-500/25"
            : "text-rose-400/90 border-rose-500/60 bg-rose-500/10 hover:bg-rose-500/20",
          className
        )}
      >
        <MdWarningAmber className="size-3" />
        {lowTrustCount}
      </button>
    </Tooltip>
  );
};

TrustBadge.displayName = "TrustBadge";

"use client";

import { MdCallSplit } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Tooltip from "@/components/basic/Tooltip";

export const isAmbiguous = (
  foodCandidates?: string[] | null,
  chemicalCandidates?: string[] | null
) =>
  (foodCandidates?.length ?? 0) > 1 || (chemicalCandidates?.length ?? 0) > 1;

interface AmbiguityTooltipContentProps {
  foodCandidates?: string[] | null;
  chemicalCandidates?: string[] | null;
}

const AmbiguityTooltipContent = ({
  foodCandidates,
  chemicalCandidates,
}: AmbiguityTooltipContentProps) => {
  const foodAmbiguous = (foodCandidates?.length ?? 0) > 1;
  const chemicalAmbiguous = (chemicalCandidates?.length ?? 0) > 1;
  return (
    <div className="flex flex-col gap-1.5 max-w-xs whitespace-normal">
      {foodAmbiguous && (
        <div>
          <span className="text-amber-400 font-semibold">Ambiguous food</span>
          <span className="text-light-300">
            {" "}— could also refer to:{" "}
          </span>
          <span className="capitalize">{foodCandidates!.join(", ")}</span>
        </div>
      )}
      {chemicalAmbiguous && (
        <div>
          <span className="text-amber-400 font-semibold">
            Ambiguous chemical
          </span>
          <span className="text-light-300">{" "}— could also be:{" "}</span>
          <span className="capitalize">{chemicalCandidates!.join(", ")}</span>
        </div>
      )}
    </div>
  );
};

interface AmbiguityIconProps {
  foodCandidates?: string[] | null;
  chemicalCandidates?: string[] | null;
  className?: string;
}

export const AmbiguityIcon = ({
  foodCandidates,
  chemicalCandidates,
  className,
}: AmbiguityIconProps) => {
  if (!isAmbiguous(foodCandidates, chemicalCandidates)) return null;
  return (
    <Tooltip
      content={
        <AmbiguityTooltipContent
          foodCandidates={foodCandidates}
          chemicalCandidates={chemicalCandidates}
        />
      }
    >
      <MdCallSplit
        aria-label="Ambiguous data point"
        className={twMerge("text-amber-500 size-4 rotate-90", className)}
      />
    </Tooltip>
  );
};

interface AmbiguityBadgeProps {
  ambiguousCount: number;
  totalCount: number;
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void;
  className?: string;
}

export const AmbiguityBadge = ({
  ambiguousCount,
  totalCount,
  onClick,
  className,
}: AmbiguityBadgeProps) => {
  if (ambiguousCount <= 0) return null;
  const fullyAmbiguous = ambiguousCount === totalCount;
  return (
    <Tooltip
      content={
        <span className="whitespace-normal">
          {fullyAmbiguous
            ? `All ${totalCount} data point${totalCount === 1 ? " is" : "s are"} ambiguous.`
            : `${ambiguousCount} of ${totalCount} data points ${ambiguousCount === 1 ? "is" : "are"} ambiguous.`}{" "}
          Click to review.
        </span>
      }
    >
      <button
        type="button"
        onClick={onClick}
        className={twMerge(
          "inline-flex items-center gap-1 rounded-full border px-1.5 py-[0.1rem] text-[0.6rem] font-mono font-medium transition-colors",
          fullyAmbiguous
            ? "text-amber-400 border-amber-500 bg-amber-500/15 hover:bg-amber-500/25"
            : "text-amber-400/90 border-amber-500/60 bg-amber-500/10 hover:bg-amber-500/20",
          className
        )}
      >
        <MdCallSplit className="size-3 rotate-90" />
        {ambiguousCount}
      </button>
    </Tooltip>
  );
};

AmbiguityIcon.displayName = "AmbiguityIcon";
AmbiguityBadge.displayName = "AmbiguityBadge";

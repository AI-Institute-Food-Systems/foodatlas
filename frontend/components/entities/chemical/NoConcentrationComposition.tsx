"use client";

import { useState } from "react";
import { MdInfoOutline, MdKeyboardArrowDown } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Link from "@/components/basic/Link";
import EntitySiblingIcon from "@/components/basic/EntitySiblingIcon";
import { useReportRows } from "@/context/reportModeContext";
import { AmbiguitySibling } from "@/types/Metadata";
import { encodeSpace } from "@/utils/utils";

interface NoConcentrationRow {
  id: string;
  name: string;
  evidence_count: number;
  ambiguity_siblings?: AmbiguitySibling[];
}

interface NoConcentrationCompositionProps {
  data: NoConcentrationRow[] | undefined | null;
  chemicalName?: string;
}

const NoConcentrationComposition = ({
  data,
  chemicalName,
}: NoConcentrationCompositionProps) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const reporter = useReportRows();

  return (
    <div>
      {data && data.length > 0 ? (
        <div className="flex flex-col gap-3">
          {/* collapsible header */}
          <button
            className="flex items-center gap-2 text-light-300 hover:text-light-100 transition duration-200 cursor-pointer"
            onClick={() => setIsExpanded((prev) => !prev)}
          >
            <MdKeyboardArrowDown
              className={`size-5 transition-transform duration-200 ${
                isExpanded ? "rotate-0" : "-rotate-90"
              }`}
            />
            <span className="text-sm">
              {data.length} food{data.length === 1 ? "" : "s"} with
              unknown concentration
            </span>
          </button>
          {/* expanded content — chips align with the synonyms vocabulary
           * elsewhere on the page so the visual language stays unified. */}
          {isExpanded && (
            <div className="flex flex-col gap-3">
            <p className="text-xs text-light-500">
              Number in parentheses indicates the number of evidence
              sources supporting this food-chemical relationship.
            </p>
            <div className="flex flex-wrap gap-1">
              {data.map((row) => {
                const rowReportProps = reporter.getRowProps({
                  kind: "food-composition-row",
                  entityType: "chemical",
                  entitySlug: chemicalName,
                  chemicalName,
                  foodId: row.id,
                  foodName: row.name,
                  dataPointCount: row.evidence_count,
                });
                return (
                <span
                  key={row.id}
                  {...rowReportProps}
                  className={twMerge(
                    "inline-flex items-baseline gap-1 capitalize text-xs leading-tight px-2 py-0.5 rounded-full border border-light-700/70 bg-light-900/40 text-light-200 max-w-full",
                    rowReportProps.className,
                  )}
                >
                  <Link
                    className="capitalize"
                    href={`/food/${encodeURIComponent(encodeSpace(row.name))}${chemicalName ? `?highlight=${encodeURIComponent(chemicalName)}#composition` : ""}`}
                    isExternal={false}
                  >
                    {row.name}
                  </Link>
                  <EntitySiblingIcon
                    siblings={row.ambiguity_siblings}
                    entityKind="food"
                  />
                  {row.evidence_count > 0 && (
                    <span className="not-italic font-mono text-[10px] tabular-nums opacity-70">
                      {row.evidence_count}
                    </span>
                  )}
                </span>
                );
              })}
            </div>
            </div>
          )}
        </div>
      ) : (
        <div className="h-16 flex items-center justify-center text-light-300 gap-2">
          {/* Mirrors the known-concentration section's wording so the two
           * read as a pair rather than as two independent verdicts. */}
          <MdInfoOutline /> No foods with an unknown concentration
        </div>
      )}
    </div>
  );
};

NoConcentrationComposition.displayName = "NoConcentrationComposition";

export default NoConcentrationComposition;

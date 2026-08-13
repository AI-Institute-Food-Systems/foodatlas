"use client";

import { MdDescription } from "react-icons/md";

import { AmbiguityIcon } from "@/components/basic/Ambiguity";
import Chip from "@/components/basic/Chip";
import ConcentrationBar from "@/components/basic/ConcentrationBar";
import Link from "@/components/basic/Link";
import { ambiguityCandidates } from "@/components/entities/chemical/ChemicalCompositionRow";
import {
  ChemicalCompositionRow as Row,
  barPercent,
  concentrationValue,
  evidenceCountOf,
  formatPercentByMass,
  rowSourceLabels,
} from "@/utils/chemicalComposition";
import { formatConcentrationValueAlt } from "@/utils/utils";

interface ChemicalCompositionCardsProps {
  rows: Row[];
  maxValue: number;
  hrefFor: (row: Row) => string;
  rowPropsFor: (row: Row) => React.HTMLAttributes<HTMLDivElement>;
}

// Mobile replacement for the table below md:. Same data, stacked — the
// concentration bar keeps its full width here because there are no sibling
// columns competing for it.
const ChemicalCompositionCards = ({
  rows,
  maxValue,
  hrefFor,
  rowPropsFor,
}: ChemicalCompositionCardsProps) => (
  <div className="w-full flex flex-col divide-y divide-light-800">
    {rows.map((row) => {
      const value = concentrationValue(row);
      const percent = barPercent(value, maxValue);
      const pctByMass = formatPercentByMass(row);
      const sources = rowSourceLabels(row);
      const evidence = evidenceCountOf(row);

      return (
        <div
          key={row.id}
          className="w-full py-3 flex flex-col gap-2"
          {...rowPropsFor(row)}
        >
          <div className="flex items-center gap-2 capitalize">
            <Link href={hrefFor(row)} isExternal={false}>
              {row.name}
            </Link>
            <AmbiguityIcon foodCandidates={ambiguityCandidates(row)} />
          </div>

          <div className="flex items-center justify-between gap-2">
            <span className="text-light-500 text-xs uppercase">
              Concentration
            </span>
            {value === null ? (
              <span className="text-light-600">—</span>
            ) : (
              <span className="font-mono text-xs text-light-200 tabular-nums">
                {formatConcentrationValueAlt(value)}
                {pctByMass && (
                  <span className="ml-2 text-light-500">
                    {pctByMass} by mass
                  </span>
                )}
              </span>
            )}
          </div>

          {value !== null && (
            <div className="flex w-full">
              <ConcentrationBar percent={percent} />
            </div>
          )}

          <div className="flex items-center justify-between gap-2">
            <span className="text-light-500 text-xs uppercase">Sources</span>
            <span className="flex items-center gap-1 flex-wrap justify-end">
              {sources.length > 0 ? (
                sources.map((label) => (
                  <Chip key={label} label={label} tone="outline" size="sm" />
                ))
              ) : (
                <span className="text-light-600">—</span>
              )}
            </span>
          </div>

          <div className="flex items-center justify-between gap-2">
            <span className="text-light-500 text-xs uppercase">Evidence</span>
            <Chip
              icon={<MdDescription className="size-3" />}
              label={`${evidence} data point${evidence === 1 ? "" : "s"}`}
              tone="outline"
              size="md"
              href={hrefFor(row)}
            />
          </div>
        </div>
      );
    })}
  </div>
);

ChemicalCompositionCards.displayName = "ChemicalCompositionCards";

export default ChemicalCompositionCards;

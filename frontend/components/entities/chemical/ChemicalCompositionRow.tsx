"use client";

import { MdDescription } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import { AmbiguityIcon } from "@/components/basic/Ambiguity";
import Chip from "@/components/basic/Chip";
import ConcentrationBar from "@/components/basic/ConcentrationBar";
import Link from "@/components/basic/Link";
import { cellPadding } from "@/components/basic/skeletonTokens";
import {
  ChemicalCompositionRow as Row,
  barPercent,
  concentrationValue,
  evidenceCountOf,
  formatPercentByMass,
  rowSourceLabels,
} from "@/utils/chemicalComposition";
import { formatConcentrationValueAlt } from "@/utils/utils";

export const COLUMN_COUNT = 3;

interface ChemicalCompositionRowProps {
  row: Row;
  // Denominator for the bar. Comes from the full result set, not this
  // page — see computeMaxValue.
  maxValue: number;
  href: string;
  // Opens the evidence modal for this row. The evidence itself is fetched
  // on demand by the table — a chemical's full evidence set is ~70x the
  // size of the table that links to it.
  onEvidenceClick: (foodName: string) => void;
  rowProps: React.HTMLAttributes<HTMLTableRowElement>;
}

// Candidate list for the ambiguity tooltip. The siblings array excludes the
// food itself, but isAmbiguous() triggers on length > 1, so the row's own
// name has to lead the list for a single sibling to register.
export const ambiguityCandidates = (row: Row): string[] => {
  const siblings = row.ambiguity_siblings ?? [];
  return siblings.length > 0
    ? [row.name, ...siblings.map((s) => s.common_name)]
    : [];
};

const ChemicalCompositionTableRow = ({
  row,
  maxValue,
  href,
  onEvidenceClick,
  rowProps,
}: ChemicalCompositionRowProps) => {
  const value = concentrationValue(row);
  const percent = barPercent(value, maxValue);
  const pctByMass = formatPercentByMass(row);
  const evidence = evidenceCountOf(row);

  return (
    <tr {...rowProps}>
      {/* food */}
      <td className={twMerge("py-1.5", cellPadding(0, COLUMN_COUNT))}>
        <div className="flex min-h-9 capitalize items-center gap-2">
          <Link href={href} isExternal={false}>
            {row.name}
          </Link>
          <AmbiguityIcon foodCandidates={ambiguityCandidates(row)} />
        </div>
      </td>

      {/* concentration: bar + value + share of the food's mass */}
      <td className={twMerge("py-1.5", cellPadding(1, COLUMN_COUNT))}>
        <div className="flex min-h-9 items-center gap-3">
          {value === null ? (
            <span className="text-light-600">—</span>
          ) : (
            <>
              <ConcentrationBar percent={percent} />
              <span className="font-mono text-xs text-light-200 whitespace-nowrap tabular-nums text-right min-w-[5rem]">
                {formatConcentrationValueAlt(value)}
              </span>
              {pctByMass && (
                <span
                  className="font-mono text-xs text-light-500 whitespace-nowrap tabular-nums text-right min-w-[3.5rem]"
                  title="Percentage of the food's mass"
                >
                  {pctByMass}
                  <span className="ml-1 text-light-600">by mass</span>
                </span>
              )}
            </>
          )}
        </div>
      </td>

      {/* evidence */}
      <td className={twMerge("py-1.5", cellPadding(2, COLUMN_COUNT))}>
        <div className="flex min-h-9 items-center justify-end">
          <Chip
            icon={<MdDescription className="size-3" />}
            label={`${evidence} data point${evidence === 1 ? "" : "s"}`}
            tone="outline"
            size="md"
            onClick={() => onEvidenceClick(row.name)}
            className="min-w-[9rem] justify-center"
          />
        </div>
      </td>
    </tr>
  );
};

ChemicalCompositionTableRow.displayName = "ChemicalCompositionTableRow";

export default ChemicalCompositionTableRow;

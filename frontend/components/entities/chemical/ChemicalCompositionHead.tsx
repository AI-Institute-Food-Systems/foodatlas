"use client";

import { twMerge } from "tailwind-merge";

import { cellPadding } from "@/components/basic/skeletonTokens";
import { COLUMN_COUNT } from "@/components/entities/chemical/ChemicalCompositionRow";
import { Th } from "@/components/entities/shared/EvidenceTable";
import {
  COLUMNS,
  SortColumn,
  SortDirection,
} from "@/utils/chemicalComposition";

// The sortable header row. Split out of ChemicalCompositionTable for the
// 300-line rule; it reads the same COLUMNS spec the <colgroup>, the body
// cells and the loading skeleton all derive from, so the header cannot
// describe a different table from the one below it. The cells themselves
// are the shared Th, so the affordance is the one every table shows.

interface Props {
  sort: { column: SortColumn; direction: SortDirection };
  onSortClick: (column: SortColumn) => void;
}

const ChemicalCompositionHead = ({ sort, onSortClick }: Props) => (
  <thead className="text-light-400 text-left">
    <tr>
      {COLUMNS.map((c, i) => {
        const key = c.sort;
        return (
          <Th
            key={c.key}
            align={c.align === "right" ? "right" : undefined}
            className={twMerge(cellPadding(i, COLUMN_COUNT), i === 0 && "pl-0")}
            sort={
              key && {
                active: key === sort.column,
                dir: sort.direction,
                onClick: () => onSortClick(key),
              }
            }
          >
            {c.label}
          </Th>
        );
      })}
    </tr>
  </thead>
);

ChemicalCompositionHead.displayName = "ChemicalCompositionHead";

export default ChemicalCompositionHead;

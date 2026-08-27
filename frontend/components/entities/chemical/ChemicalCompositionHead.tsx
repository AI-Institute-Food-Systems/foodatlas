"use client";

import {
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import { cellPadding } from "@/components/basic/skeletonTokens";
import { COLUMN_COUNT } from "@/components/entities/chemical/ChemicalCompositionRow";
import {
  COLUMNS,
  SortColumn,
  SortDirection,
} from "@/utils/chemicalComposition";

// The sortable header row. Split out of ChemicalCompositionTable for the
// 300-line rule; it reads the same COLUMNS spec the <colgroup>, the body
// cells and the loading skeleton all derive from, so the header cannot
// describe a different table from the one below it.

interface Props {
  sort: { column: SortColumn; direction: SortDirection };
  onSortClick: (column: SortColumn) => void;
}

const ChemicalCompositionHead = ({ sort, onSortClick }: Props) => (
  <thead className="text-light-400 text-left">
    <tr>
      {COLUMNS.map((c, i) => (
        <th
          key={c.key}
          className={twMerge(
            "h-9 border-b border-light-700 leading-none py-1.5",
            cellPadding(i, COLUMN_COUNT),
            c.align === "right" ? "text-right" : "text-left"
          )}
        >
          <div
            className={twMerge(
              "group flex gap-1 items-center flex-nowrap w-full",
              c.sort ? "cursor-pointer" : "pointer-events-none",
              c.align === "right" ? "justify-end" : "justify-between"
            )}
            onClick={() => c.sort && onSortClick(c.sort)}
          >
            <span
              className={twMerge(
                "select-none uppercase text-xs font-medium transition duration-300 ease-in-out group-hover:text-light-100",
                c.sort === sort.column && "text-light-100"
              )}
            >
              {c.label}
            </span>
            {c.sort &&
              (c.sort === sort.column ? (
                sort.direction === "asc" ? (
                  <MdKeyboardArrowUp className="text-accent-600 flex-shrink-0" />
                ) : (
                  <MdKeyboardArrowDown className="text-accent-600 flex-shrink-0" />
                )
              ) : (
                <MdUnfoldMore className="text-light-400 flex-shrink-0" />
              ))}
          </div>
        </th>
      ))}
    </tr>
  </thead>
);

ChemicalCompositionHead.displayName = "ChemicalCompositionHead";

export default ChemicalCompositionHead;

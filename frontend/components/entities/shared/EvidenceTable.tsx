"use client";

// Small presentational primitives shared by the evidence tables. Each of
// those tables renders a desktop <table> and a mobile card list, and
// before this they carried their own byte-identical copies of the header
// cell and the card label/value row.

import type { ReactNode } from "react";
import {
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import SortListbox from "@/components/basic/SortListbox";
import { InfoTip } from "@/components/basic/Tooltip";

export type SortDir = "asc" | "desc";

// THE header cell. Plain, sortable, explained, or both — one component,
// so a column header looks and behaves the same on every table.
//
// `sort` makes the label a button with the sort affordance: an up/down
// arrow in accent when this column drives the order, MdUnfoldMore
// otherwise. The disease page's Chemicals tab had no sorting at all
// because its two tables hand-rolled their headers and neither copy
// grew the affordance; the copies that did exist elsewhere had drifted
// (four of them). A sortable header is now something a table opts INTO
// with one prop, not something it builds.
export const Th = ({
  children,
  align,
  help,
  sort,
  className,
}: {
  children: ReactNode;
  align?: "right";
  // What the column means, as the "i" every explained header carries.
  help?: ReactNode;
  // Present ⇒ the column sorts. `active` says this column drives the
  // order now; `dir` is the current direction (only meaningful when
  // active); `onClick` asks the table to sort by this column.
  sort?: { active: boolean; dir: SortDir; onClick: () => void };
  // Padding overrides for tables whose cells carry their own scheme.
  className?: string;
}) => {
  const label = sort ? (
    <button
      type="button"
      onClick={sort.onClick}
      className={twMerge(
        "group inline-flex items-center gap-1 cursor-pointer focus:outline-none",
        align === "right" && "justify-end"
      )}
    >
      <span
        className={twMerge(
          "select-none uppercase text-xs font-medium transition duration-300 ease-in-out",
          sort.active
            ? "text-light-100"
            : "text-light-400 group-hover:text-light-100"
        )}
      >
        {children}
      </span>
      {sort.active ? (
        sort.dir === "asc" ? (
          <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
        ) : (
          <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
        )
      ) : (
        <MdUnfoldMore className="text-light-400 group-hover:text-light-100 flex-shrink-0" />
      )}
    </button>
  ) : (
    <span className="select-none uppercase text-xs font-medium">{children}</span>
  );

  return (
    <th
      aria-sort={
        sort?.active ? (sort.dir === "asc" ? "ascending" : "descending") : undefined
      }
      className={twMerge(
        // The first header drops its left padding: every body's first cell
        // is `pr-4` with nothing on the left, so the header's px-4 indented
        // it 16px past the column it labels. pl-px, not pl-0 — cells keep
        // the browser's 1px default, and the literature table's hand-rolled
        // <th> aligns because it does too. Left edge only: right-aligned
        // headers sit over cells that do keep their px-4.
        "h-9 border-b border-light-700 leading-none py-1.5 px-4 first:pl-px uppercase text-xs font-medium",
        align === "right" ? "text-right" : "text-left",
        className
      )}
    >
      {help ? (
        <span
          className={twMerge(
            "inline-flex items-center gap-1",
            align === "right" && "justify-end"
          )}
        >
          {label}
          <InfoTip content={help} label={`About the ${textOf(children)} column`} />
        </span>
      ) : (
        label
      )}
    </th>
  );
};

// The header's text, for the InfoTip's accessible name. Headers here are
// plain strings; anything richer falls back to a generic label.
const textOf = (node: ReactNode): string =>
  typeof node === "string" || typeof node === "number" ? String(node) : "this";

// The next sort state after clicking a column: a new column starts at
// `firstDir` (numbers read best largest-first, names A→Z), the same
// column flips. One rule for every table, so a header click means the
// same thing everywhere.
export const nextSort = <K extends string>(
  current: { by: K; dir: SortDir },
  clicked: K,
  firstDir: SortDir = "desc"
): { by: K; dir: SortDir } =>
  current.by === clicked
    ? { by: clicked, dir: current.dir === "asc" ? "desc" : "asc" }
    : { by: clicked, dir: firstDir };

// A sortable column, as the mobile sort control needs to know it: the
// key the table sorts by, and how each direction reads as a phrase.
// Arrows ("Chemical ↓") are ambiguous for text vs numbers, so every
// sortable column says it in words.
export interface SortableColumn<K extends string = string> {
  key: K;
  labels: { asc: string; desc: string };
}

// The card list has no clickable headers, so on small screens the sort
// is a listbox above it. One control for every table, in the same
// place with the same words — this was inlined in BioactivityTable and
// absent from the tables that never grew sorting.
export const MobileSort = <K extends string>({
  sort,
  columns,
  onChange,
  ariaLabel = "Sort",
}: {
  sort: { by: K; dir: SortDir };
  columns: readonly SortableColumn<K>[];
  onChange: (sort: { by: K; dir: SortDir }) => void;
  ariaLabel?: string;
}) => (
  <div className="mb-1.5 md:hidden flex justify-end items-center gap-2">
    <span className="font-mono italic text-[11px] text-light-500">sort</span>
    <SortListbox
      ariaLabel={ariaLabel}
      value={`${sort.by}|${sort.dir}`}
      options={columns.flatMap((c) => [
        { value: `${c.key}|desc`, label: c.labels.desc },
        { value: `${c.key}|asc`, label: c.labels.asc },
      ])}
      onChange={(value) => {
        const [by, dir] = value.split("|") as [K, SortDir];
        onChange({ by, dir });
      }}
    />
  </div>
);

export const CardRow = ({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) => (
  <div className="flex items-baseline justify-between gap-3 text-[11px] font-mono">
    <span className="text-light-500 shrink-0">{label}</span>
    <span className="text-right">{children}</span>
  </div>
);

export const CountCell = ({
  value,
  tone = "text-light-200",
}: {
  value: number;
  tone?: string;
}) => <span className={`tabular-nums ${tone}`}>{value.toLocaleString()}</span>;

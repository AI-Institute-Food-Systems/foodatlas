"use client";

// Small presentational primitives shared by the bioactivity↔disease tables.
// Each of those tables renders a desktop <table> and a mobile card list, and
// before this they carried their own byte-identical copies of the header cell
// and the card label/value row.

import type { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

export const Th = ({
  children,
  align,
  title,
}: {
  children: ReactNode;
  align?: "right";
  title?: string;
}) => (
  <th
    title={title}
    className={twMerge(
      // The first header drops its left padding: every body's first cell
      // is `pr-4` with nothing on the left, so the header's px-4 indented
      // it 16px past the column it labels. pl-px, not pl-0 — cells keep
      // the browser's 1px default, and the literature table's hand-rolled
      // <th> aligns because it does too. Left edge only: right-aligned
      // headers sit over cells that do keep their px-4.
      "h-9 border-b border-light-700 py-1.5 px-4 first:pl-px uppercase text-xs font-medium",
      align === "right" ? "text-right" : "text-left",
    )}
  >
    {children}
  </th>
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

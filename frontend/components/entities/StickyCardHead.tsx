"use client";

import { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

interface Props {
  children: ReactNode;
  className?: string;
}

// Sticky "top-of-card" chip. Meant to be rendered as the first child
// inside an existing `<Card>` — it uses negative -mx / -mt to escape
// the Card's own padding so its rounded top corners + border align
// exactly with the Card's outer frame. When the user scrolls, the
// chip stays pinned at top-[100px] md:top-[112px] (flush against the
// subnavbar + 12px breathing gap) while the Card body scrolls behind.
// The 12px page-bg box-shadow above hides the scrolling Card top edge
// as it passes into the breathing gap.
//
// Column-alignment note: the caller renders its own `<table>` with
// matching `<colgroup>` widths here AND in the tbody-only body table
// below. `table-layout: fixed` + identical colgroups keep both tables
// column-aligned.
const StickyCardHead = ({ children, className }: Props) => (
  <div
    className={twMerge(
      "hidden md:block sticky top-[100px] md:top-[112px] z-30",
      "-mx-5 md:-mx-6 -mt-3 md:-mt-4",
      "rounded-t-xl border-[1.5px] border-b-0 border-light-50/[0.08]",
      "bg-light-950 px-5 md:px-6 pt-3 md:pt-4",
      "shadow-[0_-12px_0_0_#0a0a09]",
      className
    )}
  >
    {children}
  </div>
);

StickyCardHead.displayName = "StickyCardHead";

export default StickyCardHead;

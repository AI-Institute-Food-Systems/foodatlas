"use client";

import { ReactNode, useEffect, useRef, useState } from "react";
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
//
// The 12px page-bg box-shadow above the chip is applied *only when
// stuck* — at natural position it would overlap the tab chips sitting
// on the Card's top edge and cover their bottom pixels. A sentinel
// div right above the chip is observed with rootMargin matching the
// stuck top offset; when the sentinel scrolls out of the intersection
// region, the chip is stuck → shadow on.
//
// Column-alignment note: the caller renders its own `<table>` with
// matching `<colgroup>` widths here AND in the tbody-only body table
// below. `table-layout: fixed` + identical colgroups keep both tables
// column-aligned.
const StickyCardHead = ({ children, className }: Props) => {
  const chipRef = useRef<HTMLDivElement>(null);
  const [stuck, setStuck] = useState(false);

  useEffect(() => {
    const el = chipRef.current;
    if (!el) return;
    // Chip is pinned by `position: sticky; top: 112px`. It's actually
    // "stuck" only while its rect.top === 112: before scroll reaches
    // the card it sits below (rect.top > 112), and after the card's
    // bottom scrolls above the sticky offset the browser lets the chip
    // ride up with its parent (rect.top < 112). A sentinel above the
    // chip can only detect the first transition — it latches true and
    // never releases when the card scrolls out below, so the fixed
    // backdrop lingered past the table. Reading the chip's own rect
    // catches both edges.
    const OFFSET = 112;
    const check = () => {
      const rect = el.getBoundingClientRect();
      setStuck(Math.abs(rect.top - OFFSET) < 1);
    };
    check();
    window.addEventListener("scroll", check, { passive: true });
    window.addEventListener("resize", check);
    return () => {
      window.removeEventListener("scroll", check);
      window.removeEventListener("resize", check);
    };
  }, []);

  return (
    <>
      {/* Black rectangle that appears once stuck. Sits *behind* the
       * chip (z-20 vs the chip's z-30) and spans subnav-bottom
       * through the chip's bottom. Constrained to the card's own
       * horizontal bounds so the outer padding zones (filter sidebar,
       * page edges) stay visible. Outer div carries the page's
       * horizontal padding + fixed positioning; inner div is the
       * actual max-w-5xl centered black slab. pointer-events-none so
       * header clicks still reach the chip. */}
      {stuck && (
        <div
          aria-hidden
          className="fixed inset-x-0 top-[88px] md:top-[100px] px-4 md:px-24 h-16 z-20 pointer-events-none"
        >
          <div className="mx-auto max-w-5xl h-full bg-[#0a0a09]" />
        </div>
      )}
      <div
        ref={chipRef}
        className={twMerge(
          "hidden md:block sticky top-[100px] md:top-[112px] z-30",
          // Negative margins extend by an extra 1.5px on each escaped
          // side beyond Card's padding — that shifts our outer edge
          // out to Card's OUTER edge (not just the post-border edge),
          // so our 1.5px border overlaps Card's 1.5px border into a
          // single visible line. Prevents the 1.5px step where
          // StickyCardHead ends and Card body continues.
          "-mx-[21.5px] md:-mx-[25.5px] -mt-[13.5px] md:-mt-[17.5px]",
          "rounded-t-xl border-[1.5px] border-b-0 border-light-50/[0.08]",
          "bg-light-950 px-5 md:px-6 pt-3 md:pt-4",
          className,
        )}
      >
        {children}
      </div>
    </>
  );
};

StickyCardHead.displayName = "StickyCardHead";

export default StickyCardHead;

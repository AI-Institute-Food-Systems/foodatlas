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
// When stuck we paint a 12px black slab *anchored to the chip* right
// above it, so the gap between subnav-bottom and chip-top doesn't
// reveal scrolling content. Because it's an absolutely positioned
// child (not fixed) it rides up with the chip when the card scrolls
// past — no lingering rectangle over content below the table.
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
    // Sticky pins the chip at top: 112px. It's actually "stuck" only
    // while rect.top === 112 — before scroll reaches the card it sits
    // below (rect.top > 112), and once the card's bottom scrolls above
    // the offset the chip rides up with its parent (rect.top < 112).
    // Reading the chip's own rect catches both edges cleanly.
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
      {/* Invisible page-bg mask filling the 12px gap between the
       * subnav bottom and the chip's top. Same color as body bg so
       * the gap still *looks* like page background — its only job is
       * to stop scrolling content from bleeding through the gap.
       * Anchored to the chip so it rides up and vanishes together
       * with it when the card scrolls past.
       *
       * Horizontal `-inset-x-2` gives ~8px overhang past the chip's
       * borders so the mask fully swallows AA halo on the chip's
       * outer edge. Extra height `h-6` (24px) reaches 12px down into
       * the chip — enough to cover its `rounded-t-xl` corners, which
       * otherwise leave transparent triangles that reveal scrolling
       * content behind the sticky head. */}
      {stuck && (
        <div
          aria-hidden
          className="absolute -inset-x-2 -top-3 h-6 bg-[#0a0a09] pointer-events-none"
        />
      )}
      {children}
    </div>
  );
};

StickyCardHead.displayName = "StickyCardHead";

export default StickyCardHead;

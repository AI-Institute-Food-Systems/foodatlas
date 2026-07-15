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
        "border-x-[1.5px] border-b-0 border-light-50/[0.08]",
        // When stuck the visual top of the chip is 12px higher (the
        // anchored slab below extends upward). Move rounding + border
        // + shadow onto that slab so the composite looks like one
        // taller flat-topped-then-rounded shape.
        stuck
          ? "rounded-t-none border-t-0"
          : "rounded-t-xl border-t-[1.5px]",
        "bg-light-950 px-5 md:px-6 pt-3 md:pt-4",
        className,
      )}
    >
      {/* 12px extension anchored to the chip. Fills the subnav↔chip
       * gap without a separate fixed layer, so it disappears in
       * lockstep with the chip when the card scrolls past. */}
      {stuck && (
        <div
          aria-hidden
          className="absolute inset-x-0 -top-3 h-3 rounded-t-xl bg-light-950 border-x-[1.5px] border-t-[1.5px] border-light-50/[0.08] pointer-events-none"
        />
      )}
      {children}
    </div>
  );
};

StickyCardHead.displayName = "StickyCardHead";

export default StickyCardHead;

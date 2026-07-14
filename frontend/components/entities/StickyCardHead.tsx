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
  const sentinelRef = useRef<HTMLDivElement>(null);
  const [stuck, setStuck] = useState(false);

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    // Root margin matches the sticky top: mobile 100px, desktop 112px.
    // Using the larger (112) is safe — on mobile the sentinel just
    // switches slightly earlier, which is imperceptible against the
    // already-sticky subnav layout.
    const obs = new IntersectionObserver(
      ([entry]) => setStuck(entry.intersectionRatio === 0),
      { threshold: [0, 1], rootMargin: "-112px 0px 0px 0px" },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  return (
    <>
      {/* Sentinel: 0-height sibling immediately above. When it's
       * intersecting the (viewport shifted down 112px) region, the
       * chip isn't stuck. Once it scrolls out (above y=112) the chip
       * is stuck and we can safely paint the gap-filling shadow. */}
      <div ref={sentinelRef} aria-hidden className="hidden md:block h-0" />
      <div
        className={twMerge(
          "hidden md:block sticky top-[100px] md:top-[112px] z-30",
          "-mx-5 md:-mx-6 -mt-3 md:-mt-4",
          "rounded-t-xl border-[1.5px] border-b-0 border-light-50/[0.08]",
          "bg-light-950 px-5 md:px-6 pt-3 md:pt-4",
          stuck && "shadow-[0_-12px_0_0_#0a0a09]",
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

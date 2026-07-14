"use client";

import { useEffect, useState } from "react";

interface Props {
  // Element id to watch — the subnavbar reveals as soon as the target's
  // viewport-top position drops at or below `threshold`. Set the id on
  // the element that represents "the point where the sticky tab strip
  // would start sticking" so subnav appears in lockstep with the tabs.
  targetId: string;
  // Pixel threshold: subnav shown when target.getBoundingClientRect().top
  // <= threshold. Match the tab strip's sticky-top offset (100 for the
  // desktop layout; mobile picker at 88 arrives a few px earlier).
  threshold?: number;
  children: React.ReactNode;
}

// Client wrapper that reveals its children (fixed-positioned) once the
// watched target scrolls up past `threshold`. Uses a scroll listener so
// the state syncs precisely with the sticky-tab trigger — plain
// IntersectionObserver on the target won't fire at the same scroll
// depth (it fires when the element enters/exits the viewport, not when
// it hits an arbitrary y).
const StickyOnScrollPast = ({
  targetId,
  threshold = 100,
  children,
}: Props) => {
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const target = document.getElementById(targetId);
    if (!target) return;
    const check = () => {
      const rect = target.getBoundingClientRect();
      // display:none ancestors (e.g. EntityPageGate before ready) collapse
      // the sentinel to a zero box at the origin. Without this guard the
      // initial check would see top=0, flip visible=true, and the state
      // would stick through the reveal because no scroll event follows.
      if (rect.width === 0 && rect.height === 0) {
        setVisible(false);
        return;
      }
      setVisible(rect.top <= threshold);
    };
    check();
    window.addEventListener("scroll", check, { passive: true });
    window.addEventListener("resize", check);
    // Re-check when the sentinel's box changes — covers the gate flip
    // from display:none to visible after PageReadyContext becomes ready.
    const ro = new ResizeObserver(check);
    ro.observe(target);
    return () => {
      window.removeEventListener("scroll", check);
      window.removeEventListener("resize", check);
      ro.disconnect();
    };
  }, [targetId, threshold]);

  // No opacity transition — user found the fade distracting. Snap
  // between hidden and visible.
  if (!visible) return null;
  return (
    <div className="fixed left-0 right-0 top-12 md:top-14 z-40">
      {children}
    </div>
  );
};

StickyOnScrollPast.displayName = "StickyOnScrollPast";

export default StickyOnScrollPast;

"use client";

import { useEffect, useRef, useState } from "react";

interface Props {
  // Element id to watch — when this element scrolls out of the viewport
  // top, the children are revealed (fixed position). When it comes back
  // into view, children are hidden. Typically the entity page header.
  targetId: string;
  children: React.ReactNode;
}

// Client wrapper that reveals its children (fixed-positioned) only
// once the watched target scrolls out of view. Used to gate the
// EntitySubnavbar so it doesn't stack on top of a fully-visible
// HeaderSection at the top of every entity page.
const StickyOnScrollPast = ({ targetId, children }: Props) => {
  const [visible, setVisible] = useState(false);
  const observerRef = useRef<IntersectionObserver | null>(null);

  useEffect(() => {
    const target = document.getElementById(targetId);
    if (!target) return;
    observerRef.current = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        // Show once the target has fully scrolled above the viewport.
        setVisible(!entry.isIntersecting);
      },
      { threshold: 0 },
    );
    observerRef.current.observe(target);
    return () => {
      observerRef.current?.disconnect();
      observerRef.current = null;
    };
  }, [targetId]);

  return (
    <div
      className={
        "fixed left-0 right-0 top-12 md:top-14 z-40 transition-opacity duration-150 " +
        (visible ? "opacity-100" : "opacity-0 pointer-events-none")
      }
      aria-hidden={!visible}
    >
      {children}
    </div>
  );
};

StickyOnScrollPast.displayName = "StickyOnScrollPast";

export default StickyOnScrollPast;

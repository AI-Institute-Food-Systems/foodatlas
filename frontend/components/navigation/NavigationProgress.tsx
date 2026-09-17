"use client";

import { useEffect, useRef, useState } from "react";

import { useNavigationSignal } from "@/context/navigationContext";

// Top-of-viewport progress bar. Fires the moment a nav caller invokes
// `startNav()`, ramps toward 90% while the new route is fetched, then
// completes + fades when the pathname flips (which is what clears
// isNavigating from the context). The point is perceptual — the user
// sees "something is happening" within one frame, instead of waiting a
// full network RTT for loading.tsx to arrive.
const NavigationProgress = () => {
  const { isNavigating } = useNavigationSignal();
  const [progress, setProgress] = useState(0);
  const [visible, setVisible] = useState(false);
  const timersRef = useRef<number[]>([]);

  useEffect(() => {
    const clearTimers = () => {
      timersRef.current.forEach((t) => window.clearTimeout(t));
      timersRef.current = [];
    };

    if (isNavigating) {
      clearTimers();
      setVisible(true);
      setProgress(15);
      timersRef.current.push(
        window.setTimeout(() => setProgress(45), 120),
        window.setTimeout(() => setProgress(70), 400),
        window.setTimeout(() => setProgress(88), 900),
        window.setTimeout(() => setProgress(94), 1800),
      );
    } else if (visible) {
      clearTimers();
      setProgress(100);
      timersRef.current.push(
        window.setTimeout(() => setVisible(false), 220),
        window.setTimeout(() => setProgress(0), 500),
      );
    }
    return clearTimers;
  }, [isNavigating, visible]);

  return (
    <div
      className="fixed top-0 left-0 right-0 z-[100] h-[2px] pointer-events-none"
      aria-hidden
    >
      <div
        className="h-full bg-accent-500 shadow-[0_0_8px_rgba(255,87,34,0.6)] transition-[width,opacity] ease-out"
        style={{
          width: `${progress}%`,
          opacity: visible ? 1 : 0,
          transitionDuration: isNavigating ? "300ms" : "180ms",
        }}
      />
    </div>
  );
};

NavigationProgress.displayName = "NavigationProgress";

export default NavigationProgress;

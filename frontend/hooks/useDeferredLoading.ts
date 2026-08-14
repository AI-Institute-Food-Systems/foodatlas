import { useEffect, useRef, useState } from "react";

// Suppresses the loading skeleton for genuinely fast responses.
//
// Locally the API answers a table in 15–50ms. A skeleton shown for that
// long is not an affordance, it is a flicker: one frozen frame of a 2s
// pulse, arriving and leaving before the eye resolves it. Production is
// slower, but a warm response there is still well under the threshold at
// which a placeholder helps anyone.
//
// Two thresholds, and both are needed — the delay alone makes things
// worse at the boundary:
//
//   DELAY  wait this long before admitting to loading at all. Anything
//          that resolves sooner paints content directly, no placeholder.
//   FLOOR  once shown, keep it up this long. Without it, data landing at
//          DELAY+10ms produces a 10ms flash, which is the very artefact
//          the delay exists to remove.
//
// Returns whether the *skeleton* should render, which is deliberately not
// the same question as whether a fetch is in flight. Callers still branch
// their real empty/error states on the underlying flag.
const DELAY_MS = 200;
const FLOOR_MS = 400;

export const useDeferredLoading = (
  isLoading: boolean,
  { delayMs = DELAY_MS, floorMs = FLOOR_MS } = {}
): boolean => {
  const [visible, setVisible] = useState(false);
  // When the skeleton actually appeared, so the floor is measured from
  // first paint rather than from the start of the request.
  const shownAt = useRef<number | null>(null);

  useEffect(() => {
    if (isLoading) {
      if (visible) return;
      const timer = window.setTimeout(() => {
        shownAt.current = Date.now();
        setVisible(true);
      }, delayMs);
      return () => window.clearTimeout(timer);
    }

    if (!visible) return;
    const elapsed = shownAt.current === null ? 0 : Date.now() - shownAt.current;
    const remaining = floorMs - elapsed;
    if (remaining <= 0) {
      shownAt.current = null;
      setVisible(false);
      return;
    }
    const timer = window.setTimeout(() => {
      shownAt.current = null;
      setVisible(false);
    }, remaining);
    return () => window.clearTimeout(timer);
  }, [isLoading, visible, delayMs, floorMs]);

  return visible;
};

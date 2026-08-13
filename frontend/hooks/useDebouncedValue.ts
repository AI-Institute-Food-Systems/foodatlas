import { useEffect, useState } from "react";

// Delays a rapidly-changing value so effects keyed on it don't fire per
// keystroke. The table search inputs previously entered their fetch
// effect's dependency list directly, so typing "tomato" fired six
// requests and blanked the table six times.
//
// The initial value is returned immediately — only *changes* are delayed,
// so first paint is never held back. The mount-time timeout resolves to
// the value already in state, which React discards without a re-render.
export const useDebouncedValue = <T,>(value: T, delayMs = 250): T => {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timer);
  }, [value, delayMs]);

  return debounced;
};

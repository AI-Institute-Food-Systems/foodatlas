"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import { usePathname } from "next/navigation";

interface NavigationContextValue {
  isNavigating: boolean;
  startNav: () => void;
}

const NavigationContext = createContext<NavigationContextValue>({
  isNavigating: false,
  startNav: () => {},
});

// Route transitions on mobile can eat 300-800ms of RTT + cold API before
// the server's RSC payload (and loading.tsx) reaches the browser.
// Callers wrap their `router.push` with `startNav()` so a global progress
// bar can flash instantly on click, closing the "nothing is happening"
// perceptual gap. `isNavigating` auto-clears when pathname changes.
export const NavigationProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const [isNavigating, setIsNavigating] = useState(false);
  const pathname = usePathname();

  useEffect(() => {
    setIsNavigating(false);
  }, [pathname]);

  // Safety net: if a click starts a "navigation" that never resolves in a
  // pathname change (same-URL click, cancelled push, etc.), clear the bar
  // after 8s so it doesn't stick.
  useEffect(() => {
    if (!isNavigating) return;
    const t = window.setTimeout(() => setIsNavigating(false), 8000);
    return () => window.clearTimeout(t);
  }, [isNavigating]);

  const startNav = useCallback(() => setIsNavigating(true), []);

  return (
    <NavigationContext.Provider value={{ isNavigating, startNav }}>
      {children}
    </NavigationContext.Provider>
  );
};

export const useNavigationSignal = () => useContext(NavigationContext);

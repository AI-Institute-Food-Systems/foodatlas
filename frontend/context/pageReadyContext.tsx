"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";

interface PageReadyContextValue {
  ready: boolean;
  registerLoader: () => void;
  completeLoader: () => void;
}

const PageReadyContext = createContext<PageReadyContextValue>({
  ready: true,
  registerLoader: () => {},
  completeLoader: () => {},
});

// Tracks how many client-side data fetches an entity page has open. The
// `ready` flag flips true once every loader that registered on mount has
// signalled completion — the EntityPageGate uses this to hide the SSR
// shell (which paints with real header + tab labels + badges but body
// skeleton) until all client fetches resolve, then reveals everything
// at once. Kills the "header/tabs pop in, body still loading" flash.
//
// Safety net: if a loader stalls or forgets to complete, the gate
// reveals after 8s so the user isn't stuck on a skeleton forever.
export const PageReadyProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const [registered, setRegistered] = useState(0);
  const [completed, setCompleted] = useState(0);
  const [forceReady, setForceReady] = useState(false);

  useEffect(() => {
    const t = window.setTimeout(() => setForceReady(true), 8000);
    return () => window.clearTimeout(t);
  }, []);

  const registerLoader = useCallback(
    () => setRegistered((n) => n + 1),
    [],
  );
  const completeLoader = useCallback(
    () => setCompleted((n) => n + 1),
    [],
  );

  const ready = forceReady || (registered > 0 && completed >= registered);

  return (
    <PageReadyContext.Provider
      value={{ ready, registerLoader, completeLoader }}
    >
      {children}
    </PageReadyContext.Provider>
  );
};

export const usePageReady = () => useContext(PageReadyContext);

// Client components call this once with their `isLoading` state. On
// mount they register as a loader; when `isLoading` first flips false
// they mark themselves complete. Each component registers/completes at
// most once — subsequent refetches (filter, sort, page changes) don't
// re-block the gate.
export const useLoadingGate = (isLoading: boolean) => {
  const { registerLoader, completeLoader } = usePageReady();
  const registeredRef = useRef(false);
  const completedRef = useRef(false);

  useEffect(() => {
    if (!registeredRef.current) {
      registerLoader();
      registeredRef.current = true;
    }
    if (!isLoading && !completedRef.current) {
      completeLoader();
      completedRef.current = true;
    }
  }, [isLoading, registerLoader, completeLoader]);
};

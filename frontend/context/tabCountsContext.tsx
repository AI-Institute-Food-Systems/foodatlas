"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

interface TabCountsContextValue {
  counts: Record<string, number | null>;
  setTabCount: (tabId: string, count: number | null) => void;
}

const TabCountsContext = createContext<TabCountsContextValue>({
  counts: {},
  setTabCount: () => {},
});

// Lets a tab's content component publish its own row count so the tab
// badge can reflect the current filtered view — not the server-side
// prefetched total. EntityDetailLayout wraps children with the provider;
// EntityTabs reads counts and overrides the static `tab.count` prop
// whenever a dynamic entry exists.
export const TabCountsProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const [counts, setCounts] = useState<Record<string, number | null>>({});

  const setTabCount = useCallback(
    (tabId: string, count: number | null) => {
      setCounts((prev) => {
        if (prev[tabId] === count) return prev;
        return { ...prev, [tabId]: count };
      });
    },
    [],
  );

  const value = useMemo(
    () => ({ counts, setTabCount }),
    [counts, setTabCount],
  );

  return (
    <TabCountsContext.Provider value={value}>
      {children}
    </TabCountsContext.Provider>
  );
};

export const useTabCounts = () => useContext(TabCountsContext);

// Publishes a tab's row count from inside its content. Unregisters on
// unmount so the badge falls back to the tab spec's static count when
// the tab is torn down. An empty `tabId` is a no-op — callers with an
// optional publishing key (e.g. shared table components used both as
// tab content and as sub-panels) can pass `""` to disable.
export const usePublishTabCount = (
  tabId: string,
  count: number | null | undefined,
) => {
  const { setTabCount } = useTabCounts();
  useEffect(() => {
    if (!tabId) return;
    setTabCount(tabId, count ?? null);
    return () => setTabCount(tabId, null);
  }, [tabId, count, setTabCount]);
};

"use client";

import { ReactNode, useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Tab, TabGroup, TabList, TabPanel, TabPanels } from "@headlessui/react";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";

export type EntityType = "food" | "chemical" | "disease" | "bioactivity";

export type TabSpec = {
  id: string;
  label: string;
  // optional badge count rendered after the label; omit when not yet known.
  count?: number | null;
  content: ReactNode;
};

const formatCount = (n: number): string => {
  if (n >= 10000) return `${(n / 1000).toFixed(0)}k`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return n.toLocaleString();
};

interface Props {
  entityType: EntityType;
  tabs: TabSpec[];
  defaultTabId: string;
}

const EntityTabs = ({ tabs, defaultTabId }: Props) => {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  // Derive initial index from URL, then hold local state so the tab
  // switches immediately on click. Previously this was derived from
  // useSearchParams on every render, but `router.replace` doesn't
  // reliably update useSearchParams synchronously inside Headless UI's
  // controlled TabGroup — the tab would visually "revert" and the user
  // had to click twice. Local state avoids the round-trip.
  const urlId = searchParams.get("tab") ?? defaultTabId;
  const urlIdx = tabs.findIndex((t) => t.id === urlId);
  const [selectedIndex, setSelectedIndex] = useState(
    urlIdx >= 0 ? urlIdx : 0,
  );

  // Keep local state in sync when the URL changes from OUTSIDE this
  // component (e.g. browser back/forward, deep link).
  useEffect(() => {
    if (urlIdx >= 0 && urlIdx !== selectedIndex) {
      setSelectedIndex(urlIdx);
    }
    // We intentionally don't depend on selectedIndex — that would flip
    // the tab back if the URL update lags the click.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlIdx]);

  const handleChange = (next: number) => {
    const id = tabs[next]?.id;
    if (!id) return;
    setSelectedIndex(next);
    const params = new URLSearchParams(searchParams.toString());
    params.set("tab", id);
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };

  return (
    <TabGroup selectedIndex={selectedIndex} onChange={handleChange}>
      {/* Tabs sit on top of the card as apothecary-jar labels: cream chips
       * affixed to the top of a dark cabinet. Selected chip = cream fill,
       * unselected = dark slab with a visible border. -mb-[2px] pulls all
       * tabs down 2px so the selected chip's transparent bottom border
       * blends into the Card top edge below it. */}
      <TabList className="flex items-end gap-1.5 pl-3">
        {tabs.map((tab) => (
          <Tab
            key={tab.id}
            className={({ selected }) =>
              twMerge(
                "relative z-10 px-4 py-1.5 -mb-[2px]",
                "font-mono italic text-sm min-w-[9.5rem] font-medium",
                "rounded-t-md transition-colors outline-none border-t-[1.5px] border-x-[1.5px]",
                selected
                  ? "bg-light-200 text-light-900 border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)]"
                  : "bg-light-950/50 border-light-600/60 text-light-300 hover:text-light-100 hover:border-light-400 hover:bg-light-900/70"
              )
            }
          >
            {({ selected }) => (
              // min-h-5 locks the inner content height so label-only tabs
              // ("IDs & Metadata") render the same height as tabs whose
              // count badge would otherwise bump the row height by a px or
              // two.
              <span className="flex items-center justify-center gap-1.5 min-h-5">
                <span className="leading-none">{tab.label}</span>
                {typeof tab.count === "number" && (
                  <span
                    className={
                      "not-italic font-mono text-[0.65rem] tracking-wide px-1.5 py-[1px] rounded-full " +
                      (selected
                        ? "bg-light-900/15 text-light-700"
                        : "bg-light-800/80 text-light-400")
                    }
                  >
                    {formatCount(tab.count)}
                  </span>
                )}
              </span>
            )}
          </Tab>
        ))}
      </TabList>

      <Card>
        <TabPanels>
          {tabs.map((tab) => (
            <TabPanel
              key={tab.id}
              unmount={false}
              className="outline-none focus-visible:outline-light-200 data-[selected]:animate-[fadeSlide_180ms_ease-out]"
            >
              {tab.content}
            </TabPanel>
          ))}
        </TabPanels>
      </Card>
    </TabGroup>
  );
};

EntityTabs.displayName = "EntityTabs";

export default EntityTabs;

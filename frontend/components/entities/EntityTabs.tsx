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
       * affixed to the top of a dark cabinet. The selected chip pops in
       * cream-on-dark for unmistakable selection; unselected chips read as
       * dimmer wax seals beside it. -mb-[1.5px] overlaps the card border. */}
      <TabList className="flex items-end gap-1.5 pl-3">
        {tabs.map((tab) => (
          <Tab
            key={tab.id}
            className={({ selected }) =>
              twMerge(
                "relative z-10 px-4 py-1.5 -mb-[2px]",
                "font-mono italic text-sm min-w-[9.5rem] font-medium",
                "rounded-t-md transition-colors outline-none border-[1.5px]",
                // Selected: cream chip with NO bottom border. pb is bumped
                // by the missing 1.5px so total height matches unselected.
                // The 2px cream box-shadow bleeds the chip's fill down INTO
                // the card body so the card's faint top border never shows
                // as a hairline below the chip.
                selected
                  ? "bg-light-200 text-light-900 border-light-200 border-b-0 pb-[calc(0.375rem_+_1.5px)] shadow-[inset_0_1px_2px_rgba(255,249,242,0.6),0_2px_0_0_#efe6de]"
                  : "bg-transparent border-light-700/50 border-b-transparent text-light-400 hover:text-light-100 hover:border-light-500"
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

"use client";

import { ReactNode, useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
  Tab,
  TabGroup,
  TabList,
  TabPanel,
  TabPanels,
} from "@headlessui/react";
import { MdCheck, MdKeyboardArrowDown } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import { useTabCounts } from "@/context/tabCountsContext";

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

const EntityTabs = ({ tabs: rawTabs, defaultTabId }: Props) => {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { counts: dynamicCounts } = useTabCounts();

  // Merge dynamic counts published by tab contents (via
  // usePublishTabCount) over the static server-prefetched counts, so
  // the badge reflects the current filtered view.
  const tabs: TabSpec[] = rawTabs.map((t) => {
    const dyn = dynamicCounts[t.id];
    if (dyn === undefined || dyn === null) return t;
    return { ...t, count: dyn };
  });

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
      {/* Mobile: HeadlessUI Listbox replaces the chip row — native
       * <select> on iOS Safari doesn't anchor the popup precisely
       * left, and we can't fix that from CSS. Listbox gives full
       * control of trigger and popup positioning. Options carry the
       * count as "· 42" (matching the chip badges) and zero-count
       * options are disabled. */}
      <div className="sm:hidden mb-2 pl-1">
        <Listbox value={selectedIndex} onChange={handleChange}>
          <div className="relative">
            <ListboxButton className="w-full font-mono italic text-sm font-medium bg-light-200 text-light-900 rounded-md pl-3 pr-9 py-2 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] focus:outline-none focus:ring-1 focus:ring-accent-500 text-left">
              <span className="flex items-center gap-1.5">
                <span>{tabs[selectedIndex]?.label ?? ""}</span>
                {typeof tabs[selectedIndex]?.count === "number" && (
                  <span className="text-light-700 not-italic">
                    · {formatCount(tabs[selectedIndex]!.count!)}
                  </span>
                )}
              </span>
              <MdKeyboardArrowDown
                aria-hidden
                className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 w-5 h-5 text-light-900"
              />
            </ListboxButton>
            <ListboxOptions
              anchor="bottom start"
              className="mt-1 w-[var(--button-width)] rounded-md border border-light-700/60 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
            >
              {tabs.map((tab, i) => (
                <ListboxOption
                  key={tab.id}
                  value={i}
                  disabled={tab.count === 0}
                  className="group flex items-center gap-2 px-3 py-2 font-mono italic text-sm text-light-200 data-[focus]:bg-light-900/60 data-[selected]:text-light-100 data-[disabled]:opacity-40 data-[disabled]:cursor-not-allowed cursor-pointer"
                >
                  <MdCheck className="w-4 h-4 opacity-0 group-data-[selected]:opacity-100 text-accent-500" />
                  <span>{tab.label}</span>
                  {typeof tab.count === "number" && (
                    <span className="text-light-500 not-italic">
                      · {formatCount(tab.count)}
                    </span>
                  )}
                </ListboxOption>
              ))}
            </ListboxOptions>
          </div>
        </Listbox>
      </div>

      <TabList className="hidden sm:flex items-end gap-1.5 pl-3">
        {tabs.map((tab) => (
          <Tab
            key={tab.id}
            className={({ selected }) =>
              twMerge(
                // Explicit height locks the tab strip against badge/
                // no-badge height perception drift. -mb-[1.5px] pulls
                // tabs down exactly the card border's thickness so
                // they cover the card's top border in the tab's width
                // and don't leave side-border "legs" hanging into the
                // card content.
                "relative z-10 flex items-center justify-center h-7 md:h-8 px-3 md:px-4",
                "font-mono italic text-[11px] md:text-xs min-w-[7rem] md:min-w-[9.5rem] font-medium",
                "rounded-t-lg transition-colors outline-none border-t-[1.5px] border-x-[1.5px]",
                selected
                  ? "bg-light-200 text-light-900 border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)]"
                  : // Border tone matched to the Card's light-50/[0.08]
                    // so side edges read as one continuous line with
                    // the cabinet's top border instead of clashing.
                    "bg-light-950/50 border-light-50/[0.15] text-light-300 hover:text-light-100 hover:border-light-50/25 hover:bg-light-900/70"
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

"use client";

import { ReactNode, useEffect, useRef, useState } from "react";
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
import Skeleton from "@/components/basic/Skeleton";
import { usePaginations } from "@/context/paginationsContext";
import { useTabCounts } from "@/context/tabCountsContext";

// Owned by the (React-free, server-readable) config so `loading.tsx` can
// import it without pulling this client component across the boundary.
// Re-exported here because most callers reach for it alongside TabSpec.
import type { EntityType } from "@/components/entities/entityTabs.config";

export type { EntityType };

export type TabSpec = {
  id: string;
  label: string;
  // Whether this tab ever carries a count badge. Supplied by the shared
  // config via buildTabs, and distinct from `count == null` — that means
  // the count is still pending, which is what the placeholder covers.
  hasCount?: boolean;
  // The badge count itself; null while the tab's fetch is in flight.
  count?: number | null;
  content: ReactNode;
};

// Badges are a glanceable magnitude, not a figure to read off — k notation
// keeps them narrow and uniform. This is the only place tab counts are
// rendered (all three breakpoints below call it), so every tab on every
// entity abbreviates the same way by construction.
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
  const { resetAllPaginations } = usePaginations();

  // Merge dynamic counts published by tab contents (via
  // usePublishTabCount) over the static server-prefetched counts, so
  // the badge reflects the current filtered view.
  const tabs: TabSpec[] = rawTabs.map((t) => {
    const dyn = dynamicCounts[t.id];
    if (dyn === undefined || dyn === null) return t;
    return { ...t, count: dyn };
  });

  // The chip row is the nicer control but only while it fits. Entity
  // pages carry labels like "Chemicals (assay-inferred)", and at four or
  // five tabs the strip runs past the card on narrower desktop windows.
  // When that happens we fall back to the same Listbox mobile uses.
  //
  // Measuring this needs care: hiding the strip with `display: none`
  // would zero its width, which reads as "fits", which shows it again —
  // a flip-flop. So when it overflows the strip stays laid out and is
  // taken out of flow with `absolute invisible`, keeping scrollWidth
  // meaningful and the comparison stable.
  const tabStripRef = useRef<HTMLDivElement>(null);
  const tabStripWrapRef = useRef<HTMLDivElement>(null);
  const [stripOverflows, setStripOverflows] = useState(false);

  useEffect(() => {
    const measure = () => {
      const strip = tabStripRef.current;
      const wrap = tabStripWrapRef.current;
      if (!strip || !wrap) return;
      // Only meaningful once the wrapper is actually displayed; on mobile
      // it is `hidden`, clientWidth is 0, and the Listbox already shows.
      if (wrap.clientWidth === 0) return;
      // Asymmetric thresholds: flip to the Listbox as soon as the strip
      // genuinely overflows (1px of slack absorbs sub-pixel rounding,
      // which otherwise reports a permanent 0.5px overflow at some zoom
      // levels), but only flip back once there is real room to spare.
      // Equal thresholds let a width that lands exactly on the boundary
      // oscillate, since each flip changes what is being measured.
      setStripOverflows((prev) =>
        prev
          ? strip.scrollWidth > wrap.clientWidth - 8
          : strip.scrollWidth > wrap.clientWidth + 1
      );
    };
    measure();
    const ro = new ResizeObserver(measure);
    if (tabStripWrapRef.current) ro.observe(tabStripWrapRef.current);
    return () => ro.disconnect();
    // Counts change label widths, so re-measure when they land.
  }, [tabs]);

  // Derive initial index from URL, then hold local state so the tab
  // switches immediately on click. Previously this was derived from
  // useSearchParams on every render, but `router.replace` doesn't
  // reliably update useSearchParams synchronously inside Headless UI's
  // controlled TabGroup — the tab would visually "revert" and the user
  // had to click twice. Local state avoids the round-trip.
  const urlId = searchParams.get("tab") ?? defaultTabId;
  const urlIdx = tabs.findIndex((t) => t.id === urlId);
  const [selectedIndex, setSelectedIndex] = useState(urlIdx >= 0 ? urlIdx : 0);

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
    // Each tab is its own view, so it should open in its default state
    // rather than inheriting whatever was left behind. Panels unmount on
    // switch (below), which clears their filter state; page state lives
    // in a context that survives unmount, so clear it explicitly —
    // otherwise a table returns with reset filters but on page 7.
    resetAllPaginations();
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
      <div className={twMerge("mb-2 pl-1", !stripOverflows && "sm:hidden")}>
        <Listbox value={selectedIndex} onChange={handleChange}>
          <div className="relative">
            <ListboxButton className="w-full font-mono italic text-sm font-medium bg-light-200 text-light-900 rounded-md pl-3 pr-9 py-2 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] focus:outline-none focus:ring-1 focus:ring-accent-500 text-left">
              <span className="flex items-center gap-1.5">
                <span>{tabs[selectedIndex]?.label ?? ""}</span>
                {tabs[selectedIndex]?.hasCount &&
                  (typeof tabs[selectedIndex]?.count === "number" ? (
                    <span className="text-light-700 not-italic">
                      · {formatCount(tabs[selectedIndex]!.count!)}
                    </span>
                  ) : (
                    <Skeleton shape="pill" className="h-3 w-6" />
                  ))}
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
                  disabled={tab.hasCount === true && tab.count === 0}
                  className="group flex items-center gap-2 px-3 py-2 font-mono italic text-sm text-light-200 data-[focus]:bg-light-900/60 data-[selected]:text-light-100 data-[disabled]:opacity-40 data-[disabled]:cursor-not-allowed cursor-pointer"
                >
                  <MdCheck className="w-4 h-4 opacity-0 group-data-[selected]:opacity-100 text-accent-500" />
                  <span>{tab.label}</span>
                  {tab.hasCount &&
                    (typeof tab.count === "number" ? (
                      <span className="text-light-500 not-italic">
                        · {formatCount(tab.count)}
                      </span>
                    ) : (
                      <Skeleton shape="pill" className="h-3 w-6" />
                    ))}
                </ListboxOption>
              ))}
            </ListboxOptions>
          </div>
        </Listbox>
      </div>

      <div ref={tabStripWrapRef} className="hidden sm:block relative">
        <TabList
          ref={tabStripRef}
          className={twMerge(
            // w-max so the strip reports its natural width instead of
            // shrinking to the container, which is what makes overflow
            // detectable at all.
            "flex w-max items-end gap-1.5 pl-3",
            stripOverflows && "invisible absolute pointer-events-none",
          )}
        >
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
                      "bg-light-950/50 border-light-50/[0.15] text-light-300 hover:text-light-100 hover:border-light-50/25 hover:bg-light-900/70",
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
                  {tab.hasCount &&
                    (typeof tab.count === "number" ? (
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
                    ) : (
                      // Same box as the real badge, so the chip's width is
                      // final from first paint. Without this the badge
                      // popping in widens the chip, which re-runs the
                      // overflow observer above and can flip the whole
                      // strip into the mobile Listbox mid-load.
                      <Skeleton shape="pill" className="h-[0.95rem] w-6" />
                    ))}
                </span>
              )}
            </Tab>
          ))}
        </TabList>
      </div>

      <Card>
        <TabPanels>
          {tabs.map((tab) => (
            <TabPanel
              key={tab.id}
              // Unmount inactive panels so each tab starts fresh: filter
              // state lives in the sections' useState and resets with
              // them. Also means only the visible tab runs its fetches on
              // page load, instead of every tab fetching at once.
              unmount
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

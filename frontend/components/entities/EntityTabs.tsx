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
import {
  TAB_BADGE_W,
  TAB_STRIP_FITS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

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

// Badge geometry, declared once and shared with the loading placeholder
// below. The widest string formatCount emits in practice is four
// characters ("1.5k"), which at text-[0.65rem] mono plus px-1.5 needs
// ~2.3rem; a three-digit count needs ~1.9rem. A FIXED width — not a
// min-width — because the chip is a flex row: a badge that can grow
// makes the label the thing that gives, and a long label like
// "Diseases (assay-inferred)" then wraps onto a second line and
// changes the chip's height.
//
// `shrink-0` for the same reason, and the label carries
// `whitespace-nowrap` so the chip grows past its min-width instead of
// breaking the text. The strip's overflow observer already handles a
// strip that outgrows its container by switching to the mobile Listbox.
//
// The placeholder MUST use the same value. It previously reserved w-6
// (1.5rem) against a real badge of 1.9–2.3rem, so the chip still grew
// when the count landed — the exact reflow the placeholder exists to
// prevent.
//
// Lives in the shared config because the loading shell reserves the same
// box; when the two disagreed, every chip resized at the handoff.
const BADGE_W = TAB_BADGE_W;

// The inline "· 1.5k" form used by the mobile Listbox renders at text-sm.
// It sits in a full-width button rather than a fixed chip, so it only
// needs to not wrap.
const INLINE_COUNT_W = "shrink-0 whitespace-nowrap";

interface Props {
  entityType: EntityType;
  tabs: TabSpec[];
  defaultTabId: string;
}

const EntityTabs = ({ entityType, tabs: rawTabs, defaultTabId }: Props) => {
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

  // Strip vs Listbox is a CSS breakpoint, not a measurement — see
  // TAB_STRIP_FITS. It has to be, so that this component's first paint and
  // the server-rendered loading shell agree at every width.
  const fits = TAB_STRIP_FITS[entityType];

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
      <div className={twMerge("mb-2 pl-1", fits.select)}>
        <Listbox value={selectedIndex} onChange={handleChange}>
          <div className="relative">
            <ListboxButton className="w-full font-mono italic text-sm font-medium bg-light-200 text-light-900 rounded-md pl-3 pr-9 py-2 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] focus:outline-none focus:ring-1 focus:ring-accent-500 text-left">
              <span className="flex items-center gap-1.5">
                <span>{tabs[selectedIndex]?.label ?? ""}</span>
                {tabs[selectedIndex]?.hasCount &&
                  (typeof tabs[selectedIndex]?.count === "number" ? (
                    <span
                      className={`text-light-700 not-italic ${INLINE_COUNT_W}`}
                    >
                      · {formatCount(tabs[selectedIndex]!.count!)}
                    </span>
                  ) : (
                    <Skeleton
                      shape="pill"
                      className={`h-3 ${INLINE_COUNT_W}`}
                    />
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
                      <span
                        className={`text-light-500 not-italic ${INLINE_COUNT_W}`}
                      >
                        · {formatCount(tab.count)}
                      </span>
                    ) : (
                      <Skeleton
                        shape="pill"
                        className={`h-3 ${INLINE_COUNT_W}`}
                      />
                    ))}
                </ListboxOption>
              ))}
            </ListboxOptions>
          </div>
        </Listbox>
      </div>

      {/* The breakpoint is measured at a 16px root font. A reader with a
        * larger default font scales the chips (rem widths) without
        * changing the viewport in CSS px, so the strip can need more room
        * than the number assumes. Scrolling it beats pushing the page
        * wider. No effect at the default font, where it never overflows.
        * Safe as a scroll container: the chips carry no negative margin
        * and their shadows are inset, so nothing bleeds out to be clipped. */}
      <div className={twMerge(fits.stripBlock, "relative overflow-x-auto")}>
        <TabList
          data-tab-strip
          className="flex w-max items-end gap-1.5 px-3"
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
                  "relative z-10 flex items-center justify-center h-7 md:h-8 px-2 md:px-3",
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
                  <span className="leading-none whitespace-nowrap">
                    {tab.label}
                  </span>
                  {tab.hasCount &&
                    (typeof tab.count === "number" ? (
                      <span
                        className={
                          "not-italic font-mono text-[0.65rem] tracking-wide py-[1px] rounded-full " +
                          `inline-flex items-center justify-center ${BADGE_W} ` +
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
                      <Skeleton
                        shape="pill"
                        className={`h-[0.95rem] ${BADGE_W}`}
                      />
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

"use client";

import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import { useEffect, useState } from "react";
import { MdCheck, MdKeyboardArrowDown } from "react-icons/md";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

export interface SubnavTabSpec {
  id: string;
  label: string;
  count?: number | null;
}

interface Props {
  tabs: SubnavTabSpec[];
  defaultTabId: string;
}

const formatCount = (n: number): string => {
  if (n >= 10000) return `${(n / 1000).toFixed(0)}k`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return n.toLocaleString();
};

// Compact Listbox that lives inside <EntitySubnavbar>. Reads the current
// tab from `?tab=` and writes back on change. Uses local state mirroring
// the URL to avoid the Headless UI "click and it reverts" issue caused by
// router.replace's async searchParams update — same pattern as
// <EntityTabs>. Dynamic per-filter counts (TabCountsContext) intentionally
// aren't consumed here: this selector shows the server-prefetched summary
// so it stays legible without the tab panels being mounted.
const SubnavbarTabSelector = ({ tabs, defaultTabId }: Props) => {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const urlId = searchParams.get("tab") ?? defaultTabId;
  const urlIdx = tabs.findIndex((t) => t.id === urlId);
  const [selectedIndex, setSelectedIndex] = useState(urlIdx >= 0 ? urlIdx : 0);

  useEffect(() => {
    if (urlIdx >= 0 && urlIdx !== selectedIndex) setSelectedIndex(urlIdx);
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

  const current = tabs[selectedIndex];
  if (!current) return null;

  return (
    <Listbox value={selectedIndex} onChange={handleChange}>
      <div className="relative">
        <ListboxButton className="flex items-center gap-1 font-mono italic text-[0.72rem] md:text-xs font-medium bg-light-200 text-light-900 rounded-md pl-2.5 pr-1 py-0.5 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] focus:outline-none focus:ring-1 focus:ring-accent-500 whitespace-nowrap">
          <span>{current.label}</span>
          {typeof current.count === "number" && (
            <span className="text-light-700 not-italic">
              · {formatCount(current.count)}
            </span>
          )}
          <MdKeyboardArrowDown
            aria-hidden
            className="w-4 h-4 text-light-900"
          />
        </ListboxButton>
        <ListboxOptions
          anchor="bottom end"
          className="mt-1 min-w-[var(--button-width)] rounded-md border border-light-700/60 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
        >
          {tabs.map((tab, i) => (
            <ListboxOption
              key={tab.id}
              value={i}
              disabled={tab.count === 0}
              className="group flex items-center gap-2 px-3 py-2 font-mono italic text-sm text-light-200 data-[focus]:bg-light-900/60 data-[selected]:text-light-100 data-[disabled]:opacity-40 data-[disabled]:cursor-not-allowed cursor-pointer whitespace-nowrap"
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
  );
};

SubnavbarTabSelector.displayName = "SubnavbarTabSelector";
export default SubnavbarTabSelector;

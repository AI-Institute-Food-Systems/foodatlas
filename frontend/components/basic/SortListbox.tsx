"use client";

// Reusable sort dropdown built on HeadlessUI Listbox — replaces the
// native <select> on the mobile card-view row-count/sort header. iOS
// Safari can't anchor the native popup precisely to the trigger; a
// Listbox lets us position the popup ourselves. Same visual language
// as the EntityTabs Listbox but sized as a small monospaced pill.

import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import { MdCheck, MdKeyboardArrowDown } from "react-icons/md";

export type SortOption = {
  value: string;
  label: string;
};

type Props = {
  value: string;
  options: SortOption[];
  onChange: (value: string) => void;
  ariaLabel?: string;
};

const SortListbox = ({ value, options, onChange, ariaLabel }: Props) => {
  const current = options.find((o) => o.value === value);
  return (
    <Listbox value={value} onChange={onChange}>
      <div className="relative">
        <ListboxButton
          aria-label={ariaLabel ?? "Sort"}
          className="inline-flex items-center gap-1 rounded-md border border-light-700/60 bg-light-900/60 pl-2 pr-6 py-1 text-xs font-mono italic text-light-200 focus:outline-none focus:ring-1 focus:ring-accent-500 text-left"
        >
          <span>{current?.label ?? ""}</span>
          <MdKeyboardArrowDown
            aria-hidden
            className="pointer-events-none absolute right-1.5 top-1/2 -translate-y-1/2 w-4 h-4 text-light-400"
          />
        </ListboxButton>
        <ListboxOptions
          anchor="bottom end"
          className="mt-1 min-w-[var(--button-width)] rounded-md border border-light-700/60 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
        >
          {options.map((opt) => (
            <ListboxOption
              key={opt.value}
              value={opt.value}
              className="group flex items-center gap-2 px-3 py-2 font-mono italic text-xs text-light-200 data-[focus]:bg-light-900/60 data-[selected]:text-light-100 cursor-pointer whitespace-nowrap"
            >
              <MdCheck className="w-3.5 h-3.5 opacity-0 group-data-[selected]:opacity-100 text-accent-500" />
              <span>{opt.label}</span>
            </ListboxOption>
          ))}
        </ListboxOptions>
      </div>
    </Listbox>
  );
};

SortListbox.displayName = "SortListbox";
export default SortListbox;

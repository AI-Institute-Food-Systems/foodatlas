"use client";

// THE filter-sidebar control set. Every filter panel in the app builds from
// these — see FilterPanel.tsx for the chrome that wraps them.
//
// This exists because the same three controls were implemented four times:
// FilterListItem in FoodCompositionSection, CheckRow and RadioRow in
// BioactivityMeasurementsModal, and Chip pills in ChemicalCompositionToolbar.
// Same job, four affordances, and each new panel picked whichever it happened
// to see first. A shared module is the only thing that makes "consistent"
// hold without someone re-checking it every time a panel is added.
//
// Adding a control? Put it here. `filter-panel-convention.test.ts` fails the
// build if filter-row markup shows up anywhere else.

import { ReactNode } from "react";
import { Switch } from "@headlessui/react";
import { MdCheck, MdClose, MdSearch } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Skeleton from "@/components/basic/Skeleton";

const FilterRowLabel = ({ children }: { children: ReactNode }) => (
  <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
    {children}
  </span>
);

// A labelled section in the sidebar.
//
// `onClear` renders the group-level clear link, for multi-select groups long
// enough that unpicking by hand is tedious (food's 15+ Class options, the
// modal's Evidence types). It is a first-class prop rather than a freeform
// `action` slot because both of those sites had hand-rolled their own link
// with slightly different markup — the generic slot was what let them drift.
// It complements ResetFiltersButton rather than competing: this clears one
// group, that clears the panel.
const FilterGroup = ({
  label,
  onClear,
  children,
}: {
  label: string;
  onClear?: () => void;
  children: ReactNode;
}) => (
  <div className="flex flex-col gap-1.5">
    <div className="flex items-baseline justify-between gap-2">
      <FilterRowLabel>{label}</FilterRowLabel>
      {onClear && (
        <button
          type="button"
          onClick={onClear}
          className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
        >
          clear
        </button>
      )}
    </div>
    {children}
  </div>
);

// Include/exclude switch. Distinct from FilterOption on purpose: a toggle
// widens the result set, a facet narrows it. The label also wraps, which a
// pill does not — "Without concentration" overflowed the w-48 sidebar as a
// Chip.
const ToggleSwitch = ({
  label,
  count,
  checked,
  onChange,
}: {
  label: string;
  count?: number;
  checked: boolean;
  onChange: () => void;
}) => (
  <label className="flex items-center gap-2 cursor-pointer select-none">
    <Switch
      checked={checked}
      onChange={onChange}
      className="group inline-flex h-4 w-8 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 flex-shrink-0 transition-colors"
    >
      <span className="size-3 translate-x-0.5 rounded-full bg-white transition group-data-[checked]:translate-x-[1.125rem]" />
    </Switch>
    <span
      className={twMerge(
        "text-xs transition-colors flex-1 min-w-0 leading-tight",
        checked ? "text-light-100" : "text-light-400"
      )}
    >
      {label}
    </span>
    {typeof count === "number" && (
      <span
        className={twMerge(
          "tabular-nums text-[10px] flex-shrink-0",
          checked ? "text-light-400" : "text-light-500"
        )}
      >
        {count.toLocaleString()}
      </span>
    )}
  </label>
);

// Wraps a run of FilterOptions. `maxHeightClass` caps tall facet lists.
const FilterOptionList = ({
  mode = "check",
  ariaLabel,
  maxHeightClass,
  children,
}: {
  // Mirrors the FilterOption mode used inside, so the list announces itself
  // as a radiogroup when its options are mutually exclusive.
  mode?: "check" | "radio";
  ariaLabel?: string;
  maxHeightClass?: string;
  children: ReactNode;
}) => (
  <div
    role={mode === "radio" ? "radiogroup" : undefined}
    aria-label={mode === "radio" ? ariaLabel : undefined}
    className={twMerge(
      "flex flex-col -mx-1",
      maxHeightClass ? `${maxHeightClass} overflow-y-auto` : undefined
    )}
  >
    {children}
  </div>
);

// One facet row: full-width click target, affordance left, label, count right.
//
// `mode` picks the affordance only — a square tick for multi-select, a dot for
// single-select. Behaviour is identical, which is why CheckRow and RadioRow
// collapsed into one component.
//
// A zero-count option renders disabled rather than hidden, so the facet list
// keeps its shape across pivot entities instead of reshuffling.
const FilterOption = ({
  label,
  count,
  selected,
  onClick,
  disabled,
  mode = "check",
  capitalize = true,
  countsLoaded = true,
}: {
  label: string;
  count?: number;
  selected: boolean;
  onClick: () => void;
  disabled?: boolean;
  mode?: "check" | "radio";
  // Off for case-significant labels. Units are the reason this exists:
  // `capitalize` turns uM into UM and ug/mL into Ug/mL. Prose labels
  // ("in vitro") read as typos without it, so it defaults on.
  capitalize?: boolean;
  // False while facet counts are in flight. Distinguishes "not fetched yet"
  // from "genuinely zero" — without it a row shows no count, stays enabled,
  // then greys out under the cursor when the counts land.
  countsLoaded?: boolean;
}) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    role={mode === "radio" ? "radio" : undefined}
    aria-checked={mode === "radio" ? selected : undefined}
    aria-pressed={mode === "check" ? selected : undefined}
    aria-disabled={disabled || undefined}
    className={twMerge(
      "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left",
      selected
        ? "text-light-100 hover:bg-light-900/70"
        : "text-light-400 hover:text-light-100 hover:bg-light-900/50",
      disabled &&
        "opacity-40 cursor-not-allowed hover:bg-transparent hover:text-light-400"
    )}
  >
    <span
      aria-hidden
      className={twMerge(
        "w-3.5 h-3.5 border flex-shrink-0 flex items-center justify-center transition-colors",
        mode === "radio" ? "rounded-full" : "rounded-[3px]",
        selected
          ? "border-accent-600 bg-accent-600/20 text-accent-600"
          : "border-light-700 group-hover:border-light-500",
        disabled && "group-hover:border-light-700"
      )}
    >
      {selected &&
        (mode === "radio" ? (
          <span className="size-1.5 rounded-full bg-accent-600" />
        ) : (
          <MdCheck className="w-3 h-3" />
        ))}
    </span>
    <span
      className={twMerge(
        "font-mono italic text-xs flex-1 min-w-0 truncate",
        capitalize && "capitalize"
      )}
    >
      {label}
    </span>
    {typeof count === "number" ? (
      <span
        className={twMerge(
          "not-italic tabular-nums text-[10px] flex-shrink-0",
          selected ? "text-light-400" : "text-light-500"
        )}
      >
        {count.toLocaleString()}
      </span>
    ) : (
      // Reserve the slot so the label doesn't reflow when the number lands.
      !countsLoaded && <Skeleton className="h-3 w-5 flex-shrink-0" />
    )}
  </button>
);

// THE search box for every filter panel. Seven copies of this existed; six
// agreed and the chemical composition one was a `rounded-full bg-light-800`
// pill with no clear button, so the same page's Foods and Bioactivities tabs
// showed visibly different controls.
const FilterSearchInput = ({
  value,
  onChange,
  onClear,
  placeholder = "Search…",
  ariaLabel,
  disabled,
}: {
  value: string;
  onChange: (v: string) => void;
  onClear: () => void;
  placeholder?: string;
  // Defaults to the placeholder, which is what every copy did by hand.
  ariaLabel?: string;
  disabled?: boolean;
}) => (
  <div className="relative flex items-center">
    <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
    <input
      className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none disabled:opacity-60"
      type="text"
      placeholder={placeholder}
      aria-label={ariaLabel ?? placeholder}
      value={value}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
    />
    {value && (
      <button
        type="button"
        aria-label="Clear search"
        onClick={onClear}
        className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
      >
        <MdClose className="w-3 h-3" />
      </button>
    )}
  </div>
);

// The empty-state escape hatch: shown INSIDE a table whose filters matched
// nothing, not in the panel. Distinct from ResetFiltersButton — that is a
// panel control you go looking for, this is offered at the moment you hit a
// dead end. Shared because all three tables had rebuilt it identically.
const ClearFiltersLink = ({ onClick }: { onClick: () => void }) => (
  <button
    type="button"
    onClick={onClick}
    className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
  >
    clear filters
  </button>
);

FilterRowLabel.displayName = "FilterRowLabel";
ClearFiltersLink.displayName = "ClearFiltersLink";
FilterSearchInput.displayName = "FilterSearchInput";
FilterGroup.displayName = "FilterGroup";
ToggleSwitch.displayName = "ToggleSwitch";
FilterOptionList.displayName = "FilterOptionList";
FilterOption.displayName = "FilterOption";

export {
  FilterRowLabel,
  FilterGroup,
  ToggleSwitch,
  FilterOptionList,
  FilterOption,
  FilterSearchInput,
  ClearFiltersLink,
};

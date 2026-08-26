"use client";

import { ReactNode } from "react";
import { Switch } from "@headlessui/react";
import { twMerge } from "tailwind-merge";

// The filter-sidebar primitives, shared by the food and chemical
// composition tables so the two sidebars stay the same control rather than
// two lookalikes. They were local to FoodCompositionSection until the
// chemical table grew a sidebar of its own; a second copy is exactly how
// the skeleton tokens and TableSkeleton diverged earlier in this branch set.

const FilterRowLabel = ({ children }: { children: ReactNode }) => (
  <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
    {children}
  </span>
);

const ToggleSwitch = ({
  label,
  count,
  checked,
  onChange,
}: {
  label: string;
  // Count of rows the toggle governs (e.g. rows without concentration).
  // Rendered as a right-aligned mono badge; omitted when undefined.
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

// A labelled section in the filter sidebar. The optional `action` sits in
// the label row (right-aligned) — used by Class for the "all / clear"
// button so it doesn't need its own row.
const FilterGroup = ({
  label,
  action,
  children,
}: {
  label: string;
  action?: ReactNode;
  children: ReactNode;
}) => (
  <div className="flex flex-col gap-1.5">
    <div className="flex items-baseline justify-between gap-2">
      <FilterRowLabel>{label}</FilterRowLabel>
      {action}
    </div>
    {children}
  </div>
);

FilterRowLabel.displayName = "FilterRowLabel";
ToggleSwitch.displayName = "ToggleSwitch";
FilterGroup.displayName = "FilterGroup";

export { FilterRowLabel, ToggleSwitch, FilterGroup };

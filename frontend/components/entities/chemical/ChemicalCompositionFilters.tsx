"use client";

import { ReactNode } from "react";
import { MdClose, MdTune } from "react-icons/md";

import Card from "@/components/basic/Card";
import { CompositionSearchInput } from "@/components/entities/chemical/ChemicalCompositionToolbar";

// Where the composition table's filters live at each width. Split out of
// ChemicalCompositionTable purely for the 300-line rule; it is one concern.
//
// The same `filterPanel` node is rendered into both the sidebar and the
// drawer rather than being built twice, so the two cannot drift.

interface Props {
  search: string;
  onSearchChange: (value: string) => void;
  filterPanel: ReactNode;
  mobileOpen: boolean;
  onMobileOpenChange: (open: boolean) => void;
}

const ChemicalCompositionFilters = ({
  search,
  onSearchChange,
  filterPanel,
  mobileOpen,
  onMobileOpenChange,
}: Props) => (
  <>
    {/* Filters sit to the LEFT of the table, matching every other table in
      * the app. Absolutely positioned off the tab Card via `right-full` so
      * the table keeps its full centred width, and only from min-[1440px]
      * where the max-w-5xl gutter has room for a w-48 aside; the drawer
      * covers narrower viewports. */}
    <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
      <div className="sticky top-4">
        <Card className="gap-3">
          <CompositionSearchInput
            search={search}
            onSearchChange={onSearchChange}
          />
          {filterPanel}
        </Card>
      </div>
    </aside>

    {/* Below that breakpoint search stays inline — typing should never
      * require opening a panel — and the rest goes behind a trigger. */}
    <div className="min-[1440px]:hidden flex items-center gap-3">
      <div className="flex-1 min-w-0 max-w-xs">
        <CompositionSearchInput
          search={search}
          onSearchChange={onSearchChange}
        />
      </div>
      <button
        type="button"
        onClick={() => onMobileOpenChange(true)}
        className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
      >
        <MdTune className="w-4 h-4" />
        Filters
      </button>
    </div>

    {mobileOpen && (
      <div
        className="fixed inset-0 z-50 min-[1440px]:hidden"
        role="dialog"
        aria-modal="true"
        aria-label="Filters"
      >
        <button
          type="button"
          aria-label="Close filters"
          onClick={() => onMobileOpenChange(false)}
          className="absolute inset-0 bg-black/60 cursor-default"
        />
        <aside className="absolute right-0 top-0 h-full w-[85vw] max-w-sm bg-light-950 border-l border-light-700/50 overflow-y-auto flex flex-col gap-4 p-4">
          <div className="flex items-center justify-between">
            <span className="font-mono italic text-sm text-light-300">
              Filters
            </span>
            <button
              type="button"
              aria-label="Close filters"
              onClick={() => onMobileOpenChange(false)}
              className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
            >
              <MdClose className="w-4 h-4" />
            </button>
          </div>
          {filterPanel}
        </aside>
      </div>
    )}
  </>
);

ChemicalCompositionFilters.displayName = "ChemicalCompositionFilters";

export default ChemicalCompositionFilters;

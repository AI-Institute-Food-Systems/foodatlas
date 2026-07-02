"use client";

// Shared search + source-kind filter chrome for BOTH the direct
// FoodBioactivitiesSection and the FoodInferredBioactivitiesSection.
// Instead of each section owning its own sidebar (as they do
// standalone), this component hosts one sidebar aside + one mobile
// drawer and drives both tables via `externalSearch` /
// `externalSourceKind` / `hideChrome` props.

import { useState } from "react";
import { MdCheck, MdClose, MdSearch, MdTune } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";

interface Props {
  commonName: string;
  anchorId?: string | null;
}

const SOURCE_KINDS = [
  { key: "experimental", label: "experimental" },
  { key: "predicted", label: "predicted" },
  { key: "mixed", label: "mixed" },
] as const;

const FoodBioactivitiesTab = ({ commonName, anchorId }: Props) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedSourceKinds, setSelectedSourceKinds] = useState<string[]>([]);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  const sourceKindParam = selectedSourceKinds.join("+");

  const toggleSourceKind = (kind: string) => {
    setSelectedSourceKinds((prev) =>
      prev.includes(kind) ? prev.filter((k) => k !== kind) : [...prev, kind]
    );
  };
  const clearSourceKinds = () => setSelectedSourceKinds([]);

  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
        type="text"
        placeholder="Search…"
        aria-label="Search bioactivity or chemical"
        value={searchTerm}
        onChange={(e) => setSearchTerm(e.target.value.toLowerCase())}
      />
      {searchTerm && (
        <button
          type="button"
          aria-label="Clear search"
          onClick={() => setSearchTerm("")}
          className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
        >
          <MdClose className="w-3 h-3" />
        </button>
      )}
    </div>
  );

  const sourceFilter = (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
          Source
        </span>
        {selectedSourceKinds.length > 0 && (
          <button
            type="button"
            onClick={clearSourceKinds}
            className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
          >
            clear
          </button>
        )}
      </div>
      <div className="flex flex-col -mx-1">
        {SOURCE_KINDS.map(({ key, label }) => {
          const selected = selectedSourceKinds.includes(key);
          return (
            <button
              key={key}
              type="button"
              onClick={() => toggleSourceKind(key)}
              aria-pressed={selected}
              className={twMerge(
                "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left",
                selected
                  ? "text-light-100 hover:bg-light-900/70"
                  : "text-light-400 hover:text-light-100 hover:bg-light-900/50"
              )}
            >
              <span
                aria-hidden
                className={twMerge(
                  "w-3.5 h-3.5 rounded-[3px] border flex-shrink-0 flex items-center justify-center transition-colors",
                  selected
                    ? "border-accent-600 bg-accent-600/20 text-accent-600"
                    : "border-light-700 group-hover:border-light-500"
                )}
              >
                {selected && <MdCheck className="w-3 h-3" />}
              </span>
              <span className="font-mono italic text-xs capitalize flex-1">
                {label}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );

  const filterPanel = (
    <div className="flex flex-col gap-5">
      {searchInput}
      {sourceFilter}
    </div>
  );

  return (
    <div className="relative flex flex-col gap-12">
      {/* Desktop shared sidebar for BOTH tables — same geometry as
       * FoodCompositionSection / BioactivityTable. */}
      <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
        <div className="sticky top-4">
          <Card>{filterPanel}</Card>
        </div>
      </aside>

      {/* Sub-1440 row: search visible on the left, Filters button on
       * the right. Drawer holds the source filter. */}
      <div className="min-[1440px]:hidden flex items-center gap-3">
        <div className="flex-1 min-w-0 max-w-xs">{searchInput}</div>
        <button
          type="button"
          onClick={() => setMobileFiltersOpen(true)}
          className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
        >
          <MdTune className="w-4 h-4" />
          Filters
        </button>
      </div>

      <FoodBioactivitiesSection
        commonName={commonName}
        anchorId={anchorId}
        externalSearch={searchTerm}
        externalSourceKind={sourceKindParam}
        hideChrome
      />
      <div className="border-t-2 border-double border-light-700/60" />
      <FoodInferredBioactivitiesSection
        commonName={commonName}
        externalSearch={searchTerm}
        externalSourceKind={sourceKindParam}
        hideChrome
      />

      {mobileFiltersOpen && (
        <div
          className="fixed inset-0 z-50 min-[1440px]:hidden"
          role="dialog"
          aria-modal="true"
          aria-label="Filters"
        >
          <button
            type="button"
            aria-label="Close filters"
            onClick={() => setMobileFiltersOpen(false)}
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
                onClick={() => setMobileFiltersOpen(false)}
                className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
              >
                <MdClose className="w-4 h-4" />
              </button>
            </div>
            {sourceFilter}
          </aside>
        </div>
      )}
    </div>
  );
};

FoodBioactivitiesTab.displayName = "FoodBioactivitiesTab";
export default FoodBioactivitiesTab;

"use client";

// Shared search + source-kind filter chrome for BOTH the direct
// FoodBioactivitiesSection and the FoodInferredBioactivitiesSection.
// Instead of each section owning its own sidebar (as they do
// standalone), this component hosts one sidebar aside + one mobile
// drawer and drives both tables via `externalSearch` /
// `externalSourceKind` / `hideChrome` props.

import { useEffect, useState } from "react";
import { MdCheck, MdClose, MdSearch, MdTune } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import {
  getBioactivityEndpointOptions,
  getBioactivityEvidenceTypeCounts,
  getBioactivitySourceKindCounts,
} from "@/utils/fetching";
import { usePublishTabCount } from "@/context/tabCountsContext";

interface Props {
  commonName: string;
  anchorId?: string | null;
}

const SOURCE_KINDS: { key: string; label: string }[] = [
  { key: "", label: "both" },
  { key: "experimental", label: "experimental" },
  { key: "predicted", label: "predicted" },
];

const TOP_UNITS = 5;

const FoodBioactivitiesTab = ({ commonName, anchorId }: Props) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedSourceKind, setSelectedSourceKind] = useState<string>("");
  const [selectedUnits, setSelectedUnits] = useState<string[]>([]);
  const [showAllUnits, setShowAllUnits] = useState(false);
  const [unitOptions, setUnitOptions] = useState<
    { unit: string; count: number }[]
  >([]);
  const [selectedEvidenceTypes, setSelectedEvidenceTypes] = useState<string[]>(
    []
  );
  const [evidenceTypeOptions, setEvidenceTypeOptions] = useState<
    { evidence_type: string; count: number }[]
  >([]);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // Aggregated filtered totals from direct + inferred tables → the
  // "Bioactivities" tab badge. Each sub-table reports null while its
  // fetch is in flight; we publish the SUM once at least one has
  // reported, so the badge starts refreshing as soon as data lands.
  const [directTotal, setDirectTotal] = useState<number | null>(null);
  const [inferredTotal, setInferredTotal] = useState<number | null>(null);
  const combinedTotal =
    directTotal === null && inferredTotal === null
      ? null
      : (directTotal ?? 0) + (inferredTotal ?? 0);
  usePublishTabCount("bioactivities", combinedTotal);

  // Source-kind counts for the sidebar Assay Source picker. Aggregated
  // across BOTH the direct (food-bioactivities) and inferred
  // (food-inferred-bioactivities) directions since the sidebar drives
  // both tables — same treatment as the Unit filter above.
  const [sourceKindCounts, setSourceKindCounts] = useState<{
    both: number;
    experimental: number;
    predicted: number;
  } | null>(null);
  useEffect(() => {
    if (!commonName) return;
    let cancelled = false;
    // Apply the tab's current search + unit filter (no category filter
    // on the food-bioactivities/inferred directions today) so the source
    // kind counts stay in sync with the visible tables.
    const filters = {
      filterUnit: selectedUnits.join("+"),
      search: searchTerm,
    };
    (async () => {
      const [direct, inferred] = await Promise.all([
        getBioactivitySourceKindCounts(
          commonName,
          "food-bioactivities",
          filters,
        ),
        getBioactivitySourceKindCounts(
          commonName,
          "food-inferred-bioactivities",
          filters,
        ),
      ]);
      if (cancelled) return;
      if (!direct && !inferred) {
        setSourceKindCounts(null);
        return;
      }
      setSourceKindCounts({
        both: (direct?.both ?? 0) + (inferred?.both ?? 0),
        experimental:
          (direct?.experimental ?? 0) + (inferred?.experimental ?? 0),
        predicted: (direct?.predicted ?? 0) + (inferred?.predicted ?? 0),
      });
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, selectedUnits, searchTerm]);

  const sourceKindParam = selectedSourceKind;
  const unitParam = selectedUnits.join("+");
  const evidenceTypeParam = selectedEvidenceTypes.join("+");

  // Aggregated evidence-type counts across BOTH tables (direct +
  // inferred). Each direction returns a list of {evidence_type, count};
  // we merge on evidence_type and re-sort by summed count.
  useEffect(() => {
    if (!commonName) return;
    let cancelled = false;
    (async () => {
      const [direct, inferred] = await Promise.all([
        getBioactivityEvidenceTypeCounts(commonName, "food-bioactivities"),
        getBioactivityEvidenceTypeCounts(
          commonName,
          "food-inferred-bioactivities"
        ),
      ]);
      if (cancelled) return;
      const totals = new Map<string, number>();
      for (const o of [...direct, ...inferred]) {
        const t = (o.evidence_type ?? "").trim();
        if (!t) continue;
        totals.set(t, (totals.get(t) ?? 0) + (o.count ?? 0));
      }
      setEvidenceTypeOptions(
        Array.from(totals.entries())
          .map(([evidence_type, count]) => ({ evidence_type, count }))
          .sort((a, b) => b.count - a.count)
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  // Aggregated unit list across BOTH tables — direct (food-level
  // measurements, usually just "mmol/100g") + inferred (all measurements
  // for every chemical present in this food, so IC50 uM/nM, MIC ug/mL,
  // etc). Fetches both directions and merges counts so the sidebar
  // surfaces the full spectrum of units the user might filter by.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const [direct, inferred] = await Promise.all([
        getBioactivityEndpointOptions(commonName, "food-bioactivities"),
        getBioactivityEndpointOptions(
          commonName,
          "food-inferred-bioactivities"
        ),
      ]);
      if (cancelled) return;
      const totals = new Map<string, number>();
      for (const o of [...direct, ...inferred]) {
        const u = (o.unit ?? "").trim();
        if (!u) continue;
        totals.set(u, (totals.get(u) ?? 0) + (o.count ?? 0));
      }
      setUnitOptions(
        Array.from(totals.entries())
          .map(([unit, count]) => ({ unit, count }))
          .sort((a, b) => b.count - a.count)
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  const chooseSourceKind = (kind: string) => setSelectedSourceKind(kind);
  const toggleUnit = (unit: string) => {
    setSelectedUnits((prev) =>
      prev.includes(unit) ? prev.filter((u) => u !== unit) : [...prev, unit]
    );
  };
  const clearUnits = () => setSelectedUnits([]);
  const toggleEvidenceType = (etype: string) => {
    setSelectedEvidenceTypes((prev) =>
      prev.includes(etype) ? prev.filter((e) => e !== etype) : [...prev, etype]
    );
  };
  const clearEvidenceTypes = () => setSelectedEvidenceTypes([]);
  const visibleUnits = showAllUnits
    ? unitOptions
    : unitOptions.slice(0, TOP_UNITS);
  const hiddenUnitsCount = Math.max(0, unitOptions.length - TOP_UNITS);

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
          Assay Source
        </span>
      </div>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Assay Source"
      >
        {SOURCE_KINDS.map(({ key, label }) => {
          const selected = selectedSourceKind === key;
          const c =
            sourceKindCounts === null
              ? undefined
              : key === ""
              ? sourceKindCounts.both
              : key === "experimental"
              ? sourceKindCounts.experimental
              : sourceKindCounts.predicted;
          const disabled = typeof c === "number" && key !== "" && c === 0;
          return (
            <button
              key={label}
              type="button"
              role="radio"
              aria-checked={selected}
              disabled={disabled}
              aria-disabled={disabled || undefined}
              onClick={() => chooseSourceKind(key)}
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
                  "w-3.5 h-3.5 rounded-full border flex-shrink-0 flex items-center justify-center transition-colors",
                  selected
                    ? "border-accent-600 bg-accent-600/20"
                    : "border-light-700 group-hover:border-light-500",
                  disabled && "group-hover:border-light-700"
                )}
              >
                {selected && (
                  <span
                    className="w-1.5 h-1.5 rounded-full bg-accent-600"
                    aria-hidden
                  />
                )}
              </span>
              <span className="font-mono italic text-xs capitalize flex-1">
                {label}
              </span>
              {typeof c === "number" && (
                <span
                  className={twMerge(
                    "tabular-nums text-[10px] flex-shrink-0",
                    selected ? "text-light-400" : "text-light-500"
                  )}
                >
                  {c.toLocaleString()}
                </span>
              )}
            </button>
          );
        })}
      </div>
    </div>
  );

  const unitFilter = unitOptions.length > 0 && (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
          Unit
        </span>
        {selectedUnits.length > 0 && (
          <button
            type="button"
            onClick={clearUnits}
            className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
          >
            clear
          </button>
        )}
      </div>
      <div className="flex flex-col -mx-1">
        {visibleUnits.map(({ unit, count }) => {
          const selected = selectedUnits.includes(unit);
          return (
            <button
              key={unit}
              type="button"
              onClick={() => toggleUnit(unit)}
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
              <span className="font-mono text-xs flex-1 min-w-0 truncate">
                {unit}
              </span>
              <span
                className={twMerge(
                  "tabular-nums text-[10px] flex-shrink-0",
                  selected ? "text-light-400" : "text-light-500"
                )}
              >
                {count.toLocaleString()}
              </span>
            </button>
          );
        })}
        {!showAllUnits && hiddenUnitsCount > 0 && (
          <button
            type="button"
            onClick={() => setShowAllUnits(true)}
            className="mt-1 px-1 py-1 text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors text-left"
          >
            {hiddenUnitsCount} more…
          </button>
        )}
        {showAllUnits && unitOptions.length > TOP_UNITS && (
          <button
            type="button"
            onClick={() => setShowAllUnits(false)}
            className="mt-1 px-1 py-1 text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors text-left"
          >
            collapse
          </button>
        )}
      </div>
    </div>
  );

  const isFiltersDirty =
    searchTerm !== "" ||
    selectedUnits.length > 0 ||
    selectedEvidenceTypes.length > 0 ||
    selectedSourceKind !== "";
  const resetAllFilters = () => {
    setSearchTerm("");
    setSelectedUnits([]);
    setSelectedEvidenceTypes([]);
    setSelectedSourceKind("");
  };

  const evidenceFilter = evidenceTypeOptions.length > 0 && (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
          Evidence
        </span>
        {selectedEvidenceTypes.length > 0 && (
          <button
            type="button"
            onClick={clearEvidenceTypes}
            className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
          >
            clear
          </button>
        )}
      </div>
      <div className="flex flex-col -mx-1">
        {evidenceTypeOptions.map(({ evidence_type, count }) => {
          const selected = selectedEvidenceTypes.includes(evidence_type);
          return (
            <button
              key={evidence_type}
              type="button"
              onClick={() => toggleEvidenceType(evidence_type)}
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
              <span className="font-mono text-xs flex-1 min-w-0 truncate capitalize">
                {evidence_type}
              </span>
              <span
                className={twMerge(
                  "tabular-nums text-[10px] flex-shrink-0",
                  selected ? "text-light-400" : "text-light-500"
                )}
              >
                {count.toLocaleString()}
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
      {isFiltersDirty && (
        <div className="flex justify-end -mt-3 -mb-3">
          <button
            type="button"
            onClick={resetAllFilters}
            className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
          >
            reset all
          </button>
        </div>
      )}
      {unitFilter}
      {evidenceFilter}
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
        externalUnit={unitParam}
        externalEvidenceType={evidenceTypeParam}
        hideChrome
        onTotalRowsChange={setDirectTotal}
        onResetFilters={resetAllFilters}
      />
      <div className="border-t-2 border-double border-light-700/60" />
      <FoodInferredBioactivitiesSection
        commonName={commonName}
        externalSearch={searchTerm}
        externalSourceKind={sourceKindParam}
        externalUnit={unitParam}
        externalEvidenceType={evidenceTypeParam}
        hideChrome
        onTotalRowsChange={setInferredTotal}
        onResetFilters={resetAllFilters}
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
            {unitFilter}
            {evidenceFilter}
            {sourceFilter}
          </aside>
        </div>
      )}
    </div>
  );
};

FoodBioactivitiesTab.displayName = "FoodBioactivitiesTab";
export default FoodBioactivitiesTab;

"use client";

// "Bioactivities" tab on disease pages.
//
// Answers two questions the other disease tabs don't: which biological
// activities does this disease's assay evidence actually measure, and which
// food chemicals get closest to an active dose for them.
//
// The attribution is assay-level on purpose. Health Impacts is CTD literature;
// Chemicals (assay-inferred) lists chemicals but drops what the assays were
// measuring. Going disease → chemical → all of that chemical's bioactivities
// would credit melanoma with 1,571 "antiviral" chemicals; attributing through
// the assay that bridges to the disease gives 3.

import { useEffect, useMemo, useState } from "react";
import { MdRestaurant } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import LoadingCard from "@/components/basic/LoadingCard";
import DiseaseBioactivityTable from "@/components/entities/disease/DiseaseBioactivityTable";
import { useLoadingGate } from "@/context/pageReadyContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import {
  getDiseaseBioactivities,
  getDiseaseBioactivityChemicals,
} from "@/utils/fetching";
import type {
  DiseaseBioactivityChemical,
  DiseaseBioactivitySummary,
} from "@/types";

interface Props {
  commonName: string;
}

const PAGE_SIZE = 50;
const ALL = "__all__";

const DiseaseBioactivitiesSection = ({ commonName }: Props) => {
  const [summary, setSummary] = useState<DiseaseBioactivitySummary[]>([]);
  const [rows, setRows] = useState<DiseaseBioactivityChemical[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  useLoadingGate(isLoading);

  const [bioactivity, setBioactivity] = useState<string>(ALL);
  // Most chemicals here reached the graph through assay data alone and never
  // occur in food. Defaulting to the dietary subset keeps the tab answering
  // the food question; the chip says exactly how many rows that hides.
  const [dietaryOnly, setDietaryOnly] = useState(true);
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);

  usePublishTabCount("bioactivities", isLoading ? null : summary.length);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const [summaryPayload, rowsPayload] = await Promise.all([
        getDiseaseBioactivities(commonName),
        getDiseaseBioactivityChemicals(commonName),
      ]);
      if (cancelled) return;
      setSummary(summaryPayload?.data ?? []);
      setRows(rowsPayload?.data ?? []);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  // Both filters are applied client-side: the payload is already in memory,
  // and re-fetching per chip would trade a 1-frame filter for a round trip.
  const filtered = useMemo(
    () =>
      rows.filter(
        (row) =>
          (bioactivity === ALL || row.bioactivity_name === bioactivity) &&
          (!dietaryOnly || row.dietary !== null)
      ),
    [rows, bioactivity, dietaryOnly]
  );

  const dietaryTotal = useMemo(
    () => rows.filter((r) => r.dietary !== null).length,
    [rows]
  );

  // Counts on the chips follow the dietary toggle, so a chip never promises
  // more rows than clicking it produces.
  const countFor = (name: string) =>
    rows.filter(
      (row) =>
        (name === ALL || row.bioactivity_name === name) &&
        (!dietaryOnly || row.dietary !== null)
    ).length;

  useEffect(() => setVisibleCount(PAGE_SIZE), [bioactivity, dietaryOnly]);

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <LoadingCard key={i} className="h-8" />
        ))}
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        No assay-attributed bioactivities for{" "}
        <span className="capitalize">{commonName}</span> in the current data.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-5">
      <p className="text-sm text-light-400 leading-relaxed max-w-2xl">
        Biological activities measured by the assays that link{" "}
        <span className="capitalize">{commonName}</span> to chemicals. A row
        means the chemical was <em>Active</em> in an assay that both bridges to
        this disease and is classified under that activity — so the activity is
        one this disease&apos;s own evidence measured, not merely something the
        chemical does elsewhere.
      </p>

      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap gap-1.5">
          <Chip
            label="All"
            count={countFor(ALL)}
            tone={bioactivity === ALL ? "cream" : "outline"}
            size="md"
            onClick={() => setBioactivity(ALL)}
            aria-pressed={bioactivity === ALL}
          />
          {summary.map((s) => (
            <Chip
              key={s.bioactivity_foodatlas_id}
              label={s.bioactivity_name}
              count={countFor(s.bioactivity_name)}
              tone={bioactivity === s.bioactivity_name ? "cream" : "outline"}
              size="md"
              onClick={() => setBioactivity(s.bioactivity_name)}
              aria-pressed={bioactivity === s.bioactivity_name}
            />
          ))}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Chip
            icon={<MdRestaurant className="size-3" />}
            label="Found in food"
            count={dietaryTotal}
            tone={dietaryOnly ? "cream" : "outline"}
            size="md"
            onClick={() => setDietaryOnly((v) => !v)}
            aria-pressed={dietaryOnly}
            title="Only chemicals that occur in a food we have a concentration for"
          />
          <span className="text-[11px] font-mono italic text-light-500">
            {dietaryOnly
              ? `hiding ${(rows.length - dietaryTotal).toLocaleString()} assay-only rows`
              : `${(rows.length - dietaryTotal).toLocaleString()} of ${rows.length.toLocaleString()} rows have no dietary dose`}
          </span>
        </div>
      </div>

      {filtered.length === 0 ? (
        <p className="text-sm text-light-500 italic">
          No rows match this combination.
        </p>
      ) : (
        <DiseaseBioactivityTable
          rows={filtered}
          visibleCount={visibleCount}
          onShowAll={() => setVisibleCount(filtered.length)}
        />
      )}
    </div>
  );
};

DiseaseBioactivitiesSection.displayName = "DiseaseBioactivitiesSection";
export default DiseaseBioactivitiesSection;

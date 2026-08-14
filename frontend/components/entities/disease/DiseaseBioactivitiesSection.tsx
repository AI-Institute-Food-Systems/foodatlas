"use client";

// "Bioactivities" tab on disease pages.
//
// Answers what the other disease tabs don't: which biological activities this
// disease's assay evidence actually measures, and which chemicals carry them.
//
// The attribution is assay-level on purpose. Health Impacts is CTD literature;
// Chemicals (assay-inferred) lists chemicals but drops what the assays were
// measuring. Going disease → chemical → all of that chemical's bioactivities
// would credit melanoma with 1,571 "antiviral" chemicals; attributing through
// the assay that bridges to the disease gives 3.
//
// Scope is deliberately narrow: assay counts only. Attaching each chemical's
// best food dose was tried and pulled — see the repository docstring for why.

import { useEffect, useMemo, useState } from "react";

import Chip from "@/components/basic/Chip";
import Skeleton from "@/components/basic/Skeleton";
import DiseaseBioactivityTable from "@/components/entities/disease/DiseaseBioactivityTable";
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

// Literal class strings, not interpolated widths — Tailwind only emits
// classes it can see in the source. Varied to read like the real chip row,
// whose labels are bioactivity names of differing length.
const CHIP_SKELETON_WIDTHS = [
  "w-16",
  "w-24",
  "w-20",
  "w-28",
  "w-16",
  "w-24",
] as const;
const ALL = "__all__";

const DiseaseBioactivitiesSection = ({ commonName }: Props) => {
  const [summary, setSummary] = useState<DiseaseBioactivitySummary[]>([]);
  const [rows, setRows] = useState<DiseaseBioactivityChemical[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  const [bioactivity, setBioactivity] = useState<string>(ALL);
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

  // Filtering client-side: the payload is already in memory, and re-fetching
  // per chip would trade a one-frame filter for a round trip.
  const filtered = useMemo(
    () =>
      bioactivity === ALL
        ? rows
        : rows.filter((row) => row.bioactivity_name === bioactivity),
    [rows, bioactivity]
  );

  // One pass rather than per-chip — the largest disease has 6k rows behind
  // 20 chips.
  const counts = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of rows) {
      map.set(row.bioactivity_name, (map.get(row.bioactivity_name) ?? 0) + 1);
    }
    map.set(ALL, rows.length);
    return map;
  }, [rows]);

  useEffect(() => setVisibleCount(PAGE_SIZE), [bioactivity]);

  if (!isLoading && rows.length === 0) {
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
        chemical does elsewhere. Many of these chemicals are pharmaceuticals
        rather than food constituents.
      </p>

      <div className="flex flex-wrap gap-1.5">
        {/* The chips are driven by `summary`, which arrives with the rows —
         * so while loading there is nothing to render them from. Stand-ins
         * keep the row's height and rhythm instead of letting the table
         * jump up and then back down when the real chips appear. */}
        {isLoading &&
          CHIP_SKELETON_WIDTHS.map((w, i) => (
            <Skeleton key={i} shape="pill" className={`h-6 ${w}`} />
          ))}
        {!isLoading && (
          <Chip
            label="All"
            count={counts.get(ALL) ?? 0}
            tone={bioactivity === ALL ? "cream" : "outline"}
            size="md"
            onClick={() => setBioactivity(ALL)}
            aria-pressed={bioactivity === ALL}
          />
        )}
        {!isLoading &&
          summary.map((s) => (
          <Chip
            key={s.bioactivity_foodatlas_id}
            label={s.bioactivity_name}
            count={counts.get(s.bioactivity_name) ?? 0}
            tone={bioactivity === s.bioactivity_name ? "cream" : "outline"}
            size="md"
            // Bioactivity names arrive lowercase from the KG ("anticancer");
            // the table cells already capitalize, so the chips looked unfinished
            // next to them.
            className="capitalize"
            onClick={() => setBioactivity(s.bioactivity_name)}
            aria-pressed={bioactivity === s.bioactivity_name}
          />
        ))}
      </div>

      <DiseaseBioactivityTable
        rows={filtered}
        visibleCount={visibleCount}
        onShowAll={() => setVisibleCount(filtered.length)}
        commonName={commonName}
        isLoading={isLoading}
      />
    </div>
  );
};

DiseaseBioactivitiesSection.displayName = "DiseaseBioactivitiesSection";
export default DiseaseBioactivitiesSection;

"use client";

// "Diseases" tab on bioactivity pages — the mirror of the Bioactivities tab
// on disease pages, reading the same assay-attributed view from the other end.
//
// A row means: this disease has ≥1 bridging assay that is classified under
// this bioactivity and in which some chemical was Active. That is a narrower
// claim than "chemicals active here are also linked to that disease", which is
// what you get by routing through the chemical instead of the assay.
//
// Flat list, no drilldown: a bioactivity reaches at most 1,282 diseases, and
// the useful question here is which diseases, not which chemicals.

import { useCallback, useEffect, useMemo, useState } from "react";
import { MdArrowForward, MdKeyboardArrowDown } from "react-icons/md";

import Link from "@/components/basic/Link";
import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import DirectionSplit from "@/components/entities/shared/DirectionSplit";
import {
  CardRow,
  CountCell,
  Th,
} from "@/components/entities/shared/EvidenceTable";
import TargetGeneChips from "@/components/entities/shared/TargetGeneChips";
import { useReportRows } from "@/context/reportModeContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import { getBioactivityDiseases } from "@/utils/fetching";
import { encodeSpace } from "@/utils/utils";
import type { BioactivityDisease } from "@/types";

interface Props {
  commonName: string;
}

const PAGE_SIZE = 50;

// Mirrors the <colgroup> and cell alignment of the real table below, so
// the loading grid lines up with the loaded one.
// No Active column: n_active_measurements is identically equal to
// n_assays across all 408,118 rows of mv_disease_bioactivity, so it was
// the same number printed twice.
//
// Assays STAYS here, unlike the other two tables, which dropped it
// because their Assays cell already states the count on its button. This
// endpoint (/bioactivity/diseases) returns no assay list — only the
// count — so there is no button to move it onto, and removing the column
// would lose the number rather than de-duplicate it.
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "disease", width: "w-[29%]" },
  { key: "chemicals", width: "w-[10%]", align: "right" },
  { key: "assays", width: "w-[10%]", align: "right" },
  { key: "signal", width: "w-[29%]" },
  { key: "targets", width: "w-[22%]" },
];

const BioactivityDiseasesSection = ({ commonName }: Props) => {
  const [rows, setRows] = useState<BioactivityDisease[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  const reporter = useReportRows();

  usePublishTabCount("diseases", isLoading ? null : rows.length);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getBioactivityDiseases(commonName);
      if (cancelled) return;
      setRows(payload?.data ?? []);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  const visible = useMemo(
    () => rows.slice(0, visibleCount),
    [rows, visibleCount],
  );

  const rowReportProps = useCallback(
    (row: BioactivityDisease) =>
      reporter.getRowProps({
        kind: "bioactivity-disease-row",
        entityType: "bioactivity",
        entitySlug: commonName,
        diseaseId: row.disease_foodatlas_id,
        diseaseName: row.disease_name,
        nChemicals: row.n_chemicals,
        nAssays: row.n_assays,
      }),
    [reporter, commonName],
  );

  if (!isLoading && rows.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        No assay-attributed diseases for{" "}
        <span className="capitalize">{commonName}</span> in the current data.
      </p>
    );
  }

  const hiddenCount = rows.length - visible.length;
  const diseaseHref = (name: string) =>
    `/disease/${encodeURIComponent(encodeSpace(name))}`;

  return (
    <div className="flex flex-col gap-5">
      <p className="text-sm text-light-400 leading-relaxed max-w-2xl">
        Diseases linked to <span className="capitalize">{commonName}</span> by
        the assays that measure it. A row means the disease&apos;s bridge ties
        it to at least one assay classified under this activity, in which a
        chemical was <em>Active</em> — so the link runs through what the assay
        measured, not merely through a chemical the two happen to share.
      </p>

      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[29%]" />
            <col className="w-[10%]" />
            <col className="w-[10%]" />
            <col className="w-[29%]" />
            <col className="w-[22%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>Disease</Th>
              <Th
                align="right"
                title="Distinct chemicals linking this disease to the bioactivity"
              >
                Chemicals
              </Th>
              <Th align="right" title="Bridging assays behind those links">
                Assays
              </Th>
              <Th title="How many of those chemicals CTD classifies as therapeutic (treats) versus marker/mechanism (marks or drives), and how many the literature also records. A chemical can be both, so these need not sum to the chemical count.">
                Signal
              </Th>
              <Th title="The protein targets the most chemicals converge on for this disease">
                Targets
              </Th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {isLoading && <TableSkeletonRows columns={SKELETON_COLUMNS} />}
            {!isLoading &&
              visible.map((row) => {
              const report = rowReportProps(row);
              return (
                <tr key={row.disease_foodatlas_id} {...report}>
                  <td className="py-1.5 pr-4">
                    <div className="flex min-h-9 items-center capitalize break-words">
                      <Link
                        href={diseaseHref(row.disease_name)}
                        isExternal={false}
                      >
                        {row.disease_name}
                      </Link>
                    </div>
                  </td>
                  <td className="py-1.5 px-4 text-right">
                    <CountCell value={row.n_chemicals} />
                  </td>
                  <td className="py-1.5 px-4 text-right">
                    <CountCell value={row.n_assays} />
                  </td>
                  <td className="py-1.5 px-4">
                    <DirectionSplit
                      nTherapeutic={row.n_therapeutic}
                      nMarker={row.n_marker}
                      nLiterature={row.n_literature}
                      nChemicals={row.n_chemicals}
                    />
                  </td>
                  <td className="py-1.5 px-4">
                    <TargetGeneChips targets={row.targets} visible={2} />
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Mobile cards */}
      {isLoading ? (
        <TableSkeletonCards columns={SKELETON_COLUMNS} />
      ) : (
      <div className="md:hidden divide-y divide-light-800">
        {visible.map((row) => {
          const report = rowReportProps(row);
          return (
            <div
              key={row.disease_foodatlas_id}
              className="py-3 flex flex-col gap-2"
              {...report}
            >
              <div className="flex items-baseline justify-between gap-2 capitalize">
                <Link href={diseaseHref(row.disease_name)} isExternal={false}>
                  {row.disease_name}
                </Link>
                <MdArrowForward className="w-3.5 h-3.5 text-light-500 shrink-0" />
              </div>
              <CardRow label="Chemicals">
                <CountCell value={row.n_chemicals} />
              </CardRow>
              <CardRow label="Assays">
                <CountCell value={row.n_assays} />
              </CardRow>
              <div>
                <DirectionSplit
                  nTherapeutic={row.n_therapeutic}
                  nMarker={row.n_marker}
                  nLiterature={row.n_literature}
                  nChemicals={row.n_chemicals}
                />
              </div>
              {!!row.targets?.length && (
                <CardRow label="Targets">
                  <TargetGeneChips targets={row.targets} visible={2} />
                </CardRow>
              )}
            </div>
          );
        })}
      </div>
      )}

      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={() => setVisibleCount(rows.length)}
          className="self-center inline-flex items-center gap-1 text-xs font-mono italic text-light-400 hover:text-light-100 transition-colors"
        >
          Show all {rows.length.toLocaleString()} diseases
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

BioactivityDiseasesSection.displayName = "BioactivityDiseasesSection";
export default BioactivityDiseasesSection;

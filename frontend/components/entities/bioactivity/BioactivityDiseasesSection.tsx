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
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "disease", width: "w-[54%]" },
  { key: "chemicals", width: "w-[15%]", align: "right" },
  { key: "assays", width: "w-[15%]", align: "right" },
  { key: "active", width: "w-[16%]", align: "right" },
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
            <col className="w-[54%]" />
            <col className="w-[15%]" />
            <col className="w-[15%]" />
            <col className="w-[16%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <th className="h-9 border-b border-light-700 py-1.5 pr-4 uppercase text-xs font-medium">
                Disease
              </th>
              <th
                className="h-9 border-b border-light-700 py-1.5 px-4 text-right uppercase text-xs font-medium"
                title="Distinct chemicals linking this disease to the bioactivity"
              >
                Chemicals
              </th>
              <th
                className="h-9 border-b border-light-700 py-1.5 px-4 text-right uppercase text-xs font-medium"
                title="Bridging assays behind those links"
              >
                Assays
              </th>
              <th
                className="h-9 border-b border-light-700 py-1.5 px-4 text-right uppercase text-xs font-medium"
                title="Active measurements across those assays"
              >
                Active
              </th>
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
                  <td className="py-1.5 px-4 text-right tabular-nums text-light-200">
                    {row.n_chemicals.toLocaleString()}
                  </td>
                  <td className="py-1.5 px-4 text-right tabular-nums text-light-200">
                    {row.n_assays.toLocaleString()}
                  </td>
                  <td className="py-1.5 px-4 text-right tabular-nums text-emerald-300">
                    {row.n_active_measurements.toLocaleString()}
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
              <CardRow label="Chemicals" value={row.n_chemicals} />
              <CardRow label="Assays" value={row.n_assays} />
              <CardRow
                label="Active"
                value={row.n_active_measurements}
                tone="text-emerald-300"
              />
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

const CardRow = ({
  label,
  value,
  tone = "text-light-200",
}: {
  label: string;
  value: number;
  tone?: string;
}) => (
  <div className="flex items-baseline justify-between text-[11px] font-mono">
    <span className="text-light-500">{label}</span>
    <span className={`tabular-nums ${tone}`}>{value.toLocaleString()}</span>
  </div>
);

BioactivityDiseasesSection.displayName = "BioactivityDiseasesSection";
export default BioactivityDiseasesSection;

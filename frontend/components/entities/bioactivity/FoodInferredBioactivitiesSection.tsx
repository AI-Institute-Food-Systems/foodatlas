// Foods don't directly measure bioactivity in the lab; they contain
// chemicals that do. This section surfaces the transitive inference:
// one row per (chemical-in-food, bioactivity-of-chemical) pair, with
// the food-level concentration of that chemical alongside the chemical's
// measurement counts + top measurement against the bioactivity.
//
// Sits below FoodBioactivitiesSection on the food page's Bioactivities
// tab. "View assays" opens the same measurements modal as the direct
// table, anchored on the row's chemical (not the food).

"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MdClose,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";

import Card from "@/components/basic/Card";
import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Pagination from "@/components/basic/Pagination";
import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import {
  formatTopMeasurement,
  topMeasurementOf,
} from "@/components/entities/bioactivity/format";
import { usePaginations } from "@/context/paginationsContext";
import { getFoodInferredBioactivities } from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import type {
  BioactivityMeasurement,
  BioactivityTopMeasurement,
} from "@/types";

interface InferredRow {
  bioactivity: string;
  bioactivity_id: string;
  chemical: string;
  chemical_id: string;
  median_concentration: { value: number | null; unit: string } | null;
  measurement_count: number;
  active_count: number;
  inactive_count: number;
  measurements: BioactivityMeasurement[];
  top_measurement: BioactivityTopMeasurement | null;
}

type SortDir = "asc" | "desc";

interface Props {
  commonName: string;
}

const TABLE_ID_PREFIX = "food-inferred-bioact";

const FoodInferredBioactivitiesSection = ({ commonName }: Props) => {
  const tableId = `${TABLE_ID_PREFIX}-${commonName}`;
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);

  const [searchTerm, setSearchTerm] = useState("");
  const [sort, setSort] = useState<{ by: string; dir: SortDir }>({
    by: "concentration",
    dir: "desc",
  });

  const [rows, setRows] = useState<InferredRow[]>([]);
  const [totalPages, setTotalPages] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  const [selected, setSelected] = useState<InferredRow | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getFoodInferredBioactivities(commonName, {
        page: currentPage,
        search: searchTerm,
        sortBy: sort.by,
        sortDir: sort.dir,
      });
      if (cancelled) return;
      setRows((payload?.data as InferredRow[] | undefined) ?? []);
      setTotalPages(payload?.metadata?.total_pages ?? 0);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, currentPage, searchTerm, sort]);

  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(e.target.value.toLowerCase());
    setTablePaginations(tableId, 1, 20);
  };
  const handleSearchClear = () => {
    setSearchTerm("");
    setTablePaginations(tableId, 1, 20);
  };
  const handleSortClick = (key: string) => {
    setTablePaginations(tableId, 1, 20);
    setSort((prev) =>
      prev.by === key
        ? { by: key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { by: key, dir: "desc" }
    );
  };

  const showingPaginator = totalPages > 1 || isLoading;
  const showEmpty = !isLoading && rows.length === 0;

  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
        type="text"
        placeholder="Search…"
        aria-label="Search bioactivity or chemical"
        value={searchTerm}
        onChange={handleSearchChange}
      />
      {searchTerm && (
        <button
          type="button"
          aria-label="Clear search"
          onClick={handleSearchClear}
          className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
        >
          <MdClose className="w-3 h-3" />
        </button>
      )}
    </div>
  );

  return (
    <div className="relative flex flex-col gap-7">
      {/* Desktop sidebar — matches the direct-measurements table and
       * composition table so the whole Bioactivities tab reads as one
       * design system. Only search for now; unit filter requires
       * endpoint options in the inferred direction and is pending
       * backend support. */}
      <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
        <div className="sticky top-4">
          <Card>{searchInput}</Card>
        </div>
      </aside>

      {/* Heading + provenance disclaimer — same chip vocabulary as the
       * card-catalog sections. The italic line frames the data as
       * inferred, not directly observed in the food. */}
      <div className="flex flex-col gap-2">
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded-r-md px-2.5 py-0.5 font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase -ml-3">
          Inferred via composition
        </span>
        <p className="font-serif italic text-light-400 text-sm">
          Bioactivities of chemicals found in {commonName}. The chemical
          was measured against the activity directly — {commonName} itself
          was not the test material. Concentration is the food-level
          median of that chemical.
        </p>
      </div>

      {/* Sub-1440 search input — sits in the left gutter above the
       * table (there are no non-search filters yet, so no Filters
       * button on this section). */}
      <div className="min-[1440px]:hidden w-full max-w-xs">{searchInput}</div>

      <div className="overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[22%]" />
            <col className="w-[18%]" />
            <col className="w-[16%]" />
            <col className="w-[10%]" />
            <col className="w-[20%]" />
            <col className="w-[14%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <SortableTh
                label="Bioactivity"
                sortKey="bioactivity"
                sort={sort}
                onClick={handleSortClick}
                align="left"
                first
              />
              <SortableTh
                label="Via chemical"
                sortKey="chemical"
                sort={sort}
                onClick={handleSortClick}
                align="left"
              />
              <SortableTh
                label="Concentration"
                sortKey="concentration"
                sort={sort}
                onClick={handleSortClick}
                align="right"
              />
              <SortableTh
                label="Assays"
                sortKey="measurement_count"
                sort={sort}
                onClick={handleSortClick}
                align="right"
              />
              <th className="h-9 border-b border-light-700 leading-none py-1.5 px-4 text-right">
                <span className="select-none uppercase text-xs font-medium text-light-400">
                  Top measurement
                </span>
              </th>
              <th className="h-9 border-b border-light-700 leading-none py-1.5 pl-4 text-right last:pr-0">
                <span className="select-none uppercase text-xs font-medium text-light-400">
                  Detail
                </span>
              </th>
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {isLoading ? (
              Array.from({ length: 20 }).map((_, i) => (
                <tr key={`l-${i}`}>
                  <td className="w-full py-1.5" colSpan={6}>
                    <div className="h-9 flex items-center">
                      <LoadingCard className="h-5" />
                    </div>
                  </td>
                </tr>
              ))
            ) : showEmpty ? (
              <tr>
                <td colSpan={6}>
                  <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                    <MdInfoOutline /> No chemicals in this food have measured
                    bioactivities yet
                  </div>
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <Row
                  key={`${row.chemical_id}-${row.bioactivity_id}-${idx}`}
                  row={row}
                  onOpen={() => setSelected(row)}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {showingPaginator && (
        <div className="mt-2 max-w-xl w-full mx-auto">
          <Pagination
            tableId={tableId}
            numberOfPages={totalPages}
            isLoading={isLoading}
          />
        </div>
      )}

      <BioactivityMeasurementsModal
        isOpen={selected !== null}
        onClose={() => setSelected(null)}
        headLabel={selected?.chemical ?? ""}
        tailLabel={selected?.bioactivity ?? ""}
        initialMeasurements={selected?.measurements ?? []}
        expectedCount={selected?.measurement_count}
        anchorId={selected?.chemical_id ?? null}
        selectedId={selected?.bioactivity_id ?? null}
        relationship="r6"
        headIsRow={false}
      />
    </div>
  );
};

const SortableTh = ({
  label,
  sortKey,
  sort,
  onClick,
  align,
  first,
}: {
  label: string;
  sortKey: string;
  sort: { by: string; dir: SortDir };
  onClick: (k: string) => void;
  align: "left" | "right";
  first?: boolean;
}) => {
  const active = sort.by === sortKey;
  return (
    <th
      className={`h-9 border-b border-light-700 leading-none py-1.5 ${
        first ? "pr-4" : "px-4"
      } ${align === "right" ? "text-right" : "text-left"}`}
    >
      <button
        type="button"
        onClick={() => onClick(sortKey)}
        className={`group flex items-center gap-1 cursor-pointer focus:outline-none ${
          align === "right" ? "justify-end ml-auto" : ""
        }`}
      >
        <span
          className={`select-none uppercase text-xs font-medium transition duration-300 ease-in-out ${
            active ? "text-light-100" : "text-light-400 group-hover:text-light-100"
          }`}
        >
          {label}
        </span>
        {active ? (
          sort.dir === "asc" ? (
            <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
          ) : (
            <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
          )
        ) : (
          <MdUnfoldMore className="text-light-400 group-hover:text-light-100 flex-shrink-0" />
        )}
      </button>
    </th>
  );
};

const Row = ({ row, onOpen }: { row: InferredRow; onOpen: () => void }) => {
  const conc = row.median_concentration;
  const top = topMeasurementOf({
    measurements: row.measurements,
    top_measurement: row.top_measurement,
  });
  return (
    <tr>
      <td className="py-1.5 pr-4">
        <div className="flex min-h-9 items-center capitalize">
          <Link
            href={`/bioactivity/${encodeURIComponent(
              encodeSpace(row.bioactivity)
            )}`}
            isExternal={false}
          >
            {row.bioactivity}
          </Link>
        </div>
      </td>
      <td className="py-1.5 px-4">
        <div className="flex min-h-9 items-center capitalize">
          <Link
            href={`/chemical/${encodeURIComponent(encodeSpace(row.chemical))}`}
            isExternal={false}
          >
            {row.chemical}
          </Link>
        </div>
      </td>
      <td className="py-1.5 px-4 text-right">
        <div className="flex min-h-9 items-center justify-end font-mono text-xs text-light-200 tabular-nums">
          {conc?.value == null ? (
            <span className="text-light-600">—</span>
          ) : (
            <>
              {formatConcentrationValueAlt(conc.value)}
              <span className="ml-1 text-light-500">{conc.unit ?? ""}</span>
            </>
          )}
        </div>
      </td>
      <td className="py-1.5 px-4 text-right">
        <div className="flex min-h-9 items-center justify-end tabular-nums text-light-200">
          {row.measurement_count.toLocaleString()}
        </div>
      </td>
      <td className="py-1.5 px-4 text-right">
        <div className="flex min-h-9 items-center justify-end font-mono text-xs text-light-200">
          {formatTopMeasurement(top)}
        </div>
      </td>
      <td className="py-1.5 pl-4 text-right">
        <div className="flex min-h-9 items-center justify-end">
          <button
            type="button"
            onClick={onOpen}
            disabled={row.measurement_count === 0}
            className="font-mono italic text-xs px-2.5 py-0.5 rounded-full border border-light-700/60 text-light-300 hover:text-light-100 hover:border-light-500 transition-colors disabled:opacity-40 disabled:cursor-not-allowed whitespace-nowrap"
          >
            View {row.measurement_count.toLocaleString()} →
          </button>
        </div>
      </td>
    </tr>
  );
};

FoodInferredBioactivitiesSection.displayName =
  "FoodInferredBioactivitiesSection";
export default FoodInferredBioactivitiesSection;

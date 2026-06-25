// Nutrition panel — flat ingredient ledger that mirrors the composition
// table's chrome (search input + include-without-concentration switch +
// sortable column headers + pagination + fixed 20-row table height) so the
// two views feel like siblings. Category selection lives one level up in
// FoodCompositionTab (sub-pills under the parent "Nutrients" chip).
//
// Pagination is client-side: /food/profile returns the full nutrient set
// for a given food (a few hundred items at most), and the categorized
// shape is also consumed by the parent tab for its sub-pill counts. So we
// fetch once, slice locally, and reset to page 1 when filters change.

"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MdClose,
  MdErrorOutline,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";
import { Switch } from "@headlessui/react";

import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Pagination from "@/components/basic/Pagination";
import { usePaginations } from "@/context/paginationsContext";
import { getFoodMacroAndMicroData } from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import { MacroAndMicroData } from "@/types";

type Concentration = MacroAndMicroData[string][number];
type Row = Concentration & { category: string };

interface Props {
  commonName: string;
  selectedCategory: string | null;
}

const PAGE_SIZE = 20;
const TABLE_ID = "food-nutrients-table";

const TABLE_HEADERS = [
  { label: "Nutrient", sortName: "name", align: "left" as const },
  { label: "Classification", align: "left" as const },
  {
    label: "Concentration (mg/100g)",
    sortName: "median_concentration",
    align: "right" as const,
  },
];

type Direction = "asc" | "desc";

const compareName = (a: Row, b: Row, dir: Direction): number => {
  const cmp = a.name.localeCompare(b.name);
  return dir === "asc" ? cmp : -cmp;
};

// Nulls always sort to the bottom regardless of direction — matches the
// backend's `ORDER BY value DESC NULLS LAST` for the chemical composition
// view. (Sorting asc/desc and then `.reverse()`-ing flips nulls to the
// top in desc mode, which is what the user was hitting.)
const compareConcentration = (a: Row, b: Row, dir: Direction): number => {
  const av = a.median_concentration?.value ?? null;
  const bv = b.median_concentration?.value ?? null;
  if (av === null && bv === null) return a.name.localeCompare(b.name);
  if (av === null) return 1;
  if (bv === null) return -1;
  return dir === "asc" ? av - bv : bv - av;
};

const massPercent = (
  value: number,
  unit: string | null | undefined
): number | null => {
  if (!unit || unit.replace(/\s+/g, "").toLowerCase() !== "mg/100g") {
    return null;
  }
  return value / 1000;
};

const formatPercent = (p: number): string => {
  if (p >= 10) return `${p.toFixed(0)}%`;
  if (p >= 1) return `${p.toFixed(1)}%`;
  return `${p.toFixed(2)}%`;
};

const ValueCell = ({ row }: { row: Row }) => {
  const v = row.median_concentration?.value;
  if (v === null || v === undefined) {
    return (
      <div className="flex min-h-9 items-center justify-end">
        <span className="text-light-600">—</span>
      </div>
    );
  }
  const pct = massPercent(v, row.median_concentration?.unit);
  const barPct = pct === null ? 0 : Math.max(2, Math.min(100, pct));
  return (
    <div className="flex min-h-9 items-center justify-end gap-3">
      <span
        aria-hidden
        className="relative h-1.5 w-32 shrink-0 rounded-full bg-light-800/70 overflow-hidden"
      >
        {pct !== null && (
          <span
            className="absolute inset-y-0 left-0 rounded-full bg-accent-600/80"
            style={{ width: `${barPct}%` }}
          />
        )}
      </span>
      <span className="font-mono text-xs text-light-200 whitespace-nowrap tabular-nums text-right min-w-[5rem]">
        {formatConcentrationValueAlt(v)}
      </span>
      <span className="font-mono text-xs text-light-500 whitespace-nowrap tabular-nums text-right min-w-[3.5rem]">
        {pct !== null ? formatPercent(pct) : ""}
      </span>
    </div>
  );
};

const MacrosAndMicrosSection = ({ commonName, selectedCategory }: Props) => {
  const [data, setData] = useState<MacroAndMicroData | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(true);
  const [isError, setIsError] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [includeUnmeasured, setIncludeUnmeasured] = useState(true);
  const [sort, setSort] = useState<{ column: string; direction: Direction }>({
    column: "median_concentration",
    direction: "desc",
  });
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(TABLE_ID);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    setIsError(false);
    (async () => {
      try {
        const result = await getFoodMacroAndMicroData(commonName);
        if (!cancelled) setData(result);
      } catch {
        if (!cancelled) setIsError(true);
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  // Flatten {category: [items]} → Row[] with category attached on each item.
  const flat = useMemo<Row[]>(() => {
    if (!data) return [];
    return Object.entries(data).flatMap(([category, items]) =>
      items.map<Row>((c) => ({ ...c, category }))
    );
  }, [data]);

  const filtered = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();
    const rows = flat.filter((r) => {
      if (selectedCategory && r.category !== selectedCategory) return false;
      if (term && !r.name.toLowerCase().includes(term)) return false;
      if (
        !includeUnmeasured &&
        (r.median_concentration?.value === null ||
          r.median_concentration?.value === undefined)
      ) {
        return false;
      }
      return true;
    });
    const cmp = sort.column === "name" ? compareName : compareConcentration;
    return [...rows].sort((a, b) => cmp(a, b, sort.direction));
  }, [flat, selectedCategory, searchTerm, includeUnmeasured, sort]);

  const numberOfRows = filtered.length;
  const numberOfPages = Math.max(1, Math.ceil(numberOfRows / PAGE_SIZE));

  // Snap pagination back to a valid page when filters shrink the result
  // set below the current page index (e.g. user is on p3 of 5, then types
  // a search that yields 1 page).
  useEffect(() => {
    if (currentPage > numberOfPages) {
      setTablePaginations(TABLE_ID, 1, PAGE_SIZE);
    }
  }, [currentPage, numberOfPages, setTablePaginations]);

  const visible = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE;
    return filtered.slice(start, start + PAGE_SIZE);
  }, [filtered, currentPage]);

  const placeholderRowsCount = Math.max(0, PAGE_SIZE - visible.length);

  const resetToFirstPage = () => setTablePaginations(TABLE_ID, 1, PAGE_SIZE);

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(e.target.value);
    resetToFirstPage();
  };

  const handleSearchClear = () => {
    setSearchTerm("");
    resetToFirstPage();
  };

  const handleConcentrationSwitchChange = () => {
    setIncludeUnmeasured((prev) => !prev);
    resetToFirstPage();
  };

  const handleSortClick = (sortName: string) => {
    setSort((prev) => {
      resetToFirstPage();
      const isSame = prev.column === sortName;
      return {
        column: sortName,
        direction: isSame && prev.direction === "asc" ? "desc" : "asc",
      };
    });
  };

  return (
    <div className="flex flex-col gap-7">
      {/* controls — mirror the composition table's search + switch layout */}
      <div className="w-full flex flex-col lg:flex-row justify-between">
        {/* search */}
        <div className="relative flex items-center">
          <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
          <input
            className="pl-9 pr-9 w-full lg:w-72 h-9 text-sm rounded-lg border border-light-50/5 bg-light-900 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
            type="text"
            placeholder="Search for a nutrient"
            value={searchTerm}
            onChange={handleSearch}
          />
          {searchTerm && (
            <button
              type="button"
              aria-label="Clear search"
              onClick={handleSearchClear}
              className="absolute right-2 flex items-center justify-center w-5 h-5 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
            >
              <MdClose className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
        {/* include unmeasured */}
        <div className="mt-4 lg:mt-0 flex gap-2 items-center justify-between">
          <span className="uppercase text-[10px] tracking-wider text-light-400 md:max-w-[8rem] lg:text-right leading-tight">
            include without concentration
          </span>
          <Switch
            checked={includeUnmeasured}
            onChange={handleConcentrationSwitchChange}
            className="group inline-flex h-4 w-8 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 flex-shrink-0 transition-colors"
          >
            <span className="size-3 translate-x-0.5 rounded-full bg-white transition group-data-[checked]:translate-x-[1.125rem]" />
          </Switch>
        </div>
      </div>

      {/* table */}
      <div className="mt-3 overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[40%]" />
            <col className="w-[20%]" />
            <col className="w-[40%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              {TABLE_HEADERS.map((header, index) => (
                <th
                  key={index}
                  className={`h-9 border-b border-light-700 leading-none break-all md:break-normal py-1.5 ${
                    index === 0
                      ? "pr-4"
                      : index === TABLE_HEADERS.length - 1
                        ? "pl-4"
                        : "px-4"
                  } ${header.align === "right" ? "text-right" : "text-left"}`}
                >
                  <div
                    className={`group flex gap-1 items-center flex-nowrap w-full ${
                      header.sortName
                        ? "cursor-pointer"
                        : "pointer-events-none"
                    } ${header.align === "right" ? "justify-end" : "justify-between"}`}
                    onClick={() =>
                      header.sortName && handleSortClick(header.sortName)
                    }
                  >
                    <span
                      className={`select-none uppercase text-xs font-medium group-hover:text-light-100 transition duration-300 ease-in-out ${
                        header.sortName === sort.column ? "text-light-100" : ""
                      }`}
                    >
                      {header.label}
                    </span>
                    {header.sortName &&
                      (header.sortName === sort.column ? (
                        sort.direction === "asc" ? (
                          <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 transition duration-300 ease-in-out flex-shrink-0" />
                        ) : (
                          <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 transition duration-300 ease-in-out flex-shrink-0" />
                        )
                      ) : (
                        <MdUnfoldMore className="text-light-400 group-hover:text-light-100 transition duration-300 ease-in-out flex-shrink-0" />
                      ))}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {isLoading ? (
              Array.from({ length: PAGE_SIZE }, (_, index) => (
                <tr key={index}>
                  <td
                    className="w-full py-1.5"
                    colSpan={TABLE_HEADERS.length}
                  >
                    <div className="h-9 flex items-center">
                      <LoadingCard className="h-5" />
                    </div>
                  </td>
                </tr>
              ))
            ) : isError ? (
              <tr>
                <td colSpan={TABLE_HEADERS.length}>
                  <div className="h-[10rem] flex items-center justify-center text-red-400 gap-2">
                    <MdErrorOutline /> An error occurred fetching data, please
                    refresh the page
                  </div>
                </td>
              </tr>
            ) : visible.length === 0 ? (
              <tr>
                <td colSpan={TABLE_HEADERS.length}>
                  <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                    <MdInfoOutline /> No nutrients match the current filters
                  </div>
                </td>
              </tr>
            ) : (
              visible.map((row, idx) => (
                <tr key={`${row.category}-${row.name}-${idx}`}>
                  <td className="py-1.5 pr-4">
                    <div
                      className="flex min-h-9 capitalize items-center truncate"
                      title={row.name}
                    >
                      <Link
                        href={`/chemical/${encodeURIComponent(
                          encodeSpace(row.name)
                        )}`}
                        isExternal={false}
                      >
                        {row.name}
                      </Link>
                    </div>
                  </td>
                  <td className="py-1.5 px-4">
                    <div className="flex min-h-9 capitalize items-center font-mono italic text-xs text-light-400">
                      {row.category}
                    </div>
                  </td>
                  <td className="py-1.5 pl-4">
                    <ValueCell row={row} />
                  </td>
                </tr>
              ))
            )}
            {numberOfPages > 1 &&
              !isLoading &&
              !isError &&
              Array.from({ length: placeholderRowsCount }, (_, index) => (
                <tr key={`placeholder-${index}`}>
                  <td className="py-1.5" colSpan={TABLE_HEADERS.length}>
                    <div className="h-9" />
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>

      {/* pagination */}
      {(numberOfPages > 1 || isLoading) && (
        <div className="mt-8 max-w-xl w-full mx-auto">
          <Pagination
            tableId={TABLE_ID}
            numberOfPages={numberOfPages}
            isLoading={isLoading}
          />
        </div>
      )}
    </div>
  );
};

MacrosAndMicrosSection.displayName = "MacrosAndMicrosSection";

export default MacrosAndMicrosSection;

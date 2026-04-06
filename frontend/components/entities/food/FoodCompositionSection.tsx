"use client";

import { useEffect, useState } from "react";
import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
  Portal,
  Switch,
} from "@headlessui/react";
import {
  MdCheck,
  MdDescription,
  MdErrorOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";
import Link from "@/components/basic/Link";
import Pagination from "@/components/basic/Pagination";
import LoadingCard from "@/components/basic/LoadingCard";
import Heading from "@/components/basic/Heading";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import { getFoodCompositionData } from "@/utils/fetching";
import { FoodCompositionData } from "@/types";
import { FoodEvidence } from "@/types/Evidence";

// headers for table
const TABLE_HEADERS = [
  {
    label: "Chemical",
    sortName: "common_name",
    align: "left" as const,
  },
  {
    label: "Nutrient Classification",
    align: "left" as const,
  },
  {
    label: "Median Concentration (mg/100g)",
    sortName: "median_concentration",
    align: "right" as const,
  },
  {
    label: "Evidence",
    align: "right" as const,
  },
];

// mapping of source filters to their labels
const SOURCE_OPTIONS = [
  { value: "fdc", label: "FDC" },
  { value: "foodatlas", label: "FoodAtlas" },
  { value: "dmd", label: "Dairy Molecule Database" },
];

interface FoodCompositionSectionProps {
  commonName: string;
}

const FoodCompositionSection = ({
  commonName,
}: FoodCompositionSectionProps) => {
  const [data, setData] = useState<FoodCompositionData[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isError, setIsError] = useState(false);
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations("food-composition-table");
  const [numberOfPages, setNumberOfPages] = useState(-1);
  const [numberOfRows, setNumberOfRows] = useState(-1);
  const [searchTerm, setSearchTerm] = useState("");
  const [sourceFilters, setSourceFilters] = useState<string[]>([
    "fdc",
    "foodatlas",
    "dmd",
  ]);
  const [sort, setSort] = useState({
    column: "median_concentration",
    direction: "desc",
  });
  const [showAllConcentrations, setShowAllConcentrations] = useState(true);
  const [selectedEvidenceName, setSelectedEvidenceName] = useState("");
  const [sourceCounts, setSourceCounts] = useState<Record<string, number>>({});

  // fetch per-source chemical counts on mount
  useEffect(() => {
    const fetchCounts = async () => {
      const counts: Record<string, number> = {};
      await Promise.all(
        SOURCE_OPTIONS.map(async (option) => {
          try {
            const result = await getFoodCompositionData(
              commonName,
              1,
              [option.value],
              "",
              { column: "median_concentration", direction: "desc" },
              true
            );
            counts[option.value] = result.metadata.total_rows;
          } catch {
            counts[option.value] = 0;
          }
        })
      );
      setSourceCounts(counts);
    };
    fetchCounts();
  }, [commonName]);

  // data fetching
  useEffect(() => {
    const fetchData = async () => {
      // no sources selected, show empty state
      if (sourceFilters.length === 0) {
        setData([]);
        setNumberOfPages(0);
        setNumberOfRows(0);
        setIsLoading(false);
        return;
      }

      try {
        setIsError(false);
        setIsLoading(true);
        const result = await getFoodCompositionData(
          commonName,
          currentPage,
          sourceFilters,
          searchTerm,
          sort,
          showAllConcentrations
        );
        // client-side filter: only keep rows with evidence from selected sources
        const filteredData = (
          result.data as FoodCompositionData[]
        ).filter((row) =>
          sourceFilters.some((source) => {
            const evidences =
              row[
                `${source}_evidences` as keyof FoodCompositionData
              ];
            return Array.isArray(evidences) && evidences.length > 0;
          })
        );
        setData(filteredData);
        setNumberOfPages(result.metadata.total_pages);
        setNumberOfRows(result.metadata.total_rows);
      } catch (error) {
        console.error("Error fetching food composition data:", error);
        setIsError(true);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [
    currentPage,
    commonName,
    sourceFilters,
    searchTerm,
    sort,
    showAllConcentrations,
  ]);

  // handle source filter change
  const handleFilterChange = (sources: string[]) => {
    setTablePaginations("food-composition-table", 1, 20);
    setSourceFilters(sources);
  };

  // handle evidence button click
  const handleEvidenceButtonClick = (
    event: React.MouseEvent<HTMLButtonElement>,
    name: string
  ) => {
    event.preventDefault();
    event.stopPropagation();
    setSelectedEvidenceName(name);
  };

  // handle search
  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(() => {
      setTablePaginations("food-composition-table", 1, 20);
      return e.target.value.toLowerCase();
    });
  };

  // handle sort column click
  const handleSortClick = (sortName: string) => {
    setSort((prevSort: { column: string; direction: string }) => {
      setTablePaginations("food-composition-table", 1, 20);
      const isSameColumn = prevSort.column === sortName;
      return {
        column: sortName,
        direction:
          isSameColumn && prevSort.direction === "asc" ? "desc" : "asc",
      };
    });
  };

  const handleConcentrationSwitchChange = () => {
    setShowAllConcentrations((prev) => !prev);
    setTablePaginations("food-composition-table", 1, 20);
  };

  const getRowEvidenceCount = (row: FoodCompositionData) =>
    (row.foodatlas_evidences?.length || 0) +
    (row.fdc_evidences?.length || 0) +
    (row.dmd_evidences?.length || 0);

  // number of placeholder rows to make up for the total of 20 rows
  const placeholderRowsCount = data ? 20 - data?.length : 20;

  return (
    <>
      <div className="flex flex-col gap-7">
        <Heading type="h2" variant="boxed">
          Full Chemical Composition
        </Heading>
        <Card>
          {/* table controls */}
          <div className="w-full flex flex-col lg:flex-row justify-between">
            {/* search */}
            <div className="relative flex items-center">
              <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
              <input
                className="pl-9 w-full lg:w-72 h-9 text-sm rounded-lg border border-light-50/5 bg-light-900 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
                type="text"
                placeholder="Search for a chemical"
                onChange={handleSearch}
              />
            </div>
            {/* switch and filters */}
            <div className="mt-5 lg:mt-0 flex gap-4 lg:gap-10 justify-between flex-col md:flex-row">
              {/* switch to remove n/a concentrations */}
              <div className="flex gap-3 items-center justify-between">
                <span className="uppercase text-xs text-light-400 md:max-w-[10rem] lg:text-right">
                  show chemicals without concentration data
                </span>
                <Switch
                  checked={showAllConcentrations}
                  onChange={handleConcentrationSwitchChange}
                  className="group inline-flex h-6 w-11 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50 flex-shrink-0"
                >
                  <span className="size-4 translate-x-1 rounded-full bg-white transition group-data-[checked]:translate-x-6" />
                </Switch>
              </div>
              {/* source filter */}
              <div className="flex gap-3 items-center justify-between">
                <span className="text-xs text-light-400 uppercase">Source</span>
                <div className="w-52">
                  <Listbox
                    value={sourceFilters}
                    onChange={handleFilterChange}
                    multiple
                  >
                    <ListboxButton
                      className={twMerge(
                        "h-9 relative block w-full rounded-lg bg-light-900 py-1.5 pr-8 pl-4 text-left text-sm/6 text-white truncate",
                        "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
                      )}
                    >
                      {sourceFilters.length > 0
                        ? sourceFilters
                            .map(
                              (filter) =>
                                SOURCE_OPTIONS.find(
                                  (opt) => opt.value === filter
                                )?.label
                            )
                            .join(", ")
                        : "None selected"}
                      <MdKeyboardArrowDown
                        className="group pointer-events-none absolute top-2.5 right-2.5 size-4 fill-white/60"
                        aria-hidden="true"
                      />
                    </ListboxButton>
                    <ListboxOptions
                      anchor="bottom"
                      transition
                      className={twMerge(
                        "w-[var(--button-width)] rounded-xl border border-white/5 bg-white/5 backdrop-blur-lg p-1 [--anchor-gap:var(--spacing-1)] focus:outline-none",
                        "transition duration-100 ease-in data-[leave]:data-[closed]:opacity-0"
                      )}
                    >
                      {SOURCE_OPTIONS.map((option, id) => (
                        <ListboxOption
                          key={id}
                          value={option.value}
                          className="group flex cursor-default items-center gap-2 rounded-lg py-1.5 px-4 select-none data-[focus]:bg-white/10"
                        >
                          <MdCheck className="invisible size-4 fill-white group-data-[selected]:visible flex-shrink-0" />
                          <span className="flex-1">{option.label}</span>
                          {sourceCounts[option.value] != null && (
                            <span className="text-xs text-light-400">
                              {sourceCounts[option.value]}
                            </span>
                          )}
                        </ListboxOption>
                      ))}
                    </ListboxOptions>
                  </Listbox>
                </div>
              </div>
            </div>
          </div>
          {/* # chemicals indicator */}
          <div className="mt-6">
            {isLoading ? (
              <LoadingCard className="h-5 w-36" />
            ) : (
              <div className="text-sm text-neutral-400">{`Found ${numberOfRows} chemical${
                numberOfRows === 1 ? "" : "s"
              }`}</div>
            )}
          </div>
          {/* table */}
          <div className="mt-3 overflow-x-auto">
            <table className="w-full table-fixed">
              <colgroup>
                <col className="w-[30%]" />
                <col className="w-[25%]" />
                <col className="w-[25%]" />
                <col className="w-[20%]" />
              </colgroup>
              <thead className="text-light-400 text-left">
                <tr>
                  {/* table headers */}
                  {TABLE_HEADERS.map((header, index) => (
                    <th
                      key={index}
                      className={`h-12 border-b border-light-700 leading-none break-all md:break-normal py-3 ${
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
                            header.sortName === sort.column
                              ? "text-light-100"
                              : ""
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
              <tbody className="font-light">
                {isLoading ? (
                  // loading skeleton
                  Array.from({ length: 20 }, (_, index) => (
                    <tr key={index}>
                      <td
                        className="w-full py-3"
                        colSpan={TABLE_HEADERS.length}
                      >
                        <div className="h-12 flex items-center">
                          <LoadingCard className="h-5" />
                        </div>
                      </td>
                    </tr>
                  ))
                ) : isError ? (
                  // error message
                  <tr>
                    <td colSpan={TABLE_HEADERS.length}>
                      <div className="h-[10rem] flex items-center justify-center text-red-400 gap-2">
                        <MdErrorOutline /> An error occurred fetching data,
                        please refresh the page
                      </div>
                    </td>
                  </tr>
                ) : data.length > 0 ? (
                  data.map((row) => (
                    <tr key={row.id}>
                      {/* name */}
                      <td className="py-3 pr-4">
                        <div className="flex min-h-12 capitalize items-center">
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
                      {/* nutrient classification */}
                      <td className="py-3 px-4">
                        <div className="flex min-h-12 capitalize items-center">
                          {row.nutrient_classification.join(", ")}
                        </div>
                      </td>
                      {/* median concentration */}
                      <td className="py-3 px-4">
                        <div className="flex min-h-12 items-center justify-end">
                          {formatConcentrationValueAlt(
                            row.median_concentration?.value
                          )}
                        </div>
                      </td>
                      {/* evidence */}
                      <td className="py-3 pl-4">
                        <div className="flex min-h-12 capitalize items-center justify-end">
                          <Button
                            className="border-light-500 text-light-500 w-36"
                            variant="outlined"
                            size="sm"
                            onClick={(event) =>
                              handleEvidenceButtonClick(event, row.name)
                            }
                          >
                            <MdDescription className="size-4" />{" "}
                            {getRowEvidenceCount(row)} Data Point
                            {getRowEvidenceCount(row) === 1 ? "" : "s"}
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))
                ) : (
                  // no rows
                  <tr>
                    <td colSpan={TABLE_HEADERS.length}>
                      <div className="h-[10rem] flex items-center justify-center text-light-300">
                        <>No associations found</>
                      </div>
                    </td>
                  </tr>
                )}
                {/* add empty rows to make up for the total of 20 rows */}
                {numberOfPages > 1 &&
                  !isLoading &&
                  Array.from({ length: placeholderRowsCount }, (_, index) => (
                    <tr key={index}>
                      <td className="py-3" colSpan={TABLE_HEADERS.length}>
                        <div className="h-12" />
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
                tableId={"food-composition-table"}
                numberOfPages={numberOfPages}
                isLoading={isLoading}
              />
            </div>
          )}
        </Card>
      </div>
      {/* evidence modal */}
      <Portal>
        <FoodCompositionEvidenceModal
          foodName={commonName}
          chemicalName={selectedEvidenceName}
          evidences={(() => {
            const selectedRow = data?.find(
              (row) => row.name === selectedEvidenceName
            );
            if (!selectedRow) return undefined;

            // separate dmd evidences from other evidences
            const dmdEvidences = selectedRow.dmd_evidences ?? [];
            const otherEvidences = [
              ...(selectedRow.fdc_evidences ?? []),
              ...(selectedRow.foodatlas_evidences ?? []),
            ];

            // combine all dmd evidences into one evidence item
            if (dmdEvidences.length > 0) {
              const combinedDmdEvidence: FoodEvidence = {
                premise: "",
                extraction: dmdEvidences.flatMap(
                  (evidence) => evidence.extraction
                ),
                reference: dmdEvidences[0].reference, // use first evidence's reference
              };
              return [combinedDmdEvidence, ...otherEvidences];
            }

            return otherEvidences;
          })()}
          isOpen={selectedEvidenceName !== ""}
          onClose={() => setSelectedEvidenceName("")}
        />
      </Portal>
    </>
  );
};

FoodCompositionSection.displayName = "FoodCompositionSection";

export default FoodCompositionSection;

"use client";

import { useEffect, useState } from "react";
import { MdAdd, MdErrorOutline, MdInfoOutline, MdRemove } from "react-icons/md";

import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Pagination from "@/components/basic/Pagination";
import CorrelationEvidenceModal from "@/components/entities/CorrelationEvidenceModal";
import { usePaginations } from "@/context/paginationsContext";
import { getDiseaseData } from "@/utils/fetching";
import { encodeSpace } from "@/utils/utils";
import { ChemicalCorrelation } from "@/types";

interface DiseaseTableProps {
  commonName: string;
  tableLocation: string;
  correlationType: "positive" | "negative";
  headers: { label: string }[];
}

const CorrelationTable = ({
  commonName,
  tableLocation,
  correlationType,
  headers,
}: DiseaseTableProps) => {
  const tableId = tableLocation + "-" + correlationType + "-table";
  const [data, setData] = useState<ChemicalCorrelation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isError, setIsError] = useState(false);
  const [numberOfPages, setNumberOfPages] = useState(1);
  const { getTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);
  const [selectedEvidenceName, setSelectedEvidenceName] = useState("");

  // fetch data
  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsError(false);
        const data = await getDiseaseData(
          commonName,
          currentPage,
          tableLocation,
          correlationType
        );
        // FIXME backend: "associations" should be changed to "impacts"
        const dataAccessor = `${correlationType}_associations`;
        setData(data.data[dataAccessor]);
        // FIXME backend: metadata is not returning correct total_pages per correlation type but combined?
        setNumberOfPages(data.metadata.total_pages);
      } catch (error) {
        console.log(error);
        setIsError(true);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [tableLocation, correlationType, currentPage, commonName]);

  // handle evidence show more click
  const handleEvidenceShowMoreClick = (name: string) => {
    setSelectedEvidenceName(name);
  };

  return (
    <>
      <div>
        {/* table */}
        <div className="overflow-x-auto">
          <table className="w-full table-fixed">
            {/* table headers */}
            <thead className="text-light-400 text-left">
              <tr>
                {headers.map((header, index) => (
                  <th
                    key={index}
                    className={`h-12 border-b border-light-700 leading-none break-all md:break-normal py-3 ${
                      index === 0
                        ? "pr-4"
                        : index === headers.length - 1
                        ? "pl-4 text-right"
                        : "px-4"
                    }`}
                  >
                    <div className="flex flex-nowrap">
                      <span className="select-none uppercase text-xs font-medium w-full">
                        {header.label}
                      </span>
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            {/* table body */}
            <tbody className="font-light">
              {isLoading ? (
                // loading skeleton
                Array.from({ length: 10 }, (_, index) => (
                  <tr key={index}>
                    <td className="w-full py-3" colSpan={headers.length}>
                      <div className="h-12 flex items-center">
                        <LoadingCard className="h-5" />
                      </div>
                    </td>
                  </tr>
                ))
              ) : isError ? (
                // error message
                <tr>
                  <td colSpan={headers.length}>
                    <div className="h-[10rem] flex items-center justify-center text-red-400 gap-2">
                      <MdErrorOutline /> An error occurred fetching data, please
                      refresh the page
                    </div>
                  </td>
                </tr>
              ) : data.length > 0 ? (
                // data rows
                data.map((row) => (
                  <tr key={row.id}>
                    {/* entity name */}
                    <td className="py-3 pr-4">
                      <div className="flex gap-2.5 min-h-12 capitalize items-center">
                        {correlationType === "negative" ? (
                          <div className="w-[1.2rem] h-[1.2rem] flex justify-center items-center rounded-full border-[1.5px] border-red-600 text-red-600 bg-red-600/10 shadow-red-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
                            <MdRemove />
                          </div>
                        ) : (
                          <div className="w-[1.2rem] h-[1.2rem] flex justify-center items-center rounded-full border-[1.5px] border-lime-600 text-lime-600 bg-lime-600/10 shadow-lime-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
                            <MdAdd />
                          </div>
                        )}
                        <Link
                          className="capitalize"
                          href={`/${
                            tableLocation === "chemical"
                              ? "disease"
                              : "chemical"
                          }/${encodeURIComponent(encodeSpace(row.name))}`}
                          isExternal={false}
                        >
                          {row.name}
                        </Link>
                      </div>
                    </td>
                    {/* evidence */}
                    <td className="py-3 pl-4">
                      {
                        <div className="flex min-h-12 capitalize items-center justify-end">
                          <div className="flex gap-2 justify-end items-center flex-wrap">
                            {row.evidences.slice(0, 3).map((evidence) => (
                              <Link
                                className="whitespace-nowrap"
                                key={evidence.pmid?.id ?? evidence.pmcid?.id}
                                href={evidence.pmid?.url ?? evidence.pmcid?.url}
                                isExternal
                              >
                                {evidence.pmid?.id ?? evidence.pmcid?.id}
                              </Link>
                            ))}
                            {row.evidences.length > 3 && (
                              <>
                                <Button
                                  className="font-medium"
                                  variant="outlined"
                                  size="sm"
                                  onClick={() =>
                                    handleEvidenceShowMoreClick(row.name)
                                  }
                                >
                                  {`${row.evidences.length - 3} more`}...
                                </Button>
                              </>
                            )}
                          </div>
                        </div>
                      }
                    </td>
                  </tr>
                ))
              ) : (
                // no rows
                <tr>
                  <td colSpan={headers.length}>
                    <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                      <MdInfoOutline /> No evidence found
                    </div>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {/* pagination */}
        {(numberOfPages > 1 || isLoading) && (
          <div className="mt-8 max-w-xl w-full mx-auto">
            <Pagination
              tableId={tableId ?? ""}
              numberOfPages={numberOfPages}
              isLoading={isLoading}
            />
          </div>
        )}
      </div>
      {/* evidence modal */}
      <CorrelationEvidenceModal
        entityType={tableLocation as "chemical" | "disease"}
        correlationType={correlationType}
        chemicalName={
          tableLocation === "chemical" ? commonName : selectedEvidenceName
        }
        diseaseName={
          tableLocation === "chemical" ? selectedEvidenceName : commonName
        }
        evidences={
          selectedEvidenceName === ""
            ? undefined
            : data?.filter((row) => {
                return row.name === selectedEvidenceName;
              })[0].evidences
        }
        isOpen={selectedEvidenceName !== ""}
        onClose={() => setSelectedEvidenceName("")}
      />
    </>
  );
};

CorrelationTable.displayName = "CorrelationTable";

export default CorrelationTable;

"use client";

// The assay-inferred half of the merged Diseases tab on chemical pages.
//
// A different evidence source from the CTD literature rows it now sits
// under: this is "was Active in a shared assay", not "a study reported
// it". Kept as its own table rather than merged row-wise because the two
// carry almost disjoint columns.

import { useCallback } from "react";

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import Heading from "@/components/basic/Heading";
import { getChemicalDiseaseAssociations } from "@/utils/fetching";

interface Props {
  commonName: string;
  search?: string;
  onTotalRowsChange?: (total: number) => void;
}

const ChemicalAssayInferredSection = ({
  commonName,
  search = "",
  onTotalRowsChange,
}: Props) => {
  const fetcher = useCallback(
    () => getChemicalDiseaseAssociations(commonName),
    [commonName]
  );
  return (
    <div className="flex flex-col gap-4">
      <div>
        <Heading
          type="h3"
          className="text-light-300 font-mono text-sm font-medium"
        >
          From Lab Assays
        </Heading>
        <p className="text-light-500">
          Diseases this chemical is associated with because it was <em>Active</em> in a bioactivity assay that the disease-bridge ties to the disease via target genes or mechanism. Assay signal, not a curated claim — read it alongside the literature rows above rather than as a second opinion on them.
        </p>
      </div>
      <AssayInferredAssociationsTable
        commonName={commonName}
        peer="disease"
        fetcher={fetcher}
        externalSearch={search}
        onTotalRowsChange={onTotalRowsChange}
      />
    </div>
  );
};

ChemicalAssayInferredSection.displayName = "ChemicalAssayInferredSection";
export default ChemicalAssayInferredSection;

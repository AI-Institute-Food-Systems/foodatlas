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
  signals?: string[];
  activities?: string[];
  onSignalCountsChange?: (counts: Record<string, number>) => void;
  onActivityCountsChange?: (counts: Record<string, number>) => void;
  onTotalRowsChange?: (total: number) => void;
}

const ChemicalAssayInferredSection = ({
  commonName,
  search = "",
  signals = [],
  activities = [],
  onTotalRowsChange,
  onSignalCountsChange,
  onActivityCountsChange,
}: Props) => {
  const fetcher = useCallback(
    () => getChemicalDiseaseAssociations(commonName),
    [commonName]
  );
  return (
    <div className="flex flex-col gap-4">
      {/* Matches the literature block above it and the food page's
        * stacked sections: chip label, serif blurb. */}
      <div className="flex flex-col gap-2">
        <Heading type="h3" variant="chip" className="self-start">
          From Lab Assays
        </Heading>
        <p className="font-serif italic text-light-400 text-sm">
          Diseases this chemical is associated with because it was{" "}
          <em>Active</em> in a bioactivity assay that the disease-bridge ties
          to the disease via target genes or mechanism, and what those assays
          were measuring. Assay signal, not a curated claim — read it
          alongside the literature rows above rather than as a second opinion
          on them.
        </p>
      </div>
      <AssayInferredAssociationsTable
        commonName={commonName}
        peer="disease"
        fetcher={fetcher}
        externalSearch={search}
        externalSignals={signals}
        externalActivities={activities}
        onTotalRowsChange={onTotalRowsChange}
        onSignalCountsChange={onSignalCountsChange}
        onActivityCountsChange={onActivityCountsChange}
      />
    </div>
  );
};

ChemicalAssayInferredSection.displayName = "ChemicalAssayInferredSection";
export default ChemicalAssayInferredSection;

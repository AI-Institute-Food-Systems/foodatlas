"use client";

// The assay-inferred half of the merged Chemicals tab on disease pages.
//
// A different evidence source from the CTD literature rows it now sits
// under: this is "was Active in a shared assay", not "a study reported
// it". Kept as its own table rather than merged row-wise because the two
// carry almost disjoint columns.

import { useCallback } from "react";

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import Heading from "@/components/basic/Heading";
import { getDiseaseChemicalAssociations } from "@/utils/fetching";

interface Props {
  commonName: string;
  search?: string;
  onTotalRowsChange?: (total: number) => void;
}

const DiseaseAssayInferredSection = ({
  commonName,
  search = "",
  onTotalRowsChange,
}: Props) => {
  const fetcher = useCallback(
    () => getDiseaseChemicalAssociations(commonName),
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
          Chemicals associated with this disease because they were{" "}
          <em>Active</em> in a bioactivity assay that this disease&apos;s
          bridge ties in via target genes or mechanism. Assay signal, not a
          curated claim — read it alongside the literature rows above rather
          than as a second opinion on them.
        </p>
      </div>
      <AssayInferredAssociationsTable
        commonName={commonName}
        peer="chemical"
        fetcher={fetcher}
        externalSearch={search}
        onTotalRowsChange={onTotalRowsChange}
      />
    </div>
  );
};

DiseaseAssayInferredSection.displayName = "DiseaseAssayInferredSection";
export default DiseaseAssayInferredSection;

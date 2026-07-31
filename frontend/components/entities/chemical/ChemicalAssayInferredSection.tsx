"use client";

// Diseases associated with this chemical via shared bioactivity assays.
// Rendered as the "Diseases (assay-inferred)" tab on chemical pages.
// See AssayInferredAssociationsTable + inferred-bioactivity-efficacy-column.md
// for context.

import { useCallback } from "react";

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import { getChemicalDiseaseAssociations } from "@/utils/fetching";

interface Props {
  commonName: string;
}

const ChemicalAssayInferredSection = ({ commonName }: Props) => {
  const fetcher = useCallback(
    () => getChemicalDiseaseAssociations(commonName),
    [commonName]
  );
  return (
    <div className="flex flex-col gap-4">
      <p className="text-sm text-light-400 leading-relaxed max-w-2xl">
        Diseases this chemical is associated with, inferred from shared
        bioactivity assays. A row means the chemical has ≥1 <em>Active</em>{" "}
        measurement in an assay the disease-bridge ties to the disease via
        target genes or mechanism. This is a different evidence source from
        the CTD literature correlations in the Health Impacts tab.
      </p>
      <AssayInferredAssociationsTable
        commonName={commonName}
        peer="disease"
        fetcher={fetcher}
        tabId="assay-inferred"
      />
    </div>
  );
};

ChemicalAssayInferredSection.displayName = "ChemicalAssayInferredSection";
export default ChemicalAssayInferredSection;

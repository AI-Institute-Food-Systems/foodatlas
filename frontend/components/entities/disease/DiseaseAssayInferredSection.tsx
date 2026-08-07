"use client";

// Chemicals associated with this disease via shared bioactivity assays.
// Rendered as the "Chemicals (assay-inferred)" tab on disease pages.
// Mirror of ChemicalAssayInferredSection.

import { useCallback } from "react";

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import { getDiseaseChemicalAssociations } from "@/utils/fetching";

interface Props {
  commonName: string;
}

const DiseaseAssayInferredSection = ({ commonName }: Props) => {
  const fetcher = useCallback(
    () => getDiseaseChemicalAssociations(commonName),
    [commonName]
  );
  return (
    <div className="flex flex-col gap-4">
      <p className="text-sm text-light-400 leading-relaxed max-w-2xl">
        Chemicals associated with this disease, inferred from shared
        bioactivity assays. A row means the chemical has ≥1 <em>Active</em>{" "}
        measurement in an assay this disease&apos;s bridge ties in via target
        genes or mechanism. This is a different evidence source from the
        CTD literature correlations in the Health Impacts tab.
      </p>
      <AssayInferredAssociationsTable
        commonName={commonName}
        peer="chemical"
        fetcher={fetcher}
        tabId="assay-inferred"
      />
    </div>
  );
};

DiseaseAssayInferredSection.displayName = "DiseaseAssayInferredSection";
export default DiseaseAssayInferredSection;

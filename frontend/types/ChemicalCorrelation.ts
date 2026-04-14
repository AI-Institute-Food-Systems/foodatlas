import { Evidence } from "@/types";

export type ChemicalCorrelation = {
  id: string;
  name: string;
  source_chemical_name?: string;
  source_chemical_foodatlas_id?: string;
  sources: string[];
  evidences: Evidence[];
};

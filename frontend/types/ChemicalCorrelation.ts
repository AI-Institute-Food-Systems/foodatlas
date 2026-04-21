import { Evidence } from "@/types";
import { AmbiguitySibling } from "@/types/Metadata";

export type ChemicalCorrelation = {
  id: string;
  name: string;
  source_chemical_name?: string;
  source_chemical_foodatlas_id?: string;
  sources: string[];
  evidences: Evidence[];
  ambiguity_siblings?: AmbiguitySibling[];
};

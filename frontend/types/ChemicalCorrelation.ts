import { Evidence } from "@/types";
import { AmbiguitySibling } from "@/types/Metadata";

export type ChemicalCorrelation = {
  id: string;
  name: string;
  // "r4" improves the disease, "r3" worsens it. Present on every row so
  // one table can carry both directions.
  relationship_id?: string;
  source_chemical_name?: string;
  source_chemical_foodatlas_id?: string;
  sources: string[];
  evidences: Evidence[];
  ambiguity_siblings?: AmbiguitySibling[];
};

import { Evidence } from "@/types";
import { AmbiguitySibling } from "@/types/Metadata";

export type ChemicalCorrelation = {
  id: string;
  name: string;
  // Directions this pair has been reported in — ["r4"], ["r3"], or both.
  // The API groups the view's per-direction rows into one row per pair,
  // so a pair reported both ways arrives once, carrying both ids and its
  // evidence split by direction.
  relationship_ids?: string[];
  improves_evidences?: Evidence[] | null;
  worsens_evidences?: Evidence[] | null;
  source_chemical_name?: string;
  source_chemical_foodatlas_id?: string;
  sources: string[];
  // Deduped union of both directions, computed server-side. Optional
  // because a row can predate it; use `rowEvidences` rather than
  // reading it directly.
  evidences?: Evidence[];
  ambiguity_siblings?: AmbiguitySibling[];
};

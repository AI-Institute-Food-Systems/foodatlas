// Chemical↔disease associations inferred from shared bioactivity assays
// (a chemical is associated with a disease when it has ≥1 Active
// measurement in an assay the bioactivity disease-bridge ties to that
// disease via target genes / mechanism). Backed by
// GET /chemical/disease-associations?common_name=<chemical> and the
// symmetric GET /disease/chemical-associations?common_name=<disease>.
//
// NOT the same signal as /chemical/correlation (CTD literature).
// Ordered by n_assays desc on both directions.

// A protein target the association runs through. `label` is the readable
// name the API resolved from the assay records, or null for the ~1% of
// genes it has no name for — render the id in that case. The API also
// collapses the Entrez/UniProt pair for one protein into a single entry,
// so this list is one-per-protein rather than one-per-identifier.
export type AssayTarget = {
  id: string;
  label: string | null;
};

export type AssayInferredAssociation = {
  chemical_name: string;
  chemical_foodatlas_id: string;
  disease_name: string;
  disease_foodatlas_id: string;
  n_assays: number;
  n_active_measurements: number;
  // The CTD direct-evidence class(es) behind the link. Exactly two values
  // exist and they point opposite ways: "therapeutic" (the chemical treats
  // the disease) and "marker/mechanism" (it marks or drives it).
  relationships: string[];
  // Raw target gene ids ("NCBIGene: 1956", "UniProt: P00533"). Capped at 50
  // per row by the materializer (GENE_CAP). Prefer `targets`, which carries
  // the same genes with names attached.
  target_genes: string[];
  targets: AssayTarget[];
  // Assay ids ("AID: 1055355"). Capped at 25 per row by the materializer
  // (ASSAY_CAP), so a row with n_assays > 25 lists a sample, not all of them.
  assays: string[];
  // What those assays were measuring — "anticancer", "antidementia", …
  // Attached by the API from mv_disease_bioactivity, which is this same
  // evidence one grain finer. Optional because a row can predate the
  // field; treat a missing value as an empty list, not as "unknown".
  bioactivities?: string[];
  // The same vocabulary as `relationships`, but from CTD literature rather
  // than the assay bridge — so the two can be compared. Empty for ~97.5% of
  // rows, which is what makes a match worth surfacing.
  literature_directions: string[];
};

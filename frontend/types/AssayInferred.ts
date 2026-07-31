// Chemical↔disease associations inferred from shared bioactivity assays
// (a chemical is associated with a disease when it has ≥1 Active
// measurement in an assay the bioactivity disease-bridge ties to that
// disease via target genes / mechanism). Backed by
// GET /chemical/disease-associations?common_name=<chemical> and the
// symmetric GET /disease/chemical-associations?common_name=<disease>.
//
// NOT the same signal as /chemical/correlation (CTD literature).
// Ordered by n_assays desc on both directions.
export type AssayInferredAssociation = {
  chemical_name: string;
  chemical_foodatlas_id: string;
  disease_name: string;
  disease_foodatlas_id: string;
  n_assays: number;
  n_active_measurements: number;
  // e.g. ["marker/mechanism"] — the bioactivity disease-bridge
  // relationship type(s) backing the inference.
  relationships: string[];
  // Assay target gene ids ("NCBIGene: 1956", "UniProt: ..."). Capped at
  // 50 per row by the API.
  target_genes: string[];
  // Assay ids ("AID: 1055355"). Capped at 25 per row by the API.
  assays: string[];
};

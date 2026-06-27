export type BioactivityAssayMeta = {
  source: string | null;
  description: string | null;
  target_name: string | null;
  target_organism: string | null;
  target_uniprot: string | null;
  target_entrez_gene: string | null;
  n_measurements: number | null;
};

// Light measurement row used inside the list-endpoint nested array
// (the MV-capped sample). Modal reads from here today; will switch to
// BioactivityMeasurementFull from /bioactivity/measurements once deployed.
export type BioactivityMeasurement = {
  endpoint: string | null;
  outcome: string | null;
  value: number | null;
  unit: string | null;
  assay: string | null;
  // Optional — present in the materialised sample ("in vitro" / "in vivo");
  // surfaced as a per-row chip in the list tables.
  evidence_type?: string | null;
};

// Full measurement payload from /bioactivity/measurements — all 16 cols
// from base_attestations_bioactivity + enriched assay_meta.
export type BioactivityMeasurementFull = {
  bioactivity_metadata_id: string;
  exhibit_type: string | null;
  assay: string | null;
  outcome: string | null;
  endpoint: string | null;
  relation: string | null;
  value: number | null;
  unit: string | null;
  efficacy_zeroactivity: number | null;
  efficacy_infiniteactivity: number | null;
  efficacy_logac50_value: number | null;
  efficacy_hillslope: number | null;
  evidence_source: string | null;
  evidence_type: string | null;
  evidence_fit_r2: number | null;
  evidence_fit_curveclass: string | null;
  assay_meta: BioactivityAssayMeta | null;
};

// Computed by the backend as the max-by-value measurement for a row.
// Surfaced in the tables as the "headline" / "Top measurement" column.
export type BioactivityTopMeasurement = {
  endpoint: string | null;
  value: number | null;
  unit: string | null;
};

export type BioactivityPotencySummary = {
  endpoint: string | null;
  unit: string | null;
  median: number | null;
  n: number;
};

export type BioactivityChemicalRow = {
  id: string;
  name: string;
  measurement_count: number;
  active_count: number;
  inactive_count: number;
  measurements: BioactivityMeasurement[];
  top_measurement: BioactivityTopMeasurement | null;
  // # of distinct foods containing this chemical. Only populated by
  // /bioactivity/chemicals (server-side correlated subquery); undefined
  // on the chemical-bioactivities direction where it's not relevant.
  n_foods?: number;
};

export type BioactivityFoodRow = {
  id: string;
  name: string;
  measurement_count: number;
  measurements: BioactivityMeasurement[];
  top_measurement: BioactivityTopMeasurement | null;
};

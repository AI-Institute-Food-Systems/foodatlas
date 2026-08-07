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

// Row returned by GET /food/efficacy?common_name=<food>. One row per
// (chemical × bioactivity) for a food where the chemical's dietary
// concentration could be evaluated against a Hill fit for the
// bioactivity's assay set. See inferred-bioactivity-efficacy-column.md
// for the semantics; the inferred-bioactivities table joins these onto
// its rows on (chemical_foodatlas_id, bioactivity_foodatlas_id) to
// render an Efficacy column.
export type FoodEfficacyRow = {
  food_name: string;
  food_foodatlas_id: string;
  chemical_name: string;
  chemical_foodatlas_id: string;
  cid: number | null;
  // Empty strings + bioactivity_id_raw === "UNCLASSIFIED" when the
  // efficacy was computed against a raw ToxCast target that hasn't been
  // mapped to a FoodAtlas bioactivity concept yet.
  bioactivity_name: string;
  bioactivity_foodatlas_id: string;
  bioactivity_id_raw: string;
  food_conc_mg_per_100g: number | null;
  food_conc_mass_fraction_pct: number | null;
  conc_quality_flag: string | null;
  molecular_weight: number | null;
  food_conc_m: number | null;
  food_conc_logm: number | null;
  rep_source_assay_id: string | null;
  endpoint_type: string | null;
  endpoint_class: string | null;
  curve_method: string | null;
  logac50: number | null;
  hillslope: number | null;
  zeroactivity: number | null;
  infiniteactivity: number | null;
  n_curves: number | null;
  n_curves_4param: number | null;
  // Total assays backing (chemical, bioactivity) from mv_chemical_bioactivity;
  // always ≥ n_curves (the delta is MIC / binding-kinetic records without a
  // fittable AC50). Present on staging as of 2026-08-05; optional here until
  // the backend change reaches every environment the frontend can hit.
  n_measurements_total?: number | null;
  curve_agreement: string | null;
  ac50_spread_log: number | null;
  logac50_median: number | null;
  logac50_min: number | null;
  logac50_max: number | null;
  // The two headline fields the frontend surfaces:
  dose_over_ac50_log: number | null;
  conc_vs_ac50: "above" | "below" | string | null;
  efficacy_fraction: number | null;
  efficacy_response: number | null;
  saturated: boolean | null;
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
  // Chemical classification (e.g. ["flavonoid", "polyphenol"]). Only
  // populated by /bioactivity/chemicals for the Category column +
  // sidebar filter. Undefined elsewhere.
  chemical_classification?: string[] | null;
};

export type BioactivityFoodRow = {
  id: string;
  name: string;
  measurement_count: number;
  measurements: BioactivityMeasurement[];
  top_measurement: BioactivityTopMeasurement | null;
};

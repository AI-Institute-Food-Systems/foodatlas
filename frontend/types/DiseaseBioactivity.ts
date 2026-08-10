// Disease↔bioactivity, attributed through the assay that actually bridges
// to the disease. Backed by:
//   GET /disease/bioactivities?common_name=<disease>
//   GET /disease/bioactivity-chemicals?common_name=<disease>[&bioactivity=]
//
// The attribution matters. Reaching a bioactivity the loose way — disease →
// chemical → every bioactivity that chemical was ever measured for — credits
// melanoma with 1,571 "antiviral" chemicals. Going through the bridging assay
// gives 3. These endpoints do the latter.
//
// NOT the same signal as /disease/chemical-associations, which collapses the
// assay's bioactivity away and only answers *which* chemicals are linked.

// The food where a chemical's dietary dose comes closest to its own AC50 for
// this bioactivity. Null on the ~97% of rows whose chemical either doesn't
// occur in a food we have composition for, or has no fittable curve — mostly
// pharmaceuticals that reached the graph through assay data alone.
export type DietaryDose = {
  food_name: string;
  food_foodatlas_id: string;
  food_conc_mg_per_100g: number | null;
  // "suspect_high" marks an implausible source concentration. The row is
  // still shown — flagged, not hidden — but it loses sort ties to "ok" rows
  // so the top of the list isn't a ranking of data errors.
  conc_quality_flag: string | null;
  efficacy_fraction: number | null;
  // The discriminating metric: how many log units the dietary dose sits above
  // the curve's AC50. efficacy_fraction saturates (most rows read 100%), so it
  // cannot rank; this can.
  dose_over_ac50_log: number | null;
  conc_vs_ac50: "above" | "below" | string | null;
  logac50: number | null;
  n_curves: number | null;
  endpoint_type: string | null;
  saturated: boolean | null;
};

export type DiseaseBioactivityChemical = {
  bioactivity_name: string;
  bioactivity_foodatlas_id: string;
  chemical_name: string;
  chemical_foodatlas_id: string;
  // Bridging assays for this (disease, bioactivity, chemical) triple.
  n_assays: number;
  n_active_measurements: number;
  // e.g. ["marker/mechanism", "therapeutic"] from the disease bridge.
  relationships: string[];
  dietary: DietaryDose | null;
};

export type DiseaseBioactivitySummary = {
  bioactivity_name: string;
  bioactivity_foodatlas_id: string;
  n_chemicals: number;
  // Of n_chemicals, how many carry a dietary dose — the honest denominator
  // for anything the UI describes as "in food".
  n_dietary_chemicals: number;
  n_assays: number;
  n_active_measurements: number;
  best_dose_over_ac50_log: number | null;
};

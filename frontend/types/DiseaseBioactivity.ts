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
// Assay counts only, by design. An earlier revision also carried the food
// where each chemical's dietary dose came closest to its AC50; it was pulled
// because the AC50 is constant across foods within a (chemical, bioactivity)
// pair, making "best food" just "most concentrated food", with runners-up
// usually within noise. See the API repository docstring.
//
// NOT the same signal as /disease/chemical-associations, which collapses the
// assay's bioactivity away and only answers *which* chemicals are linked.

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
};

export type DiseaseBioactivitySummary = {
  bioactivity_name: string;
  bioactivity_foodatlas_id: string;
  n_chemicals: number;
  n_assays: number;
  n_active_measurements: number;
};

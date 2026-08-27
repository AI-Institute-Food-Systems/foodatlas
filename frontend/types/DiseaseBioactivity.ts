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

import type { AssayTarget } from "./AssayInferred";

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
  target_genes: string[];
  targets: AssayTarget[];
  // Capped at 25 by the materializer, so n_assays may exceed assays.length.
  assays: string[];
  // Same vocabulary as `relationships`, from CTD literature instead of the
  // assay bridge. Usually empty.
  literature_directions: string[];
};

// The direction split, reported as counts of chemicals rather than chips.
// At a grain that rolls up hundreds or thousands of chemicals, "does any of
// this involve therapeutic evidence?" is nearly always yes and says nothing;
// "271 of 2,038" says how much. A chemical classified both ways counts in
// both, so these do not sum to n_chemicals.
type DirectionCounts = {
  n_therapeutic: number;
  n_marker: number;
  // Chemicals whose link CTD literature also records, in any direction.
  n_literature: number;
};

export type DiseaseBioactivitySummary = DirectionCounts & {
  bioactivity_name: string;
  bioactivity_foodatlas_id: string;
  n_chemicals: number;
  n_assays: number;
  n_active_measurements: number;
};

// The mirror image, for the Diseases tab on bioactivity pages:
//   GET /bioactivity/diseases?common_name=<bioactivity>
//
// Flat rather than two-level — a bioactivity reaches at most 1,282 diseases,
// few enough to list without the chip-and-drilldown shape the disease side
// needs for its 20 bioactivities.
export type BioactivityDisease = DirectionCounts & {
  disease_name: string;
  disease_foodatlas_id: string;
  // Distinct chemicals linking this disease to the bioactivity.
  n_chemicals: number;
  n_assays: number;
  n_active_measurements: number;
  // The targets the most chemicals converge on, ranked by chemical count —
  // not the union, which over thousands of chemicals is a laundry list.
  target_genes: string[];
  targets: AssayTarget[];
};

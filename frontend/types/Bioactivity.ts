export type BioactivityPotency = {
  value: number | null;
  unit: string | null;
};

export type BioactivityHillCurve = {
  zero_activity: number | null;
  infinite_activity: number | null;
  log_ac50: number | null;
  hill_slope: number | null;
};

export type BioactivityMeasurement = {
  attestation_id: string;
  bioactivity_metadata_id: string;
  source_assay_id: string | null;
  target_ids: string[];
  potency: BioactivityPotency;
  hill_curve: BioactivityHillCurve;
  evidence_source: string | null;
  evidence_type: string | null;
};

export type BioactivityChemicalRow = {
  id: string;
  name: string;
  measurement_count: number;
  measurements: BioactivityMeasurement[];
};

export type BioactivityFoodRow = {
  id: string;
  name: string;
  exhibit_type: "direct" | "inherited";
  via_chemical_id: string | null;
  via_chemical_name: string | null;
  efficacy_pred: number | null;
  evidence_count: number;
  evidences: BioactivityMeasurement[];
};

export type BioactivityDiseaseRow = {
  id: string;
  name: string;
  polarity: string | null;
  target_ids: string[];
  evidence_count: number;
  evidences: Array<{
    attestation_id: string;
    bioactivity_metadata_id: string;
    target_ids: string[];
    evidence_source: string | null;
    evidence_type: string | null;
  }>;
};

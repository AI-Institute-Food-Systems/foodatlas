export type BioactivityMeasurement = {
  endpoint: string | null;
  outcome: string | null;
  value: number | null;
  unit: string | null;
  assay: string | null;
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
  potency_summary: BioactivityPotencySummary[];
  measurements: BioactivityMeasurement[];
};

export type BioactivityFoodRow = {
  id: string;
  name: string;
  measurement_count: number;
  measurements: BioactivityMeasurement[];
};

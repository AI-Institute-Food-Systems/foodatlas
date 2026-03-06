import { FoodEvidence } from "@/types/Evidence";

type Concentration = {
  unit: string;
  value: number;
  converted: boolean;
  base_units: string[];
};

export type FoodCompositionData = {
  id: string;
  name: string;
  median_concentration: Concentration | null;
  foodatlas_evidences: FoodEvidence[] | null;
  fdc_evidences: FoodEvidence[] | null;
  dmd_evidences: FoodEvidence[] | null;
  nutrient_classification: string[];
  chemical_classification: string[];
};

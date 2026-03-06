import { Evidence } from "@/types";

export type ChemicalCorrelation = {
  id: string;
  name: string;
  sources: string[];
  evidences: Evidence[];
};

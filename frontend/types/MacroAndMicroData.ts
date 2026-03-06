import { Concentration } from "@/types/Concentration";

type MacroAndMicroConcentration = {
  name: string;
  median_concentration: Concentration | null;
};

export type MacroAndMicroData = {
  [key: string]: MacroAndMicroConcentration[];
};

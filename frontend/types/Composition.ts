import { Concentration } from "@/types/Concentration";

export type Composition = {
  id: string;
  name: string;
  median_concentration: Concentration[] | null;
};

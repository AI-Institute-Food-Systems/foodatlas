import { ExternalIds } from "@/types/ExternalIds";

export type Metadata = {
  id: string;
  entity_type: "food" | "chemical" | "disease";
  common_name: string;
  scientific_name: string | null;
  synonyms: string[];
  food_classification?: string[];
  chemical_classification?: string[];
  flavor_descriptors?: string[];
  external_ids: ExternalIds;
};

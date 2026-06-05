import { ExternalIds } from "@/types/ExternalIds";

export type AmbiguitySibling = {
  foodatlas_id: string;
  common_name: string;
};

export type Metadata = {
  id: string;
  entity_type: "food" | "chemical" | "disease" | "bioactivity";
  common_name: string;
  scientific_name: string | null;
  synonyms: string[];
  food_classification?: string[];
  chemical_classification?: string[];
  flavor_descriptors?: string[];
  description?: string;
  external_ids: ExternalIds;
  ambiguity_siblings?: AmbiguitySibling[];
};

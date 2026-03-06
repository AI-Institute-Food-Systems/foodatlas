type ExternalReference = {
  id: string;
  url: string | null;
};

type Reference = {
  ids: ExternalReference[];
  has_url: boolean;
  display_name: string;
};

type ExternalReferences = {
  [key: string]: Reference;
};

export type Suggestion = {
  foodatlas_id: string;
  associations: string;
  entity_type: string;
  common_name: string;
  scientific_name: string;
  synonyms: string[];
  external_references: ExternalReferences;
};

export type TaxonomyNode = {
  id: string;
  name: string;
  has_page: boolean;
};

export type TaxonomyEdge = {
  child_id: string;
  parent_id: string;
};

export type TaxonomyData = {
  entity_id: string | null;
  nodes: TaxonomyNode[];
  edges: TaxonomyEdge[];
};

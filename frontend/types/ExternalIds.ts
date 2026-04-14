type ExternalId = {
  id: string;
  url: string | null;
};

export type ExternalIds = {
  [key: string]: { ids: ExternalId[]; display_name: string };
};

import { Evidence } from "@/types/Evidence";

export type RelationInfo = {
  relation: string;
  name: string;
  id: string;
  sources: string[];
  evidences: Evidence[];
};

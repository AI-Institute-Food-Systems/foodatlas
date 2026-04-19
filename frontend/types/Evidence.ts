import { Concentration, Pmcid, Pmid } from "@/types";

type Reference = {
  id: string;
  display_name: string;
  source_name: string;
  url: string;
};

export type Evidence = {
  pmid: Pmid;
  pmcid?: Pmcid;
};

// export type FoodEvidence = {
//   source: string[];
//   premise: string;
//   quality: null;
//   reference: Reference;
//   concentration: {
//     "llit2kg:gpt-3.5-ft": Concentration[];
//     "lit2kg:gpt-4-ft": Concentration[];
//   };
//   extracted_food: string;
//   extracted_chemical: string;
// };

export type FoodEvidenceExtraction = {
  extracted_food_name: string | null;
  extracted_chemical_name: string | null;
  extracted_concentration: string | null;
  converted_concentration: Concentration;
  method: string;
  food_candidates?: string[] | null;
  chemical_candidates?: string[] | null;
};

export type FoodEvidence = {
  premise: string;
  extraction: FoodEvidenceExtraction[];
  reference: {
    id: string;
    source_name: "FoodAtlas" | "FDC" | "DMD";
    display_name: string;
    url: string;
  };
};

// Shared vocabulary for the "Report an issue" flow. The categories are
// mirrored on the server (`/report/issue/send`); keep both lists in
// sync when adding one.
export const REPORT_CATEGORIES = [
  "Extraction error",
  "Duplicate",
  "Wrong value",
  "Wrong unit",
  "Missing data",
  "Other",
] as const;

export type ReportCategory = (typeof REPORT_CATEGORIES)[number];

// Discriminated union carried from every evidence surface. The `kind`
// lands verbatim in the email so ops can tell at a glance where a
// report came from. `entitySlug` is optional — pageUrl (captured at
// submit time in ReportRequestBody) already identifies the page; slug
// is a convenience for ops that not every surface has cheaply in hand.
export type ReportContext =
  | {
      kind: "food-composition-evidence";
      entityType: "food" | "chemical";
      entitySlug?: string;
      attestationId?: string;
      chemicalId?: string;
      foodId?: string;
      extractedChemical?: string;
      extractedFood?: string;
      concentration?: string;
      referenceUrl?: string;
    }
  | {
      kind: "food-composition-row";
      entityType: "food" | "chemical";
      entitySlug?: string;
      chemicalId?: string;
      chemicalName?: string;
      // Populated when the report originates on the chemical page (row
      // is a food) — food page reports leave these undefined and use
      // chemicalId/chemicalName instead.
      foodId?: string;
      foodName?: string;
      dataPointCount?: number;
    }
  | {
      kind: "bioactivity-measurement";
      entityType: "food" | "chemical" | "bioactivity";
      entitySlug?: string;
      bioactivityId?: string;
      bioactivityName?: string;
      assay?: string;
      endpoint?: string;
      outcome?: string;
      value?: string;
      unit?: string;
    }
  | {
      kind: "bioactivity-row";
      // Reflects the *page* the user is on when they file the report,
      // not the row's own entity kind. On a bioactivity page, rows can
      // be either chemicals or foods; on a food or chemical page, rows
      // are bioactivities.
      entityType: "food" | "chemical" | "bioactivity";
      entitySlug?: string;
      bioactivityId?: string;
      bioactivityName?: string;
      activeCount?: number;
      inactiveCount?: number;
    }
  | {
      kind: "correlation-evidence";
      entityType: "chemical" | "disease";
      entitySlug?: string;
      counterpartId?: string;
      counterpartName?: string;
      pmid?: string;
      pmcid?: string;
      referenceUrl?: string;
    }
  | {
      kind: "correlation-row";
      entityType: "chemical" | "disease";
      entitySlug?: string;
      counterpartId?: string;
      counterpartName?: string;
      pmidCount?: number;
    }
  | {
      // Food page's "Inferred via composition" table — one row per
      // (chemical-in-food, bioactivity-of-chemical) pair.
      kind: "food-inferred-bioactivity";
      entityType: "food";
      entitySlug?: string;
      bioactivityId?: string;
      bioactivityName?: string;
      chemicalId?: string;
      chemicalName?: string;
      concentration?: string;
    }
  | {
      // Individual item on the entity's "IDs & Metadata" tab —
      // external IDs, classification, synonyms, ontology parents, or
      // taxonomy tree nodes. `field` distinguishes the sub-surface so
      // triage can filter (e.g. "wrong FoodOn id" vs "spurious synonym").
      kind: "metadata-item";
      entityType: "food" | "chemical" | "bioactivity" | "disease";
      entitySlug?: string;
      field:
        | "external_id"
        | "classification"
        | "parent"
        | "synonym"
        | "taxonomy_node"
        | "flavor";
      // Human label of the field (e.g. "FoodOn", "Classification").
      label?: string;
      // The specific value being flagged (e.g. "FOODON_00003443",
      // "flavonoid", "cherry tomato").
      value: string;
      // Optional source/ontology key when relevant (e.g. "FoodOn" for
      // external IDs, "FoodAtlas" taxonomy for tree nodes).
      source?: string;
    };

export type ReportContextKind = ReportContext["kind"];

export interface ReportRequestBody {
  category: ReportCategory;
  description: string;
  email?: string;
  context?: ReportContext;
  pageUrl?: string;
}

// Row builders for TabSnapshot: turn the payloads an entity page already
// fetched on the server into plain rows of text and links. Pure and
// server-safe — nothing here may import from a "use client" module, whose
// non-component exports can't be called on the server.

import { entityPath, type EntityType } from "@/utils/site";
import type { AssayInferredAssociation } from "@/types/AssayInferred";
import type { BioactivityDisease } from "@/types/DiseaseBioactivity";
import type { ChemicalCorrelation } from "@/types/ChemicalCorrelation";

export type SnapshotLink = { label: string; href: string };
export type SnapshotCell = string | number | SnapshotLink;
export type SnapshotSection = {
  heading: string;
  columns: string[];
  rows: SnapshotCell[][];
};

// The paginated tables' first page is already this size; the unpaginated
// assay-inferred endpoints return every row, so cap those to match.
export const SNAPSHOT_ROW_CAP = 25;

type Payload<T> = { data?: T[] | null } | null | undefined;

const rowsOf = <T>(payload: Payload<T>): T[] =>
  (Array.isArray(payload?.data) ? payload.data : []).slice(0, SNAPSHOT_ROW_CAP);

const link = (type: EntityType, name: string): SnapshotLink => ({
  label: name,
  href: entityPath(type, name),
});

// /food/bioactivities, /chemical/bioactivities, /bioactivity/{chemicals,foods}
export const bioactivityListSection = (
  heading: string,
  peer: EntityType,
  payload: Payload<{ name?: string; measurement_count?: number }>
): SnapshotSection => ({
  heading,
  columns: [peer === "bioactivity" ? "Bioactivity" : peer === "food" ? "Food" : "Chemical", "Measurements"],
  rows: rowsOf(payload)
    .filter((r) => r.name)
    .map((r) => [link(peer, r.name!), r.measurement_count ?? ""]),
});

// /food/inferred-bioactivities: food contains the chemical, the chemical was
// measured against the bioactivity.
export const inferredBioactivitySection = (
  payload: Payload<{ bioactivity?: string; chemical?: string }>
): SnapshotSection => ({
  heading: "Inferred via composition",
  columns: ["Bioactivity", "Via chemical"],
  rows: rowsOf(payload)
    .filter((r) => r.bioactivity && r.chemical)
    .map((r) => [link("bioactivity", r.bioactivity!), link("chemical", r.chemical!)]),
});

// /{chemical,disease}/correlation — CTD literature. `peer` is the side each
// row names: diseases on a chemical page, chemicals on a disease page.
export const literatureSection = (
  peer: "chemical" | "disease",
  payload: { data?: { associations?: ChemicalCorrelation[] } } | null | undefined
): SnapshotSection => ({
  heading: "From Literature",
  columns: [peer === "disease" ? "Disease" : "Chemical", "Evidence"],
  rows: (payload?.data?.associations ?? [])
    .slice(0, SNAPSHOT_ROW_CAP)
    .filter((r) => r.name)
    .map((r) => [
      link(peer, r.name),
      r.evidences?.length ??
        (r.improves_evidences?.length ?? 0) + (r.worsens_evidences?.length ?? 0),
    ]),
});

// /chemical/disease-associations, /disease/chemical-associations
export const assayInferredSection = (
  peer: "chemical" | "disease",
  payload: Payload<AssayInferredAssociation>
): SnapshotSection => ({
  heading: "From Lab Assays",
  columns: [peer === "disease" ? "Disease" : "Chemical", "Assays"],
  rows: rowsOf(payload).map((r) => [
    link(peer, peer === "disease" ? r.disease_name : r.chemical_name),
    r.n_assays,
  ]),
});

// /bioactivity/diseases
export const bioactivityDiseasesSection = (
  payload: Payload<BioactivityDisease>
): SnapshotSection => ({
  heading: "Diseases",
  columns: ["Disease", "Chemicals"],
  rows: rowsOf(payload).map((r) => [link("disease", r.disease_name), r.n_chemicals]),
});

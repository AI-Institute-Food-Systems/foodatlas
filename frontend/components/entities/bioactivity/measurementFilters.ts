// The measurements modal's filter rule, shared by the modal and the
// contract test (contracts/bioactivity_filters.json). The API applies the
// same rule in SQL for the "N assays" button, so this must stay in step
// with backend/api/src/repositories/bioactivity_counts.py.

export type ModalOutcome =
  | "all"
  | "active"
  | "inactive"
  | "inconclusive"
  | "unspecified";

export type ModalDimension = "outcome" | "source" | "evidence" | "unit";

export interface ModalFilters {
  outcome: ModalOutcome;
  source: string;
  evidence: string[];
  units: string[];
  search: string;
}

export interface FilterableMeasurement {
  outcome?: string | null;
  evidence_source?: string | null;
  evidence_type?: string | null;
  unit?: string | null;
  assay?: string | null;
  endpoint?: string | null;
}

export const matchesSourceKind = (
  source: string | null | undefined,
  kind: string
): boolean => {
  if (!kind) return true;
  const s = (source ?? "").toLowerCase();
  if (kind === "experimental") return s.startsWith("exp");
  if (kind === "predicted") return s.startsWith("pred") || s.startsWith("comp");
  return true;
};

/** True when `r` passes every filter except `skip` (for that facet's counts). */
export const measurementMatches = (
  r: FilterableMeasurement,
  f: ModalFilters,
  skip: ModalDimension | null = null
): boolean => {
  if (skip !== "outcome" && f.outcome !== "all") {
    if ((r.outcome ?? "").toLowerCase().trim() !== f.outcome) return false;
  }
  if (skip !== "source" && f.source && !matchesSourceKind(r.evidence_source, f.source)) {
    return false;
  }
  if (skip !== "evidence" && f.evidence.length > 0) {
    if (!f.evidence.includes((r.evidence_type ?? "").trim())) return false;
  }
  if (skip !== "unit" && f.units.length > 0) {
    if (!f.units.includes(r.unit ?? "")) return false;
  }
  const q = f.search.trim().toLowerCase();
  if (q) {
    const haystack = `${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase();
    if (!haystack.includes(q)) return false;
  }
  return true;
};

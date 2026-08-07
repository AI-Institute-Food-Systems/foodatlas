import { DownloadEntry, MacroAndMicroData, Metadata, TaxonomyData } from "@/types";

// API base URL. On the server we hit the upstream ALB directly (it may be
// HTTP — that's fine server-side). On the client we route through a
// same-origin rewrite (/_proxy-api → ALB; configured in next.config.mjs)
// so the browser never makes a mixed-content request when the page is
// served over HTTPS.
export const apiBase = (): string =>
  typeof window === "undefined"
    ? (process.env.NEXT_PUBLIC_API_URL ?? "")
    : "/_proxy-api";

// The /bioactivity/metadata endpoint returns external_ids in a flat
// shape ({key: ["id1", ...]}) while every other entity endpoint returns
// the structured shape ({key: {display_name, ids: [{id, url}]}}).
// Normalize here so MetainformationSection sees one shape.
function normalizeExternalIds(raw: unknown): Metadata["external_ids"] {
  if (!raw || typeof raw !== "object") return {} as Metadata["external_ids"];
  const out: Record<
    string,
    { display_name: string; ids: { id: string; url: string | null }[] }
  > = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (Array.isArray(value)) {
      out[key] = {
        display_name: key
          .split("_")
          .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : ""))
          .join(" "),
        ids: value.map((id) => ({ id: String(id), url: null })),
      };
    } else if (value && typeof value === "object") {
      out[key] = value as (typeof out)[string];
    }
  }
  return out;
}

// fetch metadata for a given entity
// returns null when no entity matches the given common name
export async function getMetaData(
  commonName: string,
  entityType: string
): Promise<Metadata | null> {
  // Best-effort fetch — any failure (network blip, non-200, parse error) is
  // surfaced as `null` so callers can fall back to notFound() / a "missing
  // entity" UI instead of crashing the page with a 500. The staging stack
  // is flaky enough that throwing here turned every blip into a hard error.
  try {
    const res = await fetch(
      `${apiBase()}/${entityType}/metadata?common_name=${encodeURIComponent(commonName)}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return null;
    const data = await res.json();
    const record = data.data[0];
    if (!record) return null;
    return {
      ...record,
      external_ids: normalizeExternalIds(record.external_ids),
    };
  } catch {
    return null;
  }
}

// fetch taxonomy ancestry for a given entity
export async function getTaxonomyData(
  commonName: string,
  entityType: string
): Promise<TaxonomyData> {
  const res = await fetch(
    `${apiBase()}/${entityType}/taxonomy?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!res.ok) {
    throw new Error(
      `Failed to fetch taxonomy for ${entityType} ${commonName}`
    );
  }

  const data = await res.json();

  return data.data;
}

// fetch food macro & micro data
export async function getFoodMacroAndMicroData(
  commonName: string
): Promise<MacroAndMicroData> {
  const response = await fetch(
    `${apiBase()}/food/profile?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed to fetch macro and micro data for food ${commonName}`
    );
  }

  const { data } = await response.json();

  return data;
}

// fetch food composition data, i.e. its chemical composition
//
// `trust` is forwarded to the API:
//   - "default"  : low-trust extractions hidden (server-side filter)
//   - "show_all" : every extraction returned, annotated with `trust_low`
//   - "low_only" : only low-trust extractions returned (used when the user
//                  clicks the trust badge from the row)
export type TrustMode = "default" | "show_all" | "low_only";

export async function getFoodCompositionData(
  commonName: string,
  currentPage: number,
  sourceFilters: string[],
  searchTerm: string,
  sort: { column: string; direction: string },
  showAllConcentrations: boolean,
  classificationFilters: string[] = [],
  trust: TrustMode = "default",
  findChemical: string = ""
) {
  const clsParam =
    classificationFilters.length > 0
      ? `&filter_classification=${classificationFilters.map(encodeURIComponent).join("%2B")}`
      : "";
  const findParam = findChemical
    ? `&find_chemical=${encodeURIComponent(findChemical)}`
    : "";
  const response = await fetch(
    `${apiBase()}/food/composition?common_name=${encodeURIComponent(
      commonName
    )}&page=${currentPage}&filter_source=${sourceFilters.join(
      "%2B"
    )}&search=${encodeURIComponent(searchTerm)}&sort_by=${
      sort.column
    }&sort_dir=${sort.direction}&show_all_rows=${showAllConcentrations}${clsParam}&trust=${trust}${findParam}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to fetch composition data for food ${commonName}`);
  }

  const data = await response.json();

  return data;
}

// fetch faceted composition counts. Every filter mirrors what
// /food/composition takes; each count in the response reflects the
// dimension's rows given every *other* filter — so the sidebar updates
// as the user narrows the view.
interface CompositionCountsFilters {
  sourceFilters?: string[];
  classificationFilters?: string[];
  showAllConcentrations?: boolean;
  showLowTrust?: boolean;
  searchTerm?: string;
}

export async function getFoodCompositionCounts(
  commonName: string,
  filters: CompositionCountsFilters = {}
) {
  const params = new URLSearchParams({ common_name: commonName });
  if (filters.sourceFilters && filters.sourceFilters.length > 0) {
    params.set("filter_source", filters.sourceFilters.join("+"));
  }
  if (
    filters.classificationFilters &&
    filters.classificationFilters.length > 0
  ) {
    params.set("filter_classification", filters.classificationFilters.join("+"));
  }
  if (filters.showAllConcentrations === false) {
    params.set("show_all_rows", "false");
  }
  if (filters.showLowTrust) {
    params.set("trust", "show_all");
  }
  if (filters.searchTerm) {
    params.set("search", filters.searchTerm);
  }
  const response = await fetch(
    `${apiBase()}/food/composition/counts?${params.toString()}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!response.ok) {
    throw new Error(
      `Failed to fetch composition counts for food ${commonName}`
    );
  }

  const data = await response.json();
  return data.data as {
    classification_counts: Record<string, number>;
    source_counts: Record<string, number>;
    // Count of composition rows whose median_concentration is NULL —
    // surfaced next to the "Include without concentration" toggle.
    // Optional so older API builds without the field still parse.
    no_concentration_count?: number;
    // Count of composition rows containing at least one low-trust
    // extraction (llm_plausibility score ≤ threshold) — surfaced next
    // to the "Include low-trust data points" toggle.
    low_trust_count?: number;
  };
}

// fetch chemical composition data, i.e. the foods containing it
export async function getChemicalCompositionData(commonName: string) {
  const res = await fetch(
    `${apiBase()}/chemical/composition?common_name=${encodeURIComponent(commonName)}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 86400 },
    }
  );

  if (!res.ok) {
    throw new Error(
      `Failed to fetch composition data for chemical ${commonName}`
    );
  }

  const { data } = await res.json();

  return data;
}

// fetch disease correlation data for a certain chemical, either negative or positive
export async function getDiseaseData(
  commonName: string,
  currentPage: number,
  tableLocation: string,
  correlationType: "positive" | "negative"
) {
  const url = `${apiBase()}/${tableLocation}/correlation?common_name=${encodeURIComponent(
    commonName
  )}&page=${currentPage}&relation=${correlationType}`;
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
    next: { revalidate: 86400 },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch data for ${tableLocation} ${commonName}`);
  }

  const data = await response.json();

  return data;
}

// fetch db bundle download entries
export async function getDownloadEntries() {
  const response = await fetch(`${apiBase()}/download`, {
    headers: {
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
    next: { revalidate: 300 },
  });

  if (!response.ok) {
    throw new Error("Failed to fetch food composition downloads");
  }

  const { data } = await response.json();

  return data;
}

// Newest bundle for the home-page notification. Returns null on any
// error so the notification can fall back to a static string rather
// than propagating a 500 (staging manifest is occasionally missing).
export async function getLatestBundle(): Promise<DownloadEntry | null> {
  try {
    const response = await fetch(`${apiBase()}/download`, {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
      },
      next: { revalidate: 300 },
    });
    if (!response.ok) return null;
    const { data } = await response.json();
    if (!Array.isArray(data) || data.length === 0) return null;
    // release_date is ISO YYYY-MM-DD, so string sort is chronological.
    // Version is the tiebreaker to keep the pick deterministic when two
    // bundles ship on the same day.
    const sorted = [...data].sort((a, b) => {
      const d = String(b.release_date).localeCompare(String(a.release_date));
      return d !== 0 ? d : String(b.version).localeCompare(String(a.version));
    });
    return sorted[0];
  } catch {
    return null;
  }
}

// Shared list-fetch params for the four paginated bioactivity endpoints —
// page/search/sort mirror /food/composition so tables can use a consistent
// toolbar. All optional; defaults match the backend.
export type BioactivityListParams = {
  page?: number;
  search?: string;
  sortBy?: string;
  sortDir?: "asc" | "desc";
  // When both are set, the API restricts rows to those with at least
  // one measurement matching the endpoint+unit, and `sort_by =
  // "top_measurement_value"` becomes meaningful (sorts by max value
  // across matching measurements).
  filterEndpoint?: string;
  filterUnit?: string;
  // Multi-select evidence-type filter ('+'-separated). Row qualifies
  // if its `measurements` sample carries at least one entry with an
  // `evidence_type` in the selected set (values: molecular-level,
  // in vitro, in vivo, adme-tox). Per-row top_measurement is also
  // recomputed from the filtered sample, so it reflects the filter.
  filterEvidenceType?: string;
  // Multi-select chemical classification filter ('+'-separated) applied
  // by /bioactivity/chemicals only; other list endpoints ignore it.
  filterCategory?: string;
  // Multi-select measurement provenance filter ('+'-separated). Values
  // are "experimental", "predicted", "mixed". Classified per-row by
  // inspecting the capped `measurements` sample's evidence_source
  // values ("exp*" vs "pred*"/"comp*").
  filterSourceKind?: string;
};

const buildBioactivityQuery = (params?: BioactivityListParams): string => {
  const p = new URLSearchParams();
  if (params?.page) p.set("page", String(params.page));
  if (params?.search) p.set("search", params.search);
  if (params?.sortBy) p.set("sort_by", params.sortBy);
  if (params?.sortDir) p.set("sort_dir", params.sortDir);
  if (params?.filterEndpoint) p.set("filter_endpoint", params.filterEndpoint);
  if (params?.filterUnit) p.set("filter_unit", params.filterUnit);
  if (params?.filterEvidenceType)
    p.set("filter_evidence_type", params.filterEvidenceType);
  if (params?.filterCategory) p.set("filter_category", params.filterCategory);
  if (params?.filterSourceKind)
    p.set("filter_source_kind", params.filterSourceKind);
  const qs = p.toString();
  return qs ? `&${qs}` : "";
};

const bioactivityListFetch = async (
  path: string,
  commonName: string,
  params?: BioactivityListParams,
  label: string = "bioactivity list"
) => {
  // Returns null on any non-2xx or network error rather than throwing —
  // staging is flaky enough that we'd rather show an empty state than
  // "An error occurred fetching data". See feedback-graceful-api-failures.
  // (Historical: the /bioactivity/chemicals + /bioactivity/foods 502s
  // that prompted this fallback were fixed once those endpoints became
  // paginated on 2026-07-31; the fallback stays as a general safety
  // net for the other list endpoints.)
  try {
    const res = await fetch(
      `${apiBase()}${path}?common_name=${encodeURIComponent(
        commonName
      )}${buildBioactivityQuery(params)}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) {
      console.warn(`Failed to fetch ${label} for ${commonName}: HTTP ${res.status}`);
      return null;
    }
    return await res.json();
  } catch (err) {
    console.warn(`Failed to fetch ${label} for ${commonName}:`, err);
    return null;
  }
};

// fetch chemicals measured against a bioactivity
export async function getBioactivityChemicals(
  commonName: string,
  params?: BioactivityListParams
) {
  return bioactivityListFetch(
    "/bioactivity/chemicals",
    commonName,
    params,
    "bioactivity chemicals"
  );
}

// fetch foods exhibiting a bioactivity
export async function getBioactivityFoods(
  commonName: string,
  params?: BioactivityListParams
) {
  return bioactivityListFetch(
    "/bioactivity/foods",
    commonName,
    params,
    "bioactivity foods"
  );
}

// bioactivities measured against a chemical (reverse of getBioactivityChemicals)
export async function getChemicalBioactivities(
  commonName: string,
  params?: BioactivityListParams
) {
  return bioactivityListFetch(
    "/chemical/bioactivities",
    commonName,
    params,
    "chemical bioactivities"
  );
}

// bioactivities exhibited by a food
export async function getFoodBioactivities(
  commonName: string,
  params?: BioactivityListParams
) {
  return bioactivityListFetch(
    "/food/bioactivities",
    commonName,
    params,
    "food bioactivities"
  );
}

// Bioactivities inferred transitively: food contains chemical X, X was
// measured against bioactivity Y. Rows carry the Hill-fit efficacy columns
// LEFT JOINed from mv_food_chemical_efficacy (null where a chemical has no
// fittable curve), so this one call backs the whole inferred table —
// including the Unit / Evidence / Source filters, which the shared sidebar
// applies to this table and the direct one alike.
export async function getFoodInferredBioactivities(
  commonName: string,
  params?: BioactivityListParams
) {
  return bioactivityListFetch(
    "/food/inferred-bioactivities",
    commonName,
    params,
    "food inferred bioactivities"
  );
}

// Lazy-load FULL measurements for a single (head, bioactivity) pair —
// bypasses the materialized view's 25-row cap by reading
// base_attestations_bioactivity directly. relationship is "r6" for
// (chemical, bioactivity) or "r5" for (food, bioactivity). Returns null on
// any fetch failure so the modal can render a graceful empty state instead
// of crashing.
export async function getBioactivityMeasurements(
  headId: string,
  tailId: string,
  relationship: "r5" | "r6"
) {
  try {
    const res = await fetch(
      `${apiBase()}/bioactivity/measurements?head_id=${encodeURIComponent(
        headId
      )}&tail_id=${encodeURIComponent(tailId)}&relationship=${relationship}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        // measurements are point-in-time data — 24h cache like everything else
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// Direction selector for the bioactivity table — one per table-page combo.
// "food-inferred-bioactivities" is a virtual direction used by the food
// page's shared sidebar to surface units from ALL chemicals present in
// the food (not just the direct food-level measurements).
export type BioactivityDirection =
  | "bioactivity-chemicals"
  | "bioactivity-foods"
  | "chemical-bioactivities"
  | "food-bioactivities"
  | "food-inferred-bioactivities";

// Distinct (endpoint, unit, count) tuples for the table's filter chips.
// Returns [] on any fetch failure so the table can render without chips.
export async function getBioactivityEndpointOptions(
  commonName: string,
  direction: BioactivityDirection
): Promise<{ endpoint: string; unit: string; count: number }[]> {
  try {
    const res = await fetch(
      `${apiBase()}/bioactivity/endpoints?common_name=${encodeURIComponent(
        commonName
      )}&direction=${direction}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return [];
    const payload = await res.json();
    return (payload?.data ?? []) as {
      endpoint: string;
      unit: string;
      count: number;
    }[];
  } catch {
    return [];
  }
}

// Faceted chemical-classification counts for the bioactivity-chemicals
// sidebar. Accepts the other active filters (unit, source kind, search)
// so the counts reflect what the table would render under each category
// selection.
interface BioactivitySidebarFilters {
  filterUnit?: string;
  filterCategory?: string;
  filterSourceKind?: string;
  search?: string;
}

const buildBioactivitySidebarParams = (
  base: URLSearchParams,
  filters: BioactivitySidebarFilters,
  { skipUnit = false, skipCategory = false, skipSourceKind = false } = {},
) => {
  if (!skipUnit && filters.filterUnit) {
    base.set("filter_unit", filters.filterUnit);
  }
  if (!skipCategory && filters.filterCategory) {
    base.set("filter_category", filters.filterCategory);
  }
  if (!skipSourceKind && filters.filterSourceKind) {
    base.set("filter_source_kind", filters.filterSourceKind);
  }
  if (filters.search) base.set("search", filters.search);
};

export async function getBioactivityCategoryOptions(
  commonName: string,
  filters: BioactivitySidebarFilters = {},
): Promise<{ category: string; count: number }[]> {
  try {
    const params = new URLSearchParams({ common_name: commonName });
    // Categories excludes its own dimension (category) — apply all others.
    buildBioactivitySidebarParams(params, filters, { skipCategory: true });
    const res = await fetch(
      `${apiBase()}/bioactivity/categories?${params.toString()}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return [];
    const payload = await res.json();
    return (payload?.data ?? []) as { category: string; count: number }[];
  } catch {
    return [];
  }
}

// Per-source-kind row counts for the sidebar Assay Source filter.
// Backend classifies rows by evidence_source prefix (exp*/pred*/comp*),
// same as `_apply_source_kind_filter` on the paginated queries.
export type BioactivitySourceKindCounts = {
  both: number;
  experimental: number;
  predicted: number;
};

export async function getBioactivitySourceKindCounts(
  commonName: string,
  direction: string,
  filters: BioactivitySidebarFilters = {},
): Promise<BioactivitySourceKindCounts | null> {
  try {
    const params = new URLSearchParams({
      common_name: commonName,
      direction,
    });
    // Source kinds excludes its own dimension — apply all others.
    buildBioactivitySidebarParams(params, filters, { skipSourceKind: true });
    const res = await fetch(
      `${apiBase()}/bioactivity/source_kinds?${params.toString()}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return null;
    const payload = await res.json();
    const d = payload?.data;
    if (!d) return null;
    return {
      both: Number(d.both ?? 0),
      experimental: Number(d.experimental ?? 0),
      predicted: Number(d.predicted ?? 0),
    };
  } catch {
    return null;
  }
}

// Per-evidence_type row counts for the sidebar Evidence filter. The
// backend counts rows that carry at least one measurement of each
// evidence type (NPASS-style: molecular-level / in vitro / in vivo /
// adme-tox), same semantics as `_apply_evidence_type_filter` on the
// paginated queries.
export async function getBioactivityEvidenceTypeCounts(
  commonName: string,
  direction: string
): Promise<{ evidence_type: string; count: number }[]> {
  try {
    const res = await fetch(
      `${apiBase()}/bioactivity/evidence_types?common_name=${encodeURIComponent(
        commonName
      )}&direction=${encodeURIComponent(direction)}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) return [];
    const payload = await res.json();
    return (payload?.data ?? []) as { evidence_type: string; count: number }[];
  } catch {
    return [];
  }
}

// Chemical↔disease associations inferred from shared bioactivity assays
// (NOT the CTD literature signal from /chemical/correlation). Returns
// { data: AssayInferredAssociation[], metadata: { row_count } } ordered
// by n_assays desc.
const assayInferredFetch = async (path: string, commonName: string) => {
  try {
    const res = await fetch(
      `${apiBase()}${path}?common_name=${encodeURIComponent(commonName)}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) {
      console.warn(
        `Failed to fetch ${path} for ${commonName}: HTTP ${res.status}`
      );
      return null;
    }
    return await res.json();
  } catch (err) {
    console.warn(`Failed to fetch ${path} for ${commonName}:`, err);
    return null;
  }
};

export const getChemicalDiseaseAssociations = (commonName: string) =>
  assayInferredFetch("/chemical/disease-associations", commonName);

export const getDiseaseChemicalAssociations = (commonName: string) =>
  assayInferredFetch("/disease/chemical-associations", commonName);

// Per-food efficacy rows (chemical × bioactivity) — see
// inferred-bioactivity-efficacy-column.md for semantics. The inferred-
// bioactivities table joins these onto its rows client-side; not every
// food has rows (e.g. onion → 181, garlic → 0), so absence is not an
// error. Returns { data: FoodEfficacyRow[], metadata: { row_count } }.
export async function getFoodEfficacy(commonName: string) {
  try {
    const res = await fetch(
      `${apiBase()}/food/efficacy?common_name=${encodeURIComponent(commonName)}`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
        },
        next: { revalidate: 86400 },
      }
    );
    if (!res.ok) {
      console.warn(
        `Failed to fetch efficacy for ${commonName}: HTTP ${res.status}`
      );
      return null;
    }
    return await res.json();
  } catch (err) {
    console.warn(`Failed to fetch efficacy for ${commonName}:`, err);
    return null;
  }
}

// cache & fetching testing function
export async function getTime() {
  const response = await fetch("https://worldtimeapi.org/api/timezone/Etc/UTC");

  const data = await response.json();

  return data.unixtime;
}

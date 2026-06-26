import { MacroAndMicroData, Metadata, TaxonomyData } from "@/types";

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

// fetch food composition counts (classification + source counts in one call)
export async function getFoodCompositionCounts(commonName: string) {
  const response = await fetch(
    `${apiBase()}/food/composition/counts?common_name=${encodeURIComponent(commonName)}`,
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
};

const buildBioactivityQuery = (params?: BioactivityListParams): string => {
  const p = new URLSearchParams();
  if (params?.page) p.set("page", String(params.page));
  if (params?.search) p.set("search", params.search);
  if (params?.sortBy) p.set("sort_by", params.sortBy);
  if (params?.sortDir) p.set("sort_dir", params.sortDir);
  if (params?.filterEndpoint) p.set("filter_endpoint", params.filterEndpoint);
  if (params?.filterUnit) p.set("filter_unit", params.filterUnit);
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
  // the staging API frequently 502s on these endpoints until the
  // pagination patch deploys, and we'd rather show an empty state than
  // "An error occurred fetching data". See feedback-graceful-api-failures.
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
export type BioactivityDirection =
  | "bioactivity-chemicals"
  | "bioactivity-foods"
  | "chemical-bioactivities"
  | "food-bioactivities";

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

// cache & fetching testing function
export async function getTime() {
  const response = await fetch("https://worldtimeapi.org/api/timezone/Etc/UTC");

  const data = await response.json();

  return data.unixtime;
}

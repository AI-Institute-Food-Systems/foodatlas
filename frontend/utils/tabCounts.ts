// Server-side tab badge counts.
//
// A badge needs one number, not a tab's rows — so `page.tsx` fetches these
// on the server and hands them to buildTabs, and the tab itself still
// mounts only when opened. Without them an unvisited tab never calls
// usePublishTabCount and its placeholder pulses for the life of the page.
//
// Every count here must match what the tab publishes once opened, or the
// badge changes number under the user the moment they click it. The
// pairings are noted per function.
//
// Best-effort throughout: a count that fails resolves to null, which
// renders as no badge rather than taking the page down with it.

import {
  getBioactivityDiseases,
  getChemicalDiseaseAssociations,
  getDiseaseBioactivities,
  getDiseaseChemicalAssociations,
  getDiseaseData,
} from "@/utils/fetching";

const rowCount = async (
  fetcher: () => Promise<{ metadata?: { row_count?: number } } | null>
): Promise<number | null> => {
  const payload = await fetcher().catch(() => null);
  return payload?.metadata?.row_count ?? null;
};

// Health Impacts is two separately paginated tables. Both sections sum
// positive + negative into one badge (see ChemicalCorrelationSection),
// so the prefetch has to sum them too. Page 1 of each: we read
// metadata.total_rows and discard the rows.
export const healthImpactsCount = async (
  commonName: string,
  tableLocation: "chemical" | "disease"
): Promise<number | null> => {
  const total = async (relation: "positive" | "negative") => {
    const data = await getDiseaseData(commonName, 1, tableLocation, relation)
      .catch(() => null);
    const n = data?.metadata?.total_rows;
    return typeof n === "number" ? n : null;
  };
  const [pos, neg] = await Promise.all([total("positive"), total("negative")]);
  if (pos === null && neg === null) return null;
  return (pos ?? 0) + (neg ?? 0);
};

// Matches AssayInferredAssociationsTable, which publishes rows.length.
export const chemicalAssayInferredCount = (commonName: string) =>
  rowCount(() => getChemicalDiseaseAssociations(commonName));

export const diseaseAssayInferredCount = (commonName: string) =>
  rowCount(() => getDiseaseChemicalAssociations(commonName));

// NOT the chemical rows. DiseaseBioactivitiesSection publishes
// summary.length — the number of distinct bioactivities — so this reads
// /disease/bioactivities, the summary endpoint, not bioactivity-chemicals.
export const diseaseBioactivitiesCount = (commonName: string) =>
  rowCount(() => getDiseaseBioactivities(commonName));

// Matches BioactivityDiseasesSection, which publishes rows.length.
export const bioactivityDiseasesCount = (commonName: string) =>
  rowCount(() => getBioactivityDiseases(commonName));

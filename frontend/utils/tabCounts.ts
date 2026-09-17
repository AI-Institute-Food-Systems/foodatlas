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
  getDiseaseChemicalAssociations,
  getDiseaseData,
} from "@/utils/fetching";

const rowCount = async (
  fetcher: () => Promise<{ metadata?: { row_count?: number } } | null>
): Promise<number | null> => {
  const payload = await fetcher().catch(() => null);
  return payload?.metadata?.row_count ?? null;
};

// The merged Diseases/Chemicals tab stacks two tables — CTD literature
// and assay-inferred — and shows ONE badge summing both, so the prefetch
// has to sum both too. Anything less and the badge lands wrong, then
// corrects itself when the tab is opened and the tables publish.
//
// `relation=all` is a single request covering both directions; we read
// metadata.total_rows and discard the rows.
export const correlationEvidenceCount = async (
  commonName: string,
  tableLocation: "chemical" | "disease"
): Promise<number | null> => {
  const literature = async () => {
    const data = await getDiseaseData(commonName, 1, tableLocation, "all");
    const n = data?.metadata?.total_rows;
    return typeof n === "number" ? n : null;
  };
  const inferred =
    tableLocation === "chemical"
      ? () => rowCount(() => getChemicalDiseaseAssociations(commonName))
      : () => rowCount(() => getDiseaseChemicalAssociations(commonName));

  const [lit, inf] = await Promise.all([literature(), inferred()]);
  if (lit === null && inf === null) return null;
  return (lit ?? 0) + (inf ?? 0);
};

// Matches BioactivityDiseasesSection, which publishes rows.length.
export const bioactivityDiseasesCount = (commonName: string) =>
  rowCount(() => getBioactivityDiseases(commonName));

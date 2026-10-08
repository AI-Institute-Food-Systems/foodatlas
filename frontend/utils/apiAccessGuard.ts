import {
  API_TERMS_SUMMARY,
  COMMERCIAL,
  Commercial,
  DATA_NEEDED,
  DataNeeded,
  USE_CATEGORIES,
  UseCategory,
  VOLUMES,
  Volume,
} from "@/utils/apiAccessFields";
import { LIMITS, str } from "@/utils/formGuard";

export interface ValidApiAccess {
  useCategory: UseCategory;
  commercial: Commercial;
  dataNeeded: DataNeeded[];
  volume: Volume;
  projectUrl: string;
  termsAccepted: true;
}

const oneOf = <T extends string>(
  options: readonly T[],
  value: unknown,
): T | null =>
  options.includes(value as T) ? (value as T) : null;

// http(s) only: the URL lands in an email the team clicks from, so
// javascript:, data: and friends are refused rather than escaped.
const httpUrl = (value: string): boolean => {
  try {
    const { protocol } = new URL(value);
    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
};

/** Validate the `apiAccess` object of a contact POST, or return null. */
export const parseApiAccess = (raw: unknown): ValidApiAccess | null => {
  if (typeof raw !== "object" || raw === null) return null;
  const {
    useCategory,
    commercial,
    dataNeeded,
    volume,
    projectUrl,
    termsAccepted,
  } = raw as Record<string, unknown>;
  if (termsAccepted !== true) return null;

  const useCategoryV = oneOf(USE_CATEGORIES, useCategory);
  const commercialV = oneOf(COMMERCIAL, commercial);
  const volumeV = oneOf(VOLUMES, volume);
  if (!useCategoryV || !commercialV || !volumeV) return null;

  if (!Array.isArray(dataNeeded) || dataNeeded.length === 0) return null;
  if (!dataNeeded.every((d) => oneOf(DATA_NEEDED, d))) return null;
  // Canonical order and no duplicates, whatever order the boxes were ticked.
  const dataNeededV = DATA_NEEDED.filter((d) => dataNeeded.includes(d));

  const projectUrlV = str(projectUrl, LIMITS.projectUrl, { required: false });
  if (projectUrlV === null) return null;
  if (projectUrlV && !httpUrl(projectUrlV)) return null;

  return {
    useCategory: useCategoryV,
    commercial: commercialV,
    dataNeeded: dataNeededV,
    volume: volumeV,
    projectUrl: projectUrlV,
    termsAccepted: true,
  };
};

/**
 * Email section for an API request. Fixed keys and order so a sheet intake
 * can parse it line by line — change the labels and that parser breaks.
 */
export const apiAccessBlock = (v: ValidApiAccess): string =>
  "\n\n--- API access ---\n" +
  `Use category: ${v.useCategory}\n` +
  `Commercial use: ${v.commercial}\n` +
  `Data needed: ${v.dataNeeded.join(", ")}\n` +
  `Expected volume: ${v.volume}\n` +
  `Project URL: ${v.projectUrl || "—"}\n` +
  `Terms accepted: ${API_TERMS_SUMMARY}\n`;

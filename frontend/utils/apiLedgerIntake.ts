import type { ValidApiAccess } from "@/utils/apiAccessGuard";

export interface ApiRequestIntake {
  name: string;
  email: string;
  affiliation: string;
  message: string;
  apiAccess: ValidApiAccess;
}

const TIMEOUT_MS = 8000;

/**
 * Adds an API request to the team's request ledger. Never throws: the email has
 * already gone out and stays the record of the request if this fails. Unset env
 * means no ledger, so preview deployments cannot add test rows to it.
 */
export const addToApiLedger = async (
  request: ApiRequestIntake,
): Promise<void> => {
  const url = process.env.API_LEDGER_URL;
  const token = process.env.API_LEDGER_INTAKE_TOKEN;
  if (!url || !token) return;

  const { apiAccess, ...requester } = request;
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: JSON.stringify({
        action: "intake",
        tab: "FoodAtlas",
        token,
        ...requester,
        useCategory: apiAccess.useCategory,
        commercial: apiAccess.commercial,
        dataNeeded: apiAccess.dataNeeded,
        volume: apiAccess.volume,
        projectUrl: apiAccess.projectUrl,
      }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    const result = (await response.json()) as { ok?: boolean; error?: string };
    if (!result.ok) console.error("API ledger intake refused:", result.error);
  } catch (error) {
    console.error("API ledger intake failed:", error);
  }
};

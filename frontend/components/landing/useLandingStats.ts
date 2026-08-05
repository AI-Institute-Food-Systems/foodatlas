"use client";

import { useEffect, useState } from "react";

import { apiBase } from "@/utils/fetching";

export interface LandingStats {
  foods: number;
  chemicals: number;
  diseases: number;
  bioactivities: number;
  associations: number;
  publications: number;
}

const EMPTY: LandingStats = {
  foods: 0,
  chemicals: 0,
  diseases: 0,
  bioactivities: 0,
  associations: 0,
  publications: 0,
};

// Shared client-side stats fetch for the landing variants. Returns
// null while loading so the UI can render skeleton placeholders instead
// of a flash of zeros, and falls back to EMPTY on error so the page
// still renders cleanly if the API is down (per the graceful-failure
// rule in memory).
export const useLandingStats = (): LandingStats | null => {
  const [stats, setStats] = useState<LandingStats | null>(null);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${apiBase()}/metadata/statistics`, {
          headers: {
            Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
          },
        });
        if (!res.ok) {
          if (!cancelled) setStats(EMPTY);
          return;
        }
        const json = await res.json();
        const s = json?.data?.statistics ?? {};
        if (cancelled) return;
        setStats({
          foods: s.foods ?? 0,
          chemicals: s.chemicals ?? 0,
          diseases: s.diseases ?? 0,
          bioactivities: s.bioactivities ?? 0,
          associations: s.connections ?? 0,
          publications: s.publications ?? 0,
        });
      } catch {
        if (!cancelled) setStats(EMPTY);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);
  return stats;
};

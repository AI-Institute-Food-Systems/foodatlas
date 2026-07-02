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
// zeros on any error so the page still renders cleanly (per the
// graceful-failure rule in memory).
export const useLandingStats = (): LandingStats => {
  const [stats, setStats] = useState<LandingStats>(EMPTY);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${apiBase()}/metadata/statistics`, {
          headers: {
            Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
          },
        });
        if (!res.ok) return;
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
        // Keep zeros.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);
  return stats;
};

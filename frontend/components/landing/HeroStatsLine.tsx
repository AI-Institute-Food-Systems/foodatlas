"use client";

// Compact mono-italic stats line that lives under the hero search.
// Replaces the previous standalone `NumbersSection` (stat cards) —
// the cards were redundant signal once the same numbers are in the
// hero. Falls back to zeros on fetch failure (per the graceful API
// rule in memory).

import { useLandingStats } from "@/components/landing/useLandingStats";

const fmt = (n: number): string => n.toLocaleString();
const fmtCompact = (n: number): string => {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return n.toLocaleString();
};

// Order matters: associations leads (it's the headline metric — every
// link in the graph counted at the row level). Within the entity-type
// block, bioactivities goes first per the same ordering rule used in
// the search placeholder + the `<TryChips>` row.
const ITEMS: {
  key: keyof Awaited<ReturnType<typeof useLandingStats>>;
  label: string;
  compact?: boolean;
}[] = [
  { key: "associations", label: "Associations", compact: true },
  { key: "bioactivities", label: "Bioactivities" },
  { key: "foods", label: "Foods" },
  { key: "chemicals", label: "Chemicals" },
  { key: "diseases", label: "Diseases" },
  { key: "publications", label: "Publications" },
];

const HeroStatsLine = () => {
  const stats = useLandingStats();
  return (
    <div className="flex justify-center">
      <div
        className={
          // Glass bar — black/40 + backdrop blur to lift the row off the
          // busy hero image; soft white outline matches the chip row
          // above. `flex-nowrap` keeps all six stacked pairs on a single
          // row; `overflow-x-auto` on very narrow viewports lets the bar
          // scroll horizontally instead of breaking the layout.
          "inline-flex flex-nowrap items-end justify-center gap-x-8 md:gap-x-12 " +
          "px-5 md:px-8 py-3 rounded-2xl border border-white/10 " +
          "bg-black/40 backdrop-blur-md max-w-full overflow-x-auto"
        }
      >
        {ITEMS.map((item) => {
          const value = stats[item.key];
          return (
            <div
              key={item.key}
              className="flex flex-col items-center leading-none whitespace-nowrap"
            >
              <span className="text-white text-base md:text-lg font-semibold tabular-nums">
                {item.compact ? fmtCompact(value) : fmt(value)}
              </span>
              <span className="mt-1.5 font-mono italic text-[10px] md:text-xs uppercase tracking-wider text-light-300">
                {item.label}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
};

HeroStatsLine.displayName = "HeroStatsLine";
export default HeroStatsLine;

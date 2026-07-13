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
  key: keyof NonNullable<ReturnType<typeof useLandingStats>>;
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
  const isLoading = stats === null;
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
          const value = stats?.[item.key];
          return (
            <div
              key={item.key}
              // min-width reserves enough space for the loaded number,
              // so the column doesn't grow/shrink between the skeleton
              // and the value — otherwise the parent inline-flex row
              // re-centers and every column jitters horizontally.
              className="flex flex-col items-center leading-none whitespace-nowrap min-w-[4rem] md:min-w-[4.5rem]"
            >
              <span className="text-white text-base md:text-lg font-semibold tabular-nums">
                {isLoading ? (
                  // Skeleton sits in the same styled outer span so the
                  // line box has identical font-metrics in both states.
                  // h-[1em] = font-size (16px base, 18px md) matching
                  // the text's exact rendered height, so the column
                  // doesn't jitter when values land.
                  <span
                    aria-hidden
                    className="inline-block h-[1em] w-10 md:w-12 rounded bg-light-800/60 animate-pulse align-middle"
                  />
                ) : (
                  <>
                    {item.compact ? fmtCompact(value ?? 0) : fmt(value ?? 0)}
                  </>
                )}
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

import { twMerge } from "tailwind-merge";

interface ConcentrationBarProps {
  // Already-clamped fill percentage from `barPercent`, or null when the
  // row has no measured amount (renders an empty track).
  percent: number | null;
  className?: string;
}

// The inline bar that replaced the chemical page's standalone bar chart.
//
// aria-hidden on purpose: the bar carries no information the numeric
// readout beside it doesn't already state exactly, and the codebase has no
// role="meter"/"progressbar" vocabulary to join. Announcing it would make a
// screen reader read every row twice.
//
// Geometry note: the track flexes to fill the column rather than taking a
// fixed width. A fixed 128px track is what forced the earlier inline bar
// out of the food composition table (893521b) — at narrow widths it stopped
// being comparable between rows while still costing the column its space.
const ConcentrationBar = ({ percent, className }: ConcentrationBarProps) => (
  <span
    aria-hidden
    className={twMerge(
      "relative inline-block h-1.5 flex-1 min-w-0 shrink rounded-full",
      "bg-light-800/70 overflow-hidden",
      className
    )}
    data-testid="concentration-bar"
    data-percent={percent === null ? "" : String(percent)}
  >
    {percent !== null && (
      <span
        className="absolute inset-y-0 left-0 rounded-full bg-accent-600/80"
        style={{ width: `${percent}%` }}
      />
    )}
  </span>
);

ConcentrationBar.displayName = "ConcentrationBar";

export default ConcentrationBar;

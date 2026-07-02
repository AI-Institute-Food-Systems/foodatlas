"use client";

import { twMerge } from "tailwind-merge";

// One primitive for every "small labelled pill" in the apothecary
// vocabulary: tab labels, badges (ambiguity, trust, source), inline
// counters, evidence-count buttons. Consumers pick a tone + size and
// pass label / count / icon; everything else is centralised here so
// the pill visual system stays consistent across the app.
//
// Tone conventions:
// - cream    → active / selected (matches selected tab, filter chip)
// - outline  → neutral / clickable slab (default action pill)
// - amber    → warning (ambiguity)
// - rose     → danger (low-trust)
//
// Size conventions:
// - sm  → inline metadata badge (row-level ambiguity/trust)
// - md  → chip toggle, section-level pill, table-cell action
// - lg  → tab label

export type ChipTone = "cream" | "outline" | "amber" | "rose";
export type ChipSize = "xs" | "sm" | "md" | "lg";

interface ChipProps {
  label: React.ReactNode;
  count?: number;
  icon?: React.ReactNode;
  tone?: ChipTone;
  size?: ChipSize;
  onClick?: (e: React.MouseEvent<HTMLButtonElement>) => void;
  title?: string;
  className?: string;
  disabled?: boolean;
  "aria-pressed"?: boolean;
  "aria-label"?: string;
}

const TONE_CLASSES: Record<ChipTone, string> = {
  cream:
    "bg-light-200 text-light-900 border-light-200 hover:bg-light-100 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)]",
  outline:
    "bg-transparent border-light-600/70 text-light-300 hover:text-light-100 hover:border-light-500 hover:bg-light-900/40",
  amber:
    "bg-amber-500/10 text-amber-400 border-amber-500/60 hover:bg-amber-500/20",
  rose: "bg-rose-500/10 text-rose-400 border-rose-500/60 hover:bg-rose-500/20",
};

const SIZE_CLASSES: Record<ChipSize, string> = {
  xs: "px-1 py-[0.05rem] text-[9px] gap-0.5 border",
  sm: "px-1.5 py-[0.1rem] text-[0.6rem] gap-1 border",
  md: "px-2.5 py-0.5 text-xs gap-1.5 border",
  lg: "px-3 py-1 text-sm gap-2 border-[1.5px]",
};

const COUNT_TONE: Record<ChipTone, string> = {
  cream: "text-light-700",
  outline: "text-light-500",
  amber: "text-amber-300/80",
  rose: "text-rose-300/80",
};

const Chip = ({
  label,
  count,
  icon,
  tone = "outline",
  size = "md",
  onClick,
  title,
  className,
  disabled,
  ...aria
}: ChipProps) => {
  const clickable = !!onClick;
  const commonClass = twMerge(
    "inline-flex items-center rounded-full font-mono italic font-medium whitespace-nowrap transition-colors",
    TONE_CLASSES[tone],
    SIZE_CLASSES[size],
    disabled && "opacity-40 cursor-not-allowed",
    className
  );
  const body = (
    <>
      {icon}
      <span>{label}</span>
      {typeof count === "number" && (
        <span
          className={twMerge(
            "not-italic tabular-nums",
            COUNT_TONE[tone]
          )}
        >
          {count.toLocaleString()}
        </span>
      )}
    </>
  );
  if (clickable) {
    return (
      <button
        type="button"
        onClick={onClick}
        title={title}
        disabled={disabled}
        className={commonClass}
        {...aria}
      >
        {body}
      </button>
    );
  }
  return (
    <span title={title} className={commonClass} {...aria}>
      {body}
    </span>
  );
};

Chip.displayName = "Chip";

export default Chip;

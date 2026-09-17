"use client";

// THE hover info box. Every explanatory hover in the app — the "i" beside
// a column header, a source badge, a warning glyph, a chip — renders
// through here, so they all look and behave the same.
//
// The bubble is portalled to <body> and positioned from the trigger's own
// getBoundingClientRect(), fixed to the viewport. That is the whole
// design: the previous version was `position: absolute` inside the
// trigger, and when it flipped to avoid the top of the screen it set
// `bottom: 0` — which resolves against the nearest POSITIONED ANCESTOR,
// not the trigger. Inside a table that ancestor was the card or the page,
// so the bubble for the Efficacy column's "i" landed at the bottom of the
// screen. An `overflow-x-auto` table wrapper also clipped it. Neither can
// happen to a fixed element in <body>.
//
// Native `title=` is NOT this. It is fine for revealing clipped text
// (FilterOption's label, an assay name), where the browser's own tooltip
// is the expected affordance; an explanation goes through Tooltip.

import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";
import { MdInfoOutline } from "react-icons/md";
import { twMerge } from "tailwind-merge";

type Placement = "top" | "bottom";

interface TooltipProps {
  content: ReactNode;
  children: ReactNode;
  // Preferred side. Flips when that side has no room and the other does,
  // so a trigger at the very top of the page opens downward and one at
  // the bottom opens upward whatever the caller asked for.
  placement?: Placement;
}

// Trigger ↔ bubble, and bubble ↔ viewport edge.
const GAP = 8;
const MARGIN = 8;
const ARROW = 8;
const DELAY_MS = 300;

interface Position {
  top: number;
  left: number;
  // The arrow tracks the trigger even when the bubble is shoved sideways
  // to stay inside the viewport.
  arrowLeft: number;
  side: Placement;
}

export const Tooltip = ({
  content,
  children,
  placement = "top",
}: TooltipProps) => {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<Position | null>(null);
  const triggerRef = useRef<HTMLSpanElement>(null);
  const bubbleRef = useRef<HTMLDivElement>(null);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const id = useId();

  const place = useCallback(() => {
    const t = triggerRef.current?.getBoundingClientRect();
    const b = bubbleRef.current?.getBoundingClientRect();
    if (!t || !b) return;
    const roomAbove = t.top - GAP - MARGIN;
    const roomBelow = window.innerHeight - t.bottom - GAP - MARGIN;
    let side = placement;
    if (side === "top" && b.height > roomAbove && roomBelow > roomAbove) {
      side = "bottom";
    } else if (
      side === "bottom" &&
      b.height > roomBelow &&
      roomAbove > roomBelow
    ) {
      side = "top";
    }
    const top = side === "top" ? t.top - GAP - b.height : t.bottom + GAP;
    const centre = t.left + t.width / 2;
    const left = Math.min(
      Math.max(centre - b.width / 2, MARGIN),
      Math.max(MARGIN, window.innerWidth - MARGIN - b.width)
    );
    setPos({ top, left, arrowLeft: centre - left, side });
  }, [placement]);

  // Measure after the bubble exists in the DOM, before paint: the first
  // render puts it at 0,0 hidden, this moves it, and the user only ever
  // sees the second.
  useLayoutEffect(() => {
    if (open) place();
  }, [open, place]);

  useEffect(() => {
    if (!open) return;
    // Capture phase so a scrolling table wrapper counts, not just window.
    window.addEventListener("scroll", place, true);
    window.addEventListener("resize", place);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("scroll", place, true);
      window.removeEventListener("resize", place);
      window.removeEventListener("keydown", onKey);
    };
  }, [open, place]);

  useEffect(
    () => () => {
      if (timer.current) clearTimeout(timer.current);
    },
    []
  );

  const show = () => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setOpen(true), DELAY_MS);
  };
  const hide = () => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = null;
    setOpen(false);
    setPos(null);
  };

  return (
    <span
      ref={triggerRef}
      onMouseEnter={show}
      onMouseLeave={hide}
      onFocus={show}
      onBlur={hide}
      aria-describedby={open ? id : undefined}
      className="inline-flex items-center cursor-pointer"
    >
      {children}
      {open &&
        typeof document !== "undefined" &&
        createPortal(
          <div
            ref={bubbleRef}
            id={id}
            role="tooltip"
            data-side={pos?.side ?? placement}
            // z-[110] clears everything that can sit under a hover
            // bubble: the navbar (z-40, z-[60] with the menu open), Modal
            // (z-50), the FAB and mobile filter panel (z-[60]) and the
            // navigation progress bar (z-[100]). A tooltip is transient
            // and pointer-driven, so nothing should ever cover it.
            className="pointer-events-none fixed z-[110]"
            style={{
              top: pos?.top ?? 0,
              left: pos?.left ?? 0,
              visibility: pos ? "visible" : "hidden",
            }}
          >
            {/* Wraps at a readable width rather than running to the edge
              * of the screen: the old bubble was nowrap, so a sentence-long
              * explanation was a 900px bar shoved sideways to fit. Short
              * content is unaffected.
              *
              * The bubble owns its width; content cannot be wider than it.
              * `[&_*]:max-w-full` caps every descendant at the bubble's
              * inner width, so a content block that sets its own `w-[28rem]`
              * (the Efficacy help did) wraps inside the background instead
              * of running past it; `break-words` does the same for a token
              * with no break in it (a URL, a long chemical name). */}
            <div className="relative max-w-[min(22rem,calc(100vw-1rem))] rounded-md bg-light-800 p-2.5 text-[14px] text-left leading-relaxed tracking-wide text-light-300 shadow-sm [font-weight:400] break-words [&_*]:max-w-full">
              {content}
              {/* The pointer: a rotated square on whichever edge faces the
                * trigger, centred under it. */}
              <span
                aria-hidden
                className={twMerge(
                  "absolute size-2 rotate-45 bg-light-800",
                  (pos?.side ?? placement) === "top"
                    ? "-bottom-1"
                    : "-top-1"
                )}
                style={{ left: (pos?.arrowLeft ?? 0) - ARROW / 2 }}
              />
            </div>
          </div>,
          document.body
        )}
    </span>
  );
};

// The "i" that opens a Tooltip. One glyph, one size, one colour, so an
// explanation looks the same beside a column header, a toggle, or a
// value — and a reader learns once what it means.
export const InfoTip = ({
  content,
  label,
  className,
}: {
  content: ReactNode;
  // What the glyph explains, for screen readers: "About the Efficacy column".
  label: string;
  className?: string;
}) => (
  <Tooltip content={content}>
    <MdInfoOutline
      role="img"
      aria-label={label}
      className={twMerge(
        "w-3.5 h-3.5 text-light-500 hover:text-light-100 transition-colors",
        className
      )}
    />
  </Tooltip>
);

export default Tooltip;

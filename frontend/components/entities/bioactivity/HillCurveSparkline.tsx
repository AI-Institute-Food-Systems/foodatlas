// 4-parameter Hill / log-logistic dose-response curve, drawn as a small
// SVG plot with permanent axis labels and a hover crosshair that
// reveals the modelled activity at any concentration. Renders null
// unless ALL four fit parameters are present (most ChEMBL/PubChem rows
// only carry a logAC50 + raw value — only ToxCast-style measurements
// have zero/infinite/slope too).
//
// Hill equation (concentration-form):
//   y(x) = bottom + (top − bottom) / (1 + 10^((logAC50 − log10(x)) * slope))
//
// Implementation notes:
// - Readout text lives INSIDE the SVG (no floating tooltip / portal)
//   so it works inside dense table cells without overflow/z-index hell.
// - Permanent labels at the four corners: top/bottom of the y-axis on
//   the left, log-concentration range on the bottom. Tabular-nums via
//   the SVG `text` element's `font-family`.

"use client";

import { useRef, useState } from "react";

interface Props {
  zero: number | null | undefined;
  infinite: number | null | undefined;
  logAC50: number | null | undefined;
  slope: number | null | undefined;
  // Concentration unit (e.g. "uM") — appended to readout values when
  // present. Optional because most call sites have it adjacent already.
  unit?: string | null;
  width?: number;
  height?: number;
  // When true, the SVG fills its container (width/height = 100%) while
  // keeping the supplied width/height as the internal viewBox. Used
  // for the accordion's expanded view so the curve takes whatever
  // horizontal space the layout offers.
  fluid?: boolean;
}

const SAMPLES = 80;
// Sweep 3 decades on each side of AC50 — captures the plateau-rise-plateau
// shape for any sane slope (-3..+3 dose-effect range covers > 99% of fits).
const DECADES = 3;
// Reserved gutters for axis labels. Tuned for an 8-px serif/mono font.
const PAD_LEFT = 22;
const PAD_BOTTOM = 10;
const PAD_TOP = 9;

const fmtConc = (logX: number): string => {
  const v = 10 ** logX;
  if (v >= 100) return v.toFixed(0);
  if (v >= 1) return v.toFixed(1);
  if (v >= 0.01) return v.toFixed(2);
  return v.toExponential(0);
};

const fmtY = (y: number): string => {
  if (Math.abs(y) >= 10) return y.toFixed(0);
  return y.toFixed(1);
};

const HillCurveSparkline = ({
  zero,
  infinite,
  logAC50,
  slope,
  unit,
  width = 160,
  height = 64,
  fluid = false,
}: Props) => {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [hoverPx, setHoverPx] = useState<number | null>(null);

  if (
    zero == null ||
    infinite == null ||
    logAC50 == null ||
    slope == null ||
    !Number.isFinite(zero) ||
    !Number.isFinite(infinite) ||
    !Number.isFinite(logAC50) ||
    !Number.isFinite(slope) ||
    slope === 0 ||
    zero === infinite
  ) {
    return null;
  }

  const plotW = width - PAD_LEFT;
  const plotH = height - PAD_TOP - PAD_BOTTOM;
  const logXMin = logAC50 - DECADES;
  const logXMax = logAC50 + DECADES;
  const yMin = Math.min(zero, infinite);
  const yMax = Math.max(zero, infinite);
  const yRange = yMax - yMin;

  const hillY = (logX: number): number =>
    zero + (infinite - zero) / (1 + 10 ** ((logAC50 - logX) * slope));

  const toPx = (logX: number): number =>
    PAD_LEFT + ((logX - logXMin) / (logXMax - logXMin)) * plotW;
  const toPy = (y: number): number =>
    PAD_TOP + ((yMax - y) / yRange) * plotH;

  const points: string[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const t = i / (SAMPLES - 1);
    const logX = logXMin + t * (logXMax - logXMin);
    const y = hillY(logX);
    points.push(`${toPx(logX).toFixed(1)},${toPy(y).toFixed(1)}`);
  }

  const midPx = toPx(logAC50);
  const midPy = toPy(hillY(logAC50));

  // Clamp hover to the plot area so the crosshair never escapes the
  // axis labels.
  let hoverLogX: number | null = null;
  let hoverY: number | null = null;
  if (hoverPx != null) {
    const clamped = Math.max(PAD_LEFT, Math.min(PAD_LEFT + plotW, hoverPx));
    const t = (clamped - PAD_LEFT) / plotW;
    hoverLogX = logXMin + t * (logXMax - logXMin);
    hoverY = hillY(hoverLogX);
  }

  const handleMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    // Account for SVG viewBox vs displayed size scaling.
    const scale = width / rect.width;
    setHoverPx((e.clientX - rect.left) * scale);
  };

  const readout =
    hoverLogX != null && hoverY != null
      ? `${fmtConc(hoverLogX)}${unit ? ` ${unit}` : ""} → ${fmtY(hoverY)}`
      : `AC50 ${fmtConc(logAC50)}${unit ? ` ${unit}` : ""}`;

  const labelFont = "ui-monospace, SFMono-Regular, Menlo, monospace";

  return (
    <svg
      ref={svgRef}
      width={fluid ? "100%" : width}
      height={fluid ? "100%" : height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`Hill curve fit, AC50 at 10^${logAC50.toFixed(2)}, slope ${slope.toFixed(2)}`}
      className="block cursor-crosshair"
      onMouseMove={handleMove}
      onMouseLeave={() => setHoverPx(null)}
    >
      {/* readout / AC50 label across the top — switches to live value on hover */}
      <text
        x={width}
        y={PAD_TOP - 2}
        fontSize="8"
        fontFamily={labelFont}
        fill="currentColor"
        fillOpacity={hoverLogX != null ? "0.9" : "0.6"}
        textAnchor="end"
      >
        {readout}
      </text>

      {/* y-axis labels (top + bottom of value range) on the left */}
      <text
        x={PAD_LEFT - 3}
        y={PAD_TOP + 3}
        fontSize="7"
        fontFamily={labelFont}
        fill="currentColor"
        fillOpacity="0.5"
        textAnchor="end"
      >
        {fmtY(yMax)}
      </text>
      <text
        x={PAD_LEFT - 3}
        y={PAD_TOP + plotH}
        fontSize="7"
        fontFamily={labelFont}
        fill="currentColor"
        fillOpacity="0.5"
        textAnchor="end"
      >
        {fmtY(yMin)}
      </text>

      {/* x-axis labels (log concentration range) at the bottom corners */}
      <text
        x={PAD_LEFT}
        y={height - 2}
        fontSize="7"
        fontFamily={labelFont}
        fill="currentColor"
        fillOpacity="0.5"
      >
        {fmtConc(logXMin)}
      </text>
      <text
        x={width}
        y={height - 2}
        fontSize="7"
        fontFamily={labelFont}
        fill="currentColor"
        fillOpacity="0.5"
        textAnchor="end"
      >
        {fmtConc(logXMax)}
      </text>

      {/* plot frame — left + bottom axes */}
      <line
        x1={PAD_LEFT}
        x2={width}
        y1={PAD_TOP + plotH}
        y2={PAD_TOP + plotH}
        stroke="currentColor"
        strokeOpacity="0.2"
      />
      <line
        x1={PAD_LEFT}
        x2={PAD_LEFT}
        y1={PAD_TOP}
        y2={PAD_TOP + plotH}
        stroke="currentColor"
        strokeOpacity="0.2"
      />

      {/* AC50 marker line (always visible — anchors the eye to the
       * inflection point even when the user isn't hovering) */}
      <line
        x1={midPx}
        x2={midPx}
        y1={PAD_TOP}
        y2={PAD_TOP + plotH}
        stroke="currentColor"
        strokeOpacity="0.2"
        strokeDasharray="2 2"
      />

      {/* curve */}
      <polyline
        points={points.join(" ")}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />

      {/* AC50 dot at midpoint */}
      <circle cx={midPx} cy={midPy} r="2" fill="currentColor" />

      {/* hover crosshair — vertical + horizontal lines through the
       * cursor's curve intersection, dot on the curve, and axis ticks
       * + value labels so the user can read the concentration / activity
       * straight off the axes without going to the top-right readout. */}
      {hoverLogX != null && hoverY != null && (
        <>
          <line
            x1={toPx(hoverLogX)}
            x2={toPx(hoverLogX)}
            y1={PAD_TOP}
            y2={PAD_TOP + plotH}
            stroke="currentColor"
            strokeOpacity="0.5"
          />
          <line
            x1={PAD_LEFT}
            x2={toPx(hoverLogX)}
            y1={toPy(hoverY)}
            y2={toPy(hoverY)}
            stroke="currentColor"
            strokeOpacity="0.5"
            strokeDasharray="2 2"
          />
          <circle
            cx={toPx(hoverLogX)}
            cy={toPy(hoverY)}
            r="2.5"
            fill="currentColor"
          />
          {/* X-axis tick + concentration label at the cursor */}
          <line
            x1={toPx(hoverLogX)}
            x2={toPx(hoverLogX)}
            y1={PAD_TOP + plotH}
            y2={PAD_TOP + plotH + 3}
            stroke="currentColor"
          />
          <text
            x={toPx(hoverLogX)}
            y={height - 1}
            fontSize="8"
            fontFamily={labelFont}
            fill="currentColor"
            textAnchor="middle"
          >
            {fmtConc(hoverLogX)}
            {unit ? ` ${unit}` : ""}
          </text>
          {/* Y-axis tick + activity label at the curve intersection */}
          <line
            x1={PAD_LEFT - 3}
            x2={PAD_LEFT}
            y1={toPy(hoverY)}
            y2={toPy(hoverY)}
            stroke="currentColor"
          />
          <text
            x={PAD_LEFT - 4}
            y={toPy(hoverY) + 3}
            fontSize="8"
            fontFamily={labelFont}
            fill="currentColor"
            textAnchor="end"
          >
            {fmtY(hoverY)}
          </text>
        </>
      )}
    </svg>
  );
};

HillCurveSparkline.displayName = "HillCurveSparkline";

// Tiny line-only glyph for the accordion indicator column — just the
// curve trace, no axes, labels, AC50 marker, or hover. Carries enough
// information (slope direction + steepness) to telegraph at a glance
// that the row has a fitted curve to expand. Defaults sized for a
// table cell (~50×18). Returns null when the fit is incomplete so the
// call site can render an em-dash instead.
export const HillCurveGlyph = ({
  zero,
  infinite,
  logAC50,
  slope,
  width = 50,
  height = 18,
}: {
  zero: number | null | undefined;
  infinite: number | null | undefined;
  logAC50: number | null | undefined;
  slope: number | null | undefined;
  width?: number;
  height?: number;
}) => {
  if (
    zero == null ||
    infinite == null ||
    logAC50 == null ||
    slope == null ||
    !Number.isFinite(zero) ||
    !Number.isFinite(infinite) ||
    !Number.isFinite(logAC50) ||
    !Number.isFinite(slope) ||
    slope === 0 ||
    zero === infinite
  ) {
    return null;
  }
  const logXMin = logAC50 - DECADES;
  const logXMax = logAC50 + DECADES;
  const yMin = Math.min(zero, infinite);
  const yMax = Math.max(zero, infinite);
  const yRange = yMax - yMin;
  // Leave 1px gutter so the line doesn't touch the edge.
  const points: string[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const t = i / (SAMPLES - 1);
    const logX = logXMin + t * (logXMax - logXMin);
    const y = zero + (infinite - zero) / (1 + 10 ** ((logAC50 - logX) * slope));
    const px = 1 + t * (width - 2);
    const py = 1 + ((yMax - y) / yRange) * (height - 2);
    points.push(`${px.toFixed(1)},${py.toFixed(1)}`);
  }
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      aria-hidden
      className="block"
    >
      <polyline
        points={points.join(" ")}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};

HillCurveGlyph.displayName = "HillCurveGlyph";

export default HillCurveSparkline;

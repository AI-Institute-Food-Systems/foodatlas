// 4-parameter Hill / log-logistic dose-response curve, drawn as a tiny
// SVG sparkline. Renders only when ALL four params are present; the
// component returns null otherwise (most ChEMBL/PubChem rows only carry
// a logAC50 + raw value, no full fit — only ToxCast-style measurements
// have zero/infinite/slope too).
//
// Hill equation (concentration-form):
//   y(x) = bottom + (top − bottom) / (1 + 10^((logAC50 − log10(x)) * slope))

interface Props {
  zero: number | null | undefined;
  infinite: number | null | undefined;
  logAC50: number | null | undefined;
  slope: number | null | undefined;
  width?: number;
  height?: number;
}

const SAMPLES = 80;
// Sweep 3 decades on each side of AC50 — captures the plateau-rise-plateau
// shape for any sane slope (-3..+3 dose-effect range covers > 99% of fits).
const DECADES = 3;

const HillCurveSparkline = ({
  zero,
  infinite,
  logAC50,
  slope,
  width = 120,
  height = 40,
}: Props) => {
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

  const points: string[] = [];
  for (let i = 0; i < SAMPLES; i++) {
    const t = i / (SAMPLES - 1);
    const logX = logXMin + t * (logXMax - logXMin);
    const y = zero + (infinite - zero) / (1 + 10 ** ((logAC50 - logX) * slope));
    const px = t * width;
    const py = height - ((y - yMin) / yRange) * height;
    points.push(`${px.toFixed(1)},${py.toFixed(1)}`);
  }

  // AC50 marker — vertical line + dot at the midpoint.
  const midX = ((logAC50 - logXMin) / (logXMax - logXMin)) * width;
  const midY = height / 2;

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={`Hill curve fit, AC50 at 10^${logAC50.toFixed(2)}, slope ${slope.toFixed(2)}`}
      className="block"
    >
      {/* baseline + asymptote ticks */}
      <line
        x1="0"
        x2={width}
        y1={height - 0.5}
        y2={height - 0.5}
        stroke="currentColor"
        strokeOpacity="0.15"
        strokeWidth="1"
      />
      <line
        x1="0"
        x2={width}
        y1="0.5"
        y2="0.5"
        stroke="currentColor"
        strokeOpacity="0.15"
        strokeWidth="1"
      />
      {/* AC50 marker line */}
      <line
        x1={midX}
        x2={midX}
        y1="0"
        y2={height}
        stroke="currentColor"
        strokeOpacity="0.2"
        strokeWidth="1"
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
      {/* AC50 dot */}
      <circle cx={midX} cy={midY} r="2" fill="currentColor" />
    </svg>
  );
};

HillCurveSparkline.displayName = "HillCurveSparkline";

export default HillCurveSparkline;

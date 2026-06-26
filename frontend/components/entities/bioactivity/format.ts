import {
  BioactivityMeasurement,
  BioactivityPotencySummary,
  BioactivityTopMeasurement,
} from "@/types";

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return value.toLocaleString(undefined, { maximumSignificantDigits: 3 });
}

export function formatTopPotency(
  summary: BioactivityPotencySummary[] | null | undefined
): string {
  if (!summary || summary.length === 0) return "—";
  const top = [...summary].sort((a, b) => b.n - a.n)[0];
  const median = formatNumber(top.median);
  const unit = top.unit ? ` ${top.unit}` : "";
  return `${top.endpoint ?? "?"}: ${median}${unit} (n=${top.n})`;
}

// "Headline" measurement surfaced in the bioactivity tables. Backend picks
// it as max-by-value; we render it as "{endpoint}: {value} {unit}" — or
// just "{value} {unit}" if no endpoint label. Same shape on food + chemical
// tables so the columns line up visually.
export function formatTopMeasurement(
  top: BioactivityTopMeasurement | null | undefined
): string {
  if (!top || top.value === null || top.value === undefined) return "—";
  const value = formatNumber(top.value);
  const unit = top.unit ? ` ${top.unit}` : "";
  return top.endpoint ? `${top.endpoint}: ${value}${unit}` : `${value}${unit}`;
}

// Pulls top_measurement from a row — prefers the backend-computed field,
// falls back to scanning the row's `measurements` sample client-side for
// the max-by-value entry. The fallback exists because the new backend
// field isn't deployed to staging yet; once it is, the fast path is hit
// for free and this helper can keep working unchanged.
export function topMeasurementOf(row: {
  top_measurement?: BioactivityTopMeasurement | null;
  measurements?: BioactivityMeasurement[];
}): BioactivityTopMeasurement | null {
  if (
    row.top_measurement &&
    row.top_measurement.value !== null &&
    row.top_measurement.value !== undefined
  ) {
    return row.top_measurement;
  }
  const ms = row.measurements ?? [];
  let top: BioactivityTopMeasurement | null = null;
  for (const m of ms) {
    if (m.value === null || m.value === undefined) continue;
    if (top === null || m.value > (top.value ?? Number.NEGATIVE_INFINITY)) {
      top = { endpoint: m.endpoint, value: m.value, unit: m.unit };
    }
  }
  return top;
}

export function formatFoodMeasurement(
  m: BioactivityMeasurement | undefined
): string {
  if (!m) return "—";
  if (m.value === null || m.value === undefined) return m.outcome ?? "—";
  const unit = m.unit ? ` ${m.unit}` : "";
  return `${formatNumber(m.value)}${unit}`;
}

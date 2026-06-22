import {
  BioactivityMeasurement,
  BioactivityPotencySummary,
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

export function formatFoodMeasurement(
  m: BioactivityMeasurement | undefined
): string {
  if (!m) return "—";
  if (m.value === null || m.value === undefined) return m.outcome ?? "—";
  const unit = m.unit ? ` ${m.unit}` : "";
  return `${formatNumber(m.value)}${unit}`;
}

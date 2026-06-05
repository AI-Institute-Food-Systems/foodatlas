import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import { getBioactivityChemicals } from "@/utils/fetching";
import { BioactivityChemicalRow } from "@/types";

interface Props {
  commonName: string;
}

function formatPotency(
  potency: { value: number | null; unit: string | null } | undefined | null
): string {
  if (!potency || potency.value === null || potency.value === undefined) {
    return "—";
  }
  const unit = potency.unit ?? "";
  return `${potency.value.toFixed(3)}${unit ? ` ${unit}` : ""}`;
}

function formatHill(
  curve:
    | {
        zero_activity: number | null;
        infinite_activity: number | null;
        log_ac50: number | null;
        hill_slope: number | null;
      }
    | undefined
    | null
): string {
  if (
    !curve ||
    (curve.zero_activity === null &&
      curve.infinite_activity === null &&
      curve.log_ac50 === null &&
      curve.hill_slope === null)
  ) {
    return "—";
  }
  const parts = [
    ["A0", curve.zero_activity],
    ["A∞", curve.infinite_activity],
    ["log AC50", curve.log_ac50],
    ["Hill", curve.hill_slope],
  ]
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => `${k}=${(v as number).toFixed(2)}`);
  return parts.join(", ");
}

const BioactivityChemicalsSection = async ({ commonName }: Props) => {
  const payload = await getBioactivityChemicals(commonName);
  const rows: BioactivityChemicalRow[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.total_rows ?? 0;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Chemicals Measured
      </Heading>
      <Card>
        <p className="mb-4 text-sm text-light-400">
          {totalRows === 0
            ? "No chemical-bioactivity measurements available yet."
            : `${totalRows} chemical${
                totalRows === 1 ? "" : "s"
              } measured against this bioactivity.`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Chemical</th>
                <th className="py-2 pr-4">Measurements</th>
                <th className="py-2 pr-4">Potency (first)</th>
                <th className="py-2">Hill curve (first)</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const first = row.measurements?.[0];
                return (
                  <tr
                    key={row.id}
                    className="border-b border-light-50/[0.05] align-top"
                  >
                    <td className="py-2 pr-4 capitalize break-all">{row.name}</td>
                    <td className="py-2 pr-4">{row.measurement_count}</td>
                    <td className="py-2 pr-4">{formatPotency(first?.potency)}</td>
                    <td className="py-2 font-mono text-xs">
                      {formatHill(first?.hill_curve)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
};

BioactivityChemicalsSection.displayName = "BioactivityChemicalsSection";
export default BioactivityChemicalsSection;

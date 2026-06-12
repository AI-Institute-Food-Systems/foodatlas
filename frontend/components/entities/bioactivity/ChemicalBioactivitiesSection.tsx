import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { encodeSpace } from "@/utils/utils";
import { getChemicalBioactivities } from "@/utils/fetching";
import { BioactivityMeasurement } from "@/types";

interface Props {
  commonName: string;
}

interface Row {
  id: string;
  name: string;
  measurement_count: number;
  measurements: BioactivityMeasurement[];
}

const ChemicalBioactivitiesSection = async ({ commonName }: Props) => {
  const payload = await getChemicalBioactivities(commonName);
  const rows: Row[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.total_rows ?? 0;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Bioactivities
      </Heading>
      <Card>
        <p className="mb-4 text-sm text-light-400">
          {totalRows === 0
            ? "No bioactivity measurements recorded for this chemical."
            : `${totalRows} bioactivit${
                totalRows === 1 ? "y" : "ies"
              } measured against this chemical.`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Bioactivity</th>
                <th className="py-2 pr-4">Measurements</th>
                <th className="py-2">First potency</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const first = row.measurements?.[0];
                const potency =
                  first?.potency?.value !== null &&
                  first?.potency?.value !== undefined
                    ? `${first.potency.value.toFixed(3)}${
                        first.potency.unit ? ` ${first.potency.unit}` : ""
                      }`
                    : "—";
                return (
                  <tr
                    key={row.id}
                    className="border-b border-light-50/[0.05] align-top"
                  >
                    <td className="py-2 pr-4 capitalize break-all">
                      <Link
                        href={`/bioactivity/${encodeURIComponent(
                          encodeSpace(row.name)
                        )}`}
                      >
                        {row.name}
                      </Link>
                    </td>
                    <td className="py-2 pr-4">{row.measurement_count}</td>
                    <td className="py-2">{potency}</td>
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

ChemicalBioactivitiesSection.displayName = "ChemicalBioactivitiesSection";
export default ChemicalBioactivitiesSection;

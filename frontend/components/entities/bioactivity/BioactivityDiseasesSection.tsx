import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import { getBioactivityDiseases } from "@/utils/fetching";
import { BioactivityDiseaseRow } from "@/types";

interface Props {
  commonName: string;
}

const BioactivityDiseasesSection = async ({ commonName }: Props) => {
  const payload = await getBioactivityDiseases(commonName);
  const rows: BioactivityDiseaseRow[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.total_rows ?? 0;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Associated Diseases
      </Heading>
      <Card>
        <p className="mb-4 text-sm text-light-400">
          {totalRows === 0
            ? "No disease associations recorded for this bioactivity yet."
            : `${totalRows} disease association${totalRows === 1 ? "" : "s"}.`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Disease</th>
                <th className="py-2 pr-4">Targets</th>
                <th className="py-2">Evidence count</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.id}
                  className="border-b border-light-50/[0.05] align-top"
                >
                  <td className="py-2 pr-4 capitalize break-all">{row.name}</td>
                  <td className="py-2 pr-4 font-mono text-xs break-all">
                    {row.target_ids.length === 0
                      ? "—"
                      : row.target_ids.join(", ")}
                  </td>
                  <td className="py-2">{row.evidence_count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
    </div>
  );
};

BioactivityDiseasesSection.displayName = "BioactivityDiseasesSection";
export default BioactivityDiseasesSection;

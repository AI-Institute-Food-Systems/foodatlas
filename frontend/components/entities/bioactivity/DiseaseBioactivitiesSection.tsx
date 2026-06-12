import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { encodeSpace } from "@/utils/utils";
import { getDiseaseBioactivities } from "@/utils/fetching";

interface Props {
  commonName: string;
}

interface Row {
  id: string;
  name: string;
  polarity: string | null;
  target_ids: string[];
  evidence_count: number;
}

const DiseaseBioactivitiesSection = async ({ commonName }: Props) => {
  const payload = await getDiseaseBioactivities(commonName);
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
            ? "No bioactivity associations recorded for this disease."
            : `${totalRows} bioactivit${
                totalRows === 1 ? "y" : "ies"
              } associated with this disease.`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Bioactivity</th>
                <th className="py-2 pr-4">Targets</th>
                <th className="py-2">Evidence</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
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

DiseaseBioactivitiesSection.displayName = "DiseaseBioactivitiesSection";
export default DiseaseBioactivitiesSection;

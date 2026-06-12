import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { encodeSpace } from "@/utils/utils";
import { getFoodBioactivities } from "@/utils/fetching";

interface Props {
  commonName: string;
}

interface Row {
  id: string;
  name: string;
  exhibit_type: "direct" | "inherited";
  via_chemical_id: string | null;
  via_chemical_name: string | null;
  efficacy_pred: number | null;
  evidence_count: number;
}

const FoodBioactivitiesSection = async ({ commonName }: Props) => {
  const payload = await getFoodBioactivities(commonName, 1, "all");
  const rows: Row[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.total_rows ?? 0;
  const direct = rows.filter((r) => r.exhibit_type === "direct").length;
  const inherited = rows.filter((r) => r.exhibit_type === "inherited").length;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Bioactivities
      </Heading>
      <Card>
        <p className="mb-4 text-sm text-light-400">
          {totalRows === 0
            ? "This food has no recorded bioactivities yet."
            : `${totalRows} bioactivit${
                totalRows === 1 ? "y" : "ies"
              } (${direct} direct, ${inherited} inherited).`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Bioactivity</th>
                <th className="py-2 pr-4">Type</th>
                <th className="py-2 pr-4">Via chemical</th>
                <th className="py-2">Evidence</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr
                  key={`${row.id}-${row.exhibit_type}-${idx}`}
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
                  <td className="py-2 pr-4 lowercase">{row.exhibit_type}</td>
                  <td className="py-2 pr-4 capitalize break-all">
                    {row.via_chemical_name ?? "—"}
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

FoodBioactivitiesSection.displayName = "FoodBioactivitiesSection";
export default FoodBioactivitiesSection;

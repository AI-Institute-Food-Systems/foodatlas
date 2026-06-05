import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import { getBioactivityFoods } from "@/utils/fetching";
import { BioactivityFoodRow } from "@/types";

interface Props {
  commonName: string;
}

const BioactivityFoodsSection = async ({ commonName }: Props) => {
  const payload = await getBioactivityFoods(commonName, 1, "all");
  const rows: BioactivityFoodRow[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.total_rows ?? 0;

  const directCount = rows.filter((r) => r.exhibit_type === "direct").length;
  const inheritedCount = rows.filter(
    (r) => r.exhibit_type === "inherited"
  ).length;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Foods Exhibiting
      </Heading>
      <Card>
        <p className="mb-4 text-sm text-light-400">
          {totalRows === 0
            ? "No foods exhibit this bioactivity yet."
            : `${totalRows} food${totalRows === 1 ? "" : "s"} exhibit this bioactivity (` +
              `${directCount} direct, ${inheritedCount} inherited).`}
        </p>
        {rows.length > 0 && (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-50/10 text-left font-mono text-xs italic text-light-400">
                <th className="py-2 pr-4">Food</th>
                <th className="py-2 pr-4">Type</th>
                <th className="py-2 pr-4">Via chemical</th>
                <th className="py-2">Evidence count</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, idx) => (
                <tr
                  key={`${row.id}-${row.exhibit_type}-${idx}`}
                  className="border-b border-light-50/[0.05] align-top"
                >
                  <td className="py-2 pr-4 capitalize break-all">{row.name}</td>
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

BioactivityFoodsSection.displayName = "BioactivityFoodsSection";
export default BioactivityFoodsSection;

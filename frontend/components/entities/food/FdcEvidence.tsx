import Badge from "@/components/basic/Badge";
import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import { FoodEvidence } from "@/types/Evidence";
import { formatConcentrationValueAlt } from "@/utils/utils";

type FdcEvidenceProps = {
  evidence: FoodEvidence;
};

const FdcEvidence = ({ evidence }: FdcEvidenceProps) => {
  return (
    <Card className="bg-light-900">
      {/* source & link */}
      <div className="flex justify-between items-center">
        <Badge
          size="xs"
          color="text-sky-600 border-sky-600 bg-sky-600/10 shadow-sky-700"
        >
          FDC Database
        </Badge>
        <Link className="text-xs" href={evidence.reference.url}>
          View Source
        </Link>
      </div>
      {/* extraction table */}
      <div className="mt-5 overflow-x-auto">
        <table className="text-xs w-full table-fixed">
          <colgroup>
            <col className="w-[22%]" />
            <col className="w-[22%]" />
            <col className="w-[18%]" />
            <col className="w-[25%]" />
            <col className="w-[13%]" />
          </colgroup>
          <thead>
            <tr className="border-b border-light-700">
              <th className="text-light-400 uppercase font-normal text-left pb-2 pr-4">
                Food
              </th>
              <th className="text-light-400 uppercase font-normal text-left pb-2 px-4">
                Chemical
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 px-4">
                Concentration
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 px-4">
                Converted Concentration
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 pl-4">
                Method
              </th>
            </tr>
          </thead>
          <tbody>
            {evidence.extraction.map((extraction, index) => (
              <tr key={index}>
                <td className="py-2 pr-4 break-all">
                  {extraction.extracted_food_name}
                </td>
                <td className="py-2 px-4 break-all">
                  {extraction.extracted_chemical_name}
                </td>
                <td className="py-2 px-4 text-right whitespace-nowrap">
                  {extraction.extracted_concentration ?? "n/a"}
                </td>
                <td className="py-2 px-4 text-right whitespace-nowrap">
                  {extraction.converted_concentration.unit &&
                  extraction.converted_concentration.value
                    ? `${formatConcentrationValueAlt(
                        extraction.converted_concentration.value
                      )} ${extraction.converted_concentration.unit}`
                    : "n/a"}
                </td>
                <td className="py-2 pl-4 text-right uppercase whitespace-nowrap">
                  {extraction.method}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Card>
  );
};

FdcEvidence.displayName = "FdcEvidence";

export default FdcEvidence;

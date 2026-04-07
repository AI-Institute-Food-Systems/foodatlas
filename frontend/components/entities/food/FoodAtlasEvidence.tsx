import Link from "@/components/basic/Link";
import Badge from "@/components/basic/Badge";
import Card from "@/components/basic/Card";
import { FoodEvidence } from "@/types/Evidence";
import { formatConcentrationValueAlt } from "@/utils/utils";

type FoodAtlasEvidenceProps = {
  evidence: FoodEvidence;
};

const FoodAtlasEvidence = ({ evidence }: FoodAtlasEvidenceProps) => {
  return (
    <Card className="bg-light-900">
      {/* source & link */}
      <div className="flex justify-between items-center">
        <Badge size="xs">FoodAtlas-Extraction</Badge>
        <Link className="text-xs" href={evidence.reference.url}>
          View Paper
        </Link>
      </div>
      {/* premise */}
      <p className="mt-4 leading-tight font-serif italic">
        &quot;
        {evidence.premise
          .split(
            new RegExp(
              `(${evidence.extraction
                .map(
                  (e) =>
                    `${e.extracted_chemical_name}|${e.extracted_food_name}|${e.extracted_concentration}`
                )
                .join("|")})`,
              "gi"
            )
          )
          .map((part, index) => {
            const matchingExtraction = evidence.extraction.find(
              (e) =>
                part?.toLowerCase() === e.extracted_food_name?.toLowerCase() ||
                part?.toLowerCase() ===
                  e.extracted_chemical_name?.toLowerCase() ||
                part?.toLowerCase() === e.extracted_concentration?.toLowerCase()
            );

            if (
              matchingExtraction?.extracted_food_name?.toLowerCase() ===
              part?.toLowerCase()
            ) {
              return (
                <span key={index} className="text-amber-600 bg-amber-600/10">
                  {part}
                </span>
              );
            } else if (
              matchingExtraction?.extracted_chemical_name?.toLowerCase() ===
              part?.toLowerCase()
            ) {
              return (
                <span key={index} className="text-cyan-600 bg-cyan-600/10">
                  {part}
                </span>
              );
            } else if (
              matchingExtraction?.extracted_concentration?.toLowerCase() ===
              part?.toLowerCase()
            ) {
              return (
                <span key={index} className="text-teal-600 bg-teal-600/10">
                  {part}
                </span>
              );
            }
            return part;
          })}
      </p>
      {/* extraction table */}
      <div className="mt-5 overflow-x-auto">
        <table className="text-xs w-full table-fixed">
          <colgroup>
            <col className="w-[20%]" />
            <col className="w-[20%]" />
            <col className="w-[16%]" />
            <col className="w-[24%]" />
            <col className="w-[20%]" />
          </colgroup>
          <thead>
            <tr className="border-b border-light-700">
              <th className="text-light-400 uppercase font-normal text-left pb-2 pr-2">
                Food
              </th>
              <th className="text-light-400 uppercase font-normal text-left pb-2 px-2">
                Chemical
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 px-2">
                Concentration
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 px-2">
                Converted Concentration
              </th>
              <th className="text-light-400 uppercase font-normal text-right pb-2 pl-2">
                Method
              </th>
            </tr>
          </thead>
          <tbody>
            {evidence.extraction.map((extraction, index) => (
              <tr key={index}>
                <td className="py-2 pr-2 break-all">
                  {extraction.extracted_food_name}
                </td>
                <td className="py-2 px-2 break-all">
                  {extraction.extracted_chemical_name}
                </td>
                <td className="py-2 px-2 text-right whitespace-nowrap">
                  {extraction.extracted_concentration ?? "-"}
                </td>
                <td className="py-2 px-2 text-right whitespace-nowrap">
                  {extraction.converted_concentration.unit &&
                  extraction.converted_concentration.value
                    ? `${formatConcentrationValueAlt(
                        extraction.converted_concentration.value
                      )} ${extraction.converted_concentration.unit}`
                    : "-"}
                </td>
                <td className="py-2 pl-2 text-right uppercase break-words">
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

FoodAtlasEvidence.displayName = "FoodAtlasEvidence";

export default FoodAtlasEvidence;

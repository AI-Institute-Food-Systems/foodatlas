import Link from "@/components/basic/Link";
import Badge from "@/components/basic/Badge";
import Card from "@/components/basic/Card";
import { FoodEvidence } from "@/types/Evidence";
import { formatConcentrationValueAlt } from "@/utils/utils";

const HEADERS = [
  { label: "Method" },
  { label: "Extracted Food" },
  { label: "Extracted Chemical" },
  { label: "Extracted Concentration" },
  { label: "Converted Concentration" },
];

type FoodAtlasEvidenceProps = {
  evidence: FoodEvidence;
};

const FoodAtlasEvidence = ({ evidence }: FoodAtlasEvidenceProps) => {
  return (
    <Card className="bg-light-900">
      {/* source & link */}
      <div className="flex justify-between items-center">
        {/* source */}
        <Badge size="xs">FoodAtlas-Extraction</Badge>
        {/* link */}
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
      {/* extraction */}
      {evidence.extraction.map((extraction, index) => (
        <div key={index}>
          <table className="mt-5 text-xs w-full border-collapse table-fixed md:table block">
            {/* table header */}
            <thead className="md:table-header-group hidden">
              {index === 0 && (
                <tr>
                  {HEADERS.map((header, index) => (
                    <th
                      key={index}
                      className={`text-light-400 uppercase font-normal text-left pb-2 first:pr-1 last:pl-1 [&:not(:first-child):not(:last-child)]:px-1 border-b border-light-700 leading-tight`}
                    >
                      {header.label}
                    </th>
                  ))}
                </tr>
              )}
            </thead>
            {/* table body */}
            <tbody className="block md:table-row-group">
              <tr className="last:border-b-0 md:border-light-700 md:table-row flex flex-col">
                {/* method */}
                <td className="py-1.5 grid grid-cols-[auto,1fr] gap-4 items-center md:pr-1 md:table-cell text-right md:text-left">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Method
                  </span>
                  <span className="break-all">{extraction.method}</span>
                </td>
                {/* extracted food */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell text-right md:text-left">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Food
                  </span>
                  <span className="break-all">
                    {extraction.extracted_food_name}
                  </span>
                </td>
                {/* extracted chemical */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell text-right md:text-left">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Chemical
                  </span>
                  <span className="break-all">
                    {extraction.extracted_chemical_name}
                  </span>
                </td>
                {/* extracted concentration */}
                <td className="py-1.5 border-light-600/10  grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell text-right">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Concentration
                  </span>
                  <span className="break-all">
                    {extraction.extracted_concentration ?? "n/a"}
                  </span>
                </td>
                {/* converted concentration */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:pl-1 md:table-cell text-right">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Converted Concentration
                  </span>
                  <span className="break-all">
                    {extraction.converted_concentration.unit &&
                    extraction.converted_concentration.value
                      ? `${formatConcentrationValueAlt(
                          extraction.converted_concentration.value
                        )} ${extraction.converted_concentration.unit}`
                      : "n/a"}
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
          {/* add divider after each extraction except the last one */}
          {index < evidence.extraction.length - 1 && (
            <hr className="my-2 border-light-600 md:hidden" />
          )}
        </div>
      ))}
    </Card>
  );
};

FoodAtlasEvidence.displayName = "FoodAtlasEvidence";

export default FoodAtlasEvidence;

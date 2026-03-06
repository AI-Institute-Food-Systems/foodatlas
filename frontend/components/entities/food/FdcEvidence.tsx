import Badge from "@/components/basic/Badge";
import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import { FoodEvidence } from "@/types/Evidence";
import { formatConcentrationValueAlt } from "@/utils/utils";

const HEADERS = [
  { label: "Extracted Food" },
  { label: "Extracted Chemical" },
  { label: "Extracted Concentration" },
  { label: "Converted Concentration" },
  { label: "Method" },
];

type FdcEvidenceProps = {
  evidence: FoodEvidence;
};

const FdcEvidence = ({ evidence }: FdcEvidenceProps) => {
  return (
    <Card className="bg-light-900">
      {/* source & link */}
      <div className="flex justify-between items-center">
        {/* source */}
        <Badge
          size="xs"
          color="text-sky-600 border-sky-600 bg-sky-600/10 shadow-sky-700"
        >
          FDC Database
        </Badge>
        {/* link */}
        <Link className="text-xs" href={evidence.reference.url}>
          View Source
        </Link>
      </div>
      {/* extraction table */}
      <table className="mt-5 text-xs w-full border-collapse table-fixed md:table block">
        {/* table header */}
        <thead className="md:table-header-group hidden">
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
        </thead>
        {/* table body */}
        <tbody className="block md:table-row-group">
          {evidence.extraction.map((extraction, index) => (
            <>
              <tr
                key={index}
                className="last:border-b-0 md:border-light-700 md:table-row flex flex-col"
              >
                {/* extracted food */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:pr-1 md:table-cell">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Food
                  </span>
                  <span className="text-right md:text-left break-all">
                    {extraction.extracted_food_name}
                  </span>
                </td>
                {/* extracted chemical */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell ">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Chemical
                  </span>
                  <span className="text-right md:text-left break-all">
                    {extraction.extracted_chemical_name}
                  </span>
                </td>
                {/* extracted concentration */}
                <td className="py-1.5 border-light-600/10  grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Extracted Concentration
                  </span>
                  <span className="text-right md:text-left break-all">
                    {extraction.extracted_concentration ?? "n/a"}
                  </span>
                </td>
                {/* converted concentration */}
                <td className="py-1.5 border-light-600/10 grid grid-cols-[auto,1fr] gap-4 items-center md:px-1 md:table-cell">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Converted Concentration
                  </span>
                  <span className="text-right break-all md:text-left">
                    {extraction.converted_concentration.unit &&
                    extraction.converted_concentration.value
                      ? `${formatConcentrationValueAlt(
                          extraction.converted_concentration.value
                        )} ${extraction.converted_concentration.unit}`
                      : "n/a"}
                  </span>
                </td>
                {/* method */}
                <td className="py-1.5 grid grid-cols-[auto,1fr] gap-4 items-center md:pl-1 md:table-cell ">
                  <span className="text-light-400 italic font-mono md:hidden">
                    Method
                  </span>
                  <span className="text-right break-all md:text-left">
                    {extraction.method}
                  </span>
                </td>
              </tr>
              {/* divider when smaller than md */}
              {index < evidence.extraction.length - 1 && (
                <hr className="my-2 border-light-600 md:hidden" />
              )}
            </>
          ))}
        </tbody>
      </table>
    </Card>
  );
};

FdcEvidence.displayName = "FdcEvidence";

export default FdcEvidence;

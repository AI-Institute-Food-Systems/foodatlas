import Badge from "@/components/basic/Badge";
import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import { AmbiguityIcon } from "@/components/basic/Ambiguity";
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
      {/* extraction table — desktop */}
      <div className="hidden md:block mt-5 overflow-x-auto">
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
                  <span className="inline-flex items-center gap-1 align-middle">
                    {extraction.extracted_chemical_name}
                    <AmbiguityIcon
                      chemicalCandidates={extraction.chemical_candidates}
                    />
                  </span>
                </td>
                <td className="py-2 px-2 text-right whitespace-nowrap">
                  {extraction.extracted_concentration ?? "—"}
                </td>
                <td className="py-2 px-2 text-right whitespace-nowrap">
                  {extraction.converted_concentration.unit &&
                  extraction.converted_concentration.value
                    ? `${formatConcentrationValueAlt(
                        extraction.converted_concentration.value
                      )} ${extraction.converted_concentration.unit}`
                    : "—"}
                </td>
                <td className="py-2 pl-2 text-right uppercase break-words">
                  {extraction.method}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* extraction cards — mobile. Chemical is the primary line;
       * Food / Concentration / Converted / Method sit below as
       * label-value rows with justify-between. */}
      <div className="md:hidden mt-4 w-full flex flex-col divide-y divide-light-800 text-xs">
        {evidence.extraction.map((extraction, index) => (
          <div key={index} className="w-full py-3 flex flex-col gap-1.5">
            <div className="w-full flex items-center gap-1.5 flex-wrap capitalize text-sm text-light-100">
              {extraction.extracted_chemical_name}
              <AmbiguityIcon
                chemicalCandidates={extraction.chemical_candidates}
              />
            </div>
            <div className="w-full flex items-baseline justify-between gap-2">
              <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                Food
              </span>
              <span className="capitalize text-light-300 text-right break-all">
                {extraction.extracted_food_name}
              </span>
            </div>
            <div className="w-full flex items-baseline justify-between gap-2">
              <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                Conc.
              </span>
              <span className="font-mono tabular-nums text-light-300 text-right">
                {extraction.extracted_concentration ?? "—"}
              </span>
            </div>
            <div className="w-full flex items-baseline justify-between gap-2">
              <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                Converted
              </span>
              <span className="font-mono tabular-nums text-light-300 text-right">
                {extraction.converted_concentration.unit &&
                extraction.converted_concentration.value
                  ? `${formatConcentrationValueAlt(
                      extraction.converted_concentration.value
                    )} ${extraction.converted_concentration.unit}`
                  : "—"}
              </span>
            </div>
            <div className="w-full flex items-baseline justify-between gap-2">
              <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                Method
              </span>
              <span className="uppercase text-light-300 text-right break-words">
                {extraction.method}
              </span>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
};

FdcEvidence.displayName = "FdcEvidence";

export default FdcEvidence;

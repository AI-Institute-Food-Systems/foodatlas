import { MdInfoOutline } from "react-icons/md";

import Card from "@/components/basic/Card";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { encodeSpace } from "@/utils/utils";
import { getBioactivityFoods } from "@/utils/fetching";
import { BioactivityFoodRow } from "@/types";
import { formatFoodMeasurement } from "./format";

interface Props {
  commonName: string;
}

const HEADERS = [
  { label: "Food", align: "left" as const },
  { label: "Measurements", align: "right" as const },
  { label: "First value", align: "right" as const },
];

const BioactivityFoodsSection = async ({ commonName }: Props) => {
  const payload = await getBioactivityFoods(commonName);
  const rows: BioactivityFoodRow[] = payload.data ?? [];
  const totalRows: number = payload.metadata?.row_count ?? rows.length;

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Foods Exhibiting
      </Heading>
      <Card>
        <div className="text-sm text-neutral-400">
          {`Found ${totalRows.toLocaleString()} food${
            totalRows === 1 ? "" : "s"
          }`}
        </div>
        <div className="mt-3 overflow-x-auto">
          <table className="w-full table-fixed">
            <colgroup>
              <col className="w-[50%]" />
              <col className="w-[20%]" />
              <col className="w-[30%]" />
            </colgroup>
            <thead className="text-light-400 text-left">
              <tr>
                {HEADERS.map((header, idx) => (
                  <th
                    key={header.label}
                    className={`h-12 border-b border-light-700 leading-none break-all md:break-normal py-3 ${
                      idx === 0
                        ? "pr-4"
                        : idx === HEADERS.length - 1
                        ? "pl-4"
                        : "px-4"
                    } ${header.align === "right" ? "text-right" : "text-left"}`}
                  >
                    <span className="select-none uppercase text-xs font-medium">
                      {header.label}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="font-light">
              {rows.length > 0 ? (
                rows.map((row) => (
                  <tr key={row.id}>
                    <td className="py-3 pr-4">
                      <div className="flex min-h-12 capitalize items-center">
                        <Link
                          href={`/food/${encodeURIComponent(
                            encodeSpace(row.name)
                          )}`}
                          isExternal={false}
                        >
                          {row.name}
                        </Link>
                      </div>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex min-h-12 items-center justify-end">
                        {row.measurement_count.toLocaleString()}
                      </div>
                    </td>
                    <td className="py-3 pl-4">
                      <div className="flex min-h-12 items-center justify-end font-mono text-xs">
                        {formatFoodMeasurement(row.measurements?.[0])}
                      </div>
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={HEADERS.length}>
                    <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                      <MdInfoOutline /> No foods exhibit this bioactivity yet
                    </div>
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
};

BioactivityFoodsSection.displayName = "BioactivityFoodsSection";
export default BioactivityFoodsSection;

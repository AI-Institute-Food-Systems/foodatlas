export const dynamic = "force-dynamic";

import { Metadata } from "next";
import { MdDownload } from "react-icons/md";

import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";
import { DownloadEntry } from "@/types";
import { getDownloadEntries } from "@/utils/fetching";

const HEADERS = [
  {
    label: "version",
  },
  {
    label: "release date",
  },
  {
    label: "changelog",
  },
  {
    label: "size",
  },
  {
    label: "",
  },
];

export const metadata: Metadata = {
  title: "FoodAtlas | Download Food Composition Data",
  description:
    "FoodAtlas is provided as a free resource for public use. Download version-controlled database bundles to work with evidence-based food composition data on your machine.",
};

const Downloads = async () => {
  const data: DownloadEntry[] = await getDownloadEntries();

  return (
    <div>
      {/* heading & caption */}
      <div>
        <Heading type="h1">Download Database Bundles</Heading>
        <SubHeading>
          Want to work with our data locally? Download our version-controlled
          database bundles below and explore <i>FoodAtlas</i> data on your
          machine
        </SubHeading>
        <p className="mt-10 text-lg leading-relaxed text-light-200">
          Our extensive food composition database contains only evidence-based
          data that can be traced back to its source. As a USDA-NSF funded
          research project, the data is presented as a free resource under the{" "}
          <Link href="https://www.apache.org/licenses/LICENSE-2.0">
            Apache-2.0
          </Link>{" "}
          license.
        </p>
      </div>
      <Divider />
      {/* table */}
      <div className="mt-14">
        <Card>
          <div className="overflow-x-auto">
            <table className="w-full md:table-fixed">
              <thead className="text-light-400 text-left">
                <tr>
                  {HEADERS.map((header, index) => (
                    <th
                      key={index}
                      className={`h-12 border-b border-light-700 leading-none  py-2 ${
                        index === 0
                          ? "pr-3"
                          : index === HEADERS.length
                          ? "pl-3"
                          : "px-3"
                      }`}
                    >
                      <div className="flex flex-nowrap">
                        <span className="select-none uppercase text-xs font-medium w-full">
                          {header.label}
                        </span>
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="font-light">
                {data.map((row, index) => {
                  return (
                    <tr key={row.release_date + "_" + row.version}>
                      <td className="py-2 pr-3">
                        <div className="flex min-h-12 capitalize items-center">
                          {row.version}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex min-h-12 capitalize items-center">
                          {row.release_date}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex min-h-12 capitalize items-center">
                          {row.change_log}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex min-h-12 justify-end capitalize items-center">
                          {row.file_size}
                        </div>
                      </td>
                      <td className="py-2 pl-3">
                        <div className="flex min-h-12 justify-end capitalize items-center">
                          <Button
                            variant="outlined"
                            size="xs"
                            href={row.download_link}
                          >
                            <MdDownload />
                            Download
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>
      </div>
    </div>
  );
};

export default Downloads;

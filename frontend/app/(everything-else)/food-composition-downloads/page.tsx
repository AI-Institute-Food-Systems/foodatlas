export const dynamic = "force-dynamic";

import { Metadata } from "next";

import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";
import DownloadsTable, {
  DownloadRow,
} from "@/components/misc/DownloadsTable";
import { DownloadEntry } from "@/types";
import { getDownloadEntries } from "@/utils/fetching";

export const metadata: Metadata = {
  title: "FoodAtlas | Download Food Composition Data",
  description:
    "FoodAtlas is provided as a free resource for public use. Download version-controlled database bundles to work with evidence-based food composition data on your machine.",
};

async function fetchSummary(url: string): Promise<string> {
  try {
    const res = await fetch(url, { next: { revalidate: 3600 } });
    if (!res.ok) return "";
    return (await res.text()).trim();
  } catch {
    return "";
  }
}

const Downloads = async () => {
  const entries: DownloadEntry[] = await getDownloadEntries();
  const summaries = await Promise.all(
    entries.map((e) => fetchSummary(e.summary_link)),
  );
  const data: DownloadRow[] = entries.map((entry, i) => ({
    ...entry,
    summary: summaries[i],
  }));

  return (
    <div>
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
      <div className="mt-14">
        <Heading type="h2" className="text-3xl">
          How to Cite
        </Heading>
        <SubHeading>
          If you use <i>FoodAtlas</i> in your research, please cite:
        </SubHeading>
        <Card className="mt-6">
          <p className="leading-relaxed text-light-200">
            Li, F., Youn, J., Xie, K. et al. A unified knowledge graph linking
            foodomics to chemical-disease networks and flavor profiles.{" "}
            <i>npj Sci Food</i> <strong>10</strong>, 33 (2026).{" "}
            <Link href="https://doi.org/10.1038/s41538-025-00680-9">
              https://doi.org/10.1038/s41538-025-00680-9
            </Link>
          </p>
        </Card>
      </div>
      <Divider />
      <div className="mt-14">
        <Card>
          <DownloadsTable data={data} />
        </Card>
        <p className="mt-4 text-sm text-light-400">
          Versions prior to v4.0 are retired and no longer available for
          download.
        </p>
      </div>
    </div>
  );
};

export default Downloads;

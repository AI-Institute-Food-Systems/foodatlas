export const dynamic = "force-dynamic";

import { Metadata } from "next";

import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import Citation from "@/components/basic/Citation";
import Heading from "@/components/basic/Heading";
import DownloadsTable, {
  DownloadRow,
} from "@/components/misc/DownloadsTable";
import JsonLd from "@/components/misc/JsonLd";
import { DownloadEntry } from "@/types";
import { getDownloadEntries } from "@/utils/fetching";
import { CANONICAL_PUBLICATION } from "@/utils/publications";
import { datasetJsonLd } from "@/utils/structuredData";

export const metadata: Metadata = {
  alternates: { canonical: "/food-composition-downloads" },
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

interface DownloadsPageProps {
  searchParams?: { error?: string };
}

const Downloads = async ({ searchParams }: DownloadsPageProps) => {
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
      {/* schema.org Dataset: what Google Dataset Search and data-hungry
       * crawlers read instead of scraping entity pages. */}
      <JsonLd data={datasetJsonLd(entries)} />
      <div>
        <Heading type="h1" variant="display">Download Database Bundles</Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          Our extensive food composition database contains only evidence-based
          data that can be traced back to its source. As a USDA-NSF funded
          research project, the data is presented as a free resource under the{" "}
          <Link href="https://www.apache.org/licenses/LICENSE-2.0">
            Apache-2.0
          </Link>{" "}
          license. Downloads are gated the same way as the API: request a free
          key through the{" "}
          <Link href="/contact?api-access" isExternal={false}>
            contact form
          </Link>{" "}
          and use it below, or fetch bundles programmatically via{" "}
          <Link href="/developers" isExternal={false}>
            /v1/bundles
          </Link>
          .
        </p>
      </div>
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          How to Cite
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="leading-relaxed text-light-200">
            <Citation publication={CANONICAL_PUBLICATION} />
          </p>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Bundles
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <DownloadsTable data={data} error={searchParams?.error} />
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

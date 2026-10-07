export const dynamic = "force-dynamic";

import { Metadata } from "next";

import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import Citation from "@/components/basic/Citation";
import Heading from "@/components/basic/Heading";
import DownloadsTable, {
  DownloadRow,
} from "@/components/misc/DownloadsTable";
import DataLicenseNote from "@/components/misc/DataLicenseNote";
import JsonLd from "@/components/misc/JsonLd";
import { DownloadEntry } from "@/types";
import { getDownloadEntries } from "@/utils/fetching";
import { CANONICAL_PUBLICATION } from "@/utils/publications";
import { datasetJsonLd } from "@/utils/structuredData";
import { buildMetadata } from "@/utils/site";

export const metadata: Metadata = buildMetadata({
  title: "Download Food Composition Data",
  description:
    "Download versioned FoodAtlas bundles: the full evidence-based food composition knowledge graph as parquet tables. Free for public use with an API key.",
  path: "/food-composition-downloads",
});

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
        <Heading type="h1" variant="display">
          Download Database Bundles
        </Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          Each bundle is a versioned snapshot of the full food–chemical–disease
          graph as parquet tables, with every association traced to its source.
          Download them below, or fetch them programmatically via{" "}
          <Link href="/developers" isExternal={false}>
            /v1/bundles
          </Link>
          .
        </p>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Get an API key
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="text-base font-light text-light-300">
            Downloads use the same free key as the API.{" "}
            <Link href="/contact?api-access" isExternal={false}>
              Request access via the contact form
            </Link>{" "}
            with your name, affiliation, and a one-line description of what
            you&apos;re building. You&apos;ll usually hear back within a few
            business days.
          </p>
          <DataLicenseNote />
        </Card>
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

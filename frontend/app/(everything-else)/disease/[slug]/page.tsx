import { Metadata } from "next";
import { Suspense } from "react";

import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { requireEntity } from "@/components/entities/requireEntity";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import { buildTabs } from "@/components/entities/buildTabs";
import { correlationEvidenceCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import { CORRELATION_DEFAULT_SORT } from "@/components/entities/shared/correlationSort";
import { getDiseaseData, getMetaData } from "@/utils/fetching";
import JsonLd from "@/components/misc/JsonLd";
import { apiEntityUrl, canonicalUrl } from "@/utils/site";
import { entityJsonLd } from "@/utils/structuredData";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface DiseasePageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: DiseasePageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  // Repeats the layout's check (cached) so a 404 doesn't get the slug as
  // its <title>. Past it, null metaData means the API did not answer; fall
  // back to the slug rather than fail the page.
  await requireEntity("disease", slug);
  const metaData = await getMetaData(commonName, "disease");
  const name = metaData?.common_name ?? commonName;

  return {
    title: `${toTitleCase(name)} and Your Health`,
    description: `Evidence-based correlations between ${toTitleCase(name)} and the foods that contain it.`,
    // The same entity as JSON, for anyone who wants the data not the page.
    alternates: {
      canonical: canonicalUrl("disease", name),
      ...(metaData && {
        types: { "application/json": apiEntityUrl("disease", metaData.id) },
      }),
    },
  };
}

const DiseasePage = async ({ params }: DiseasePageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "disease" as const;

  // The one counted tab. A tab mounts only when opened, so an unfetched
  // count leaves its badge placeholder pulsing for the life of the page.
  // Count only; the tab still loads lazily.
  // metaPayload is the same cached call generateMetadata made; it only
  // feeds the JSON-LD here.
  // literaturePage is the Chemicals tab's first page, rendered on the
  // server so crawlers see the table; the count fetches the same URL.
  const [healthCount, metaPayload, literaturePage] = await Promise.all([
    correlationEvidenceCount(commonName, "disease"),
    getMetaData(commonName, entityType).catch(() => null),
    getDiseaseData(
      commonName,
      1,
      "disease",
      "all",
      "",
      CORRELATION_DEFAULT_SORT
    ).catch(() => null),
  ]);

  // Server-only; rendered outright and in the Overview tab's snapshot slot,
  // so it's in the HTML whether or not the tab was opened. A factory, not
  // one shared element: RSC dedupes a repeated element into a single
  // reference, and SSR then fails on the shared <Suspense> ("reading
  // 'fallback'"), dropping the whole page to client rendering.
  const overview = () => (
    <Suspense fallback={<EntityOverviewPanelSuspense entityType={entityType} />}>
      <EntityOverviewPanel commonName={commonName} entityType={entityType} />
    </Suspense>
  );

  return (
    <>
      {metaPayload && <JsonLd data={entityJsonLd(entityType, metaPayload)} />}
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId={DEFAULT_TAB_ID[entityType]}
        tabs={buildTabs(entityType, {
          health: {
            count: healthCount,
            content: (
              <CorrelationEvidenceTab
                commonName={commonName}
                anchor="disease"
                initialLiterature={literaturePage}
              />
            ),
          },
          overview: { content: overview(), snapshot: overview() },
        })}
      />
    </>
  );
};

DiseasePage.displayName = "DiseasePage";
export default DiseasePage;

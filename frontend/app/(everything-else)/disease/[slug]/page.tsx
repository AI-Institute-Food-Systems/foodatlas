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
import { CORRELATION_DEFAULT_SORT } from "@/components/entities/shared/correlationSort";
import TabSnapshot from "@/components/entities/shared/TabSnapshot";
import {
  getDiseaseChemicalAssociations,
  getDiseaseData,
  getMetaData,
} from "@/utils/fetching";
import { assayInferredSection } from "@/utils/tabSnapshots";
import JsonLd from "@/components/misc/JsonLd";
import {
  apiEntityUrl,
  buildMetadata,
  canonicalUrl,
  fitDescription,
  fitTitle,
} from "@/utils/site";
import {
  entityBreadcrumbJsonLd,
  entityJsonLd,
} from "@/utils/structuredData";
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

  return buildMetadata({
    title: fitTitle(toTitleCase(name), ": Chemical Associations"),
    description: fitDescription(
      `Chemicals linked to ${toTitleCase(name)} by literature and bioassay evidence, and the foods that contain them. Every link traced to its source.`
    ),
    path: canonicalUrl("disease", name),
    // The same entity as JSON, for anyone who wants the data not the page.
    jsonAlternate: metaData ? apiEntityUrl("disease", metaData.id) : undefined,
  });
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
  // labAssays is the count's other fetch, reused for the lab-assay
  // table's hidden server rows.
  const [healthCount, metaPayload, literaturePage, labAssays] = await Promise.all([
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
    getDiseaseChemicalAssociations(commonName).catch(() => null),
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
      <JsonLd
        data={entityBreadcrumbJsonLd(
          entityType,
          toTitleCase(metaPayload?.common_name ?? commonName),
          metaPayload?.common_name ?? commonName
        )}
      />
      {/* Rendered in order with the page, from the metadata awaited above.
       * Behind its own Suspense it streamed last, so the h1 (the mobile
       * LCP element) painted only once the whole document was in. */}
      <HeaderSection
        commonName={commonName}
        entityType={entityType}
        metadata={metaPayload}
      />
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
                labAssaySnapshot={
                  <TabSnapshot
                    sections={[assayInferredSection("chemical", labAssays)]}
                  />
                }
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

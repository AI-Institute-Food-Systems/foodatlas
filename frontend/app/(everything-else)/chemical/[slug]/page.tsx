import { Suspense } from "react";
import { Metadata } from "next";

import ChemicalCompositionSection from "@/components/entities/chemical/ChemicalCompositionSection";
import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { requireEntity } from "@/components/entities/requireEntity";
import { buildTabs } from "@/components/entities/buildTabs";
import { correlationEvidenceCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import ChemicalCompositionSectionSuspense from "@/components/entities/chemical/ChemicalCompositionSectionSuspense";
import {
  getChemicalBioactivities,
  getChemicalCompositionData,
  getChemicalDiseaseAssociations,
  getDiseaseData,
  getMetaData,
} from "@/utils/fetching";
import JsonLd from "@/components/misc/JsonLd";
import TabSnapshot from "@/components/entities/shared/TabSnapshot";
import { CORRELATION_DEFAULT_SORT } from "@/components/entities/shared/correlationSort";
import {
  apiEntityUrl,
  buildMetadata,
  canonicalUrl,
  fitTitle,
} from "@/utils/site";
import { entityJsonLd } from "@/utils/structuredData";
import {
  assayInferredSection,
  bioactivityListSection,
  literatureSection,
} from "@/utils/tabSnapshots";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface ChemicalPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: ChemicalPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  // Repeats the layout's check (cached) so a 404 doesn't get the slug as
  // its <title>.
  await requireEntity("chemical", slug);
  // Same cached call the page body makes; only needed here for the id.
  // Null for a real page too: chemicals known only from bioassays have no
  // metadata.
  const metaData = await getMetaData(commonName, "chemical");

  // Falls back to the slug-derived name for metadata-less chemicals.
  const name = metaData?.common_name ?? commonName;

  return buildMetadata({
    title: fitTitle(toTitleCase(name), " in Foods"),
    description: `Discover which foods contain ${toTitleCase(name)} and how it impacts your health.`,
    path: canonicalUrl("chemical", name),
    // The same entity as JSON, for anyone who wants the data not the page.
    jsonAlternate: metaData ? apiEntityUrl("chemical", metaData.id) : undefined,
  });
}

const ChemicalPage = async ({ params }: ChemicalPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "chemical" as const;

  // Parallel best-effort count fetches for the tab badges. Every counted
  // tab needs one: a tab only mounts when opened, so without a count from
  // here its badge placeholder pulses for the life of the page. These are
  // counts, not content — the tabs still load lazily. The Health tab's two
  // first pages feed its snapshot; they're the same URLs the count fetches,
  // so they come out of the fetch cache.
  const [
    composition,
    bioPayload,
    metaPayload,
    healthCount,
    literaturePage,
    assayPayload,
  ] = await Promise.all([
    getChemicalCompositionData(commonName).catch(() => null),
    getChemicalBioactivities(commonName).catch(() => null),
    getMetaData(commonName, entityType).catch(() => null),
    correlationEvidenceCount(commonName, "chemical"),
    getDiseaseData(
      commonName,
      1,
      "chemical",
      "all",
      "",
      CORRELATION_DEFAULT_SORT
    ).catch(() => null),
    getChemicalDiseaseAssociations(commonName),
  ]);
  const compositionCount = composition
    ? (composition.with_concentrations?.length ?? 0) +
      (composition.without_concentrations?.length ?? 0)
    : null;
  const bioactivitiesCount =
    (bioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

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
          composition: {
            count: compositionCount,
            content: (
              <Suspense fallback={<ChemicalCompositionSectionSuspense />}>
                <ChemicalCompositionSection commonName={commonName} />
              </Suspense>
            ),
          },
          bioactivities: {
            count: bioactivitiesCount,
            content: (
              <ChemicalBioactivitiesSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
            snapshot: (
              <TabSnapshot
                sections={[
                  bioactivityListSection("Bioactivities", "bioactivity", bioPayload),
                ]}
              />
            ),
          },
          health: {
            count: healthCount,
            content: (
              <CorrelationEvidenceTab
                commonName={commonName}
                anchor="chemical"
              />
            ),
            snapshot: (
              <TabSnapshot
                sections={[
                  literatureSection("disease", literaturePage),
                  assayInferredSection("disease", assayPayload),
                ]}
              />
            ),
          },
          overview: { content: overview(), snapshot: overview() },
        })}
      />
    </>
  );
};

ChemicalPage.displayName = "ChemicalPage";

export default ChemicalPage;

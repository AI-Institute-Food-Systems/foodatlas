import { Suspense } from "react";
import { Metadata } from "next";

import ChemicalCompositionSection from "@/components/entities/chemical/ChemicalCompositionSection";
import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { buildTabs } from "@/components/entities/buildTabs";
import { correlationEvidenceCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import ChemicalCompositionSectionSuspense from "@/components/entities/chemical/ChemicalCompositionSectionSuspense";
import {
  getChemicalBioactivities,
  getChemicalCompositionData,
  getMetaData,
} from "@/utils/fetching";
import { apiEntityUrl, canonicalUrl } from "@/utils/site";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface ChemicalPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: ChemicalPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  // Same cached call the page body makes; only needed here for the id.
  // Unlike the other entity pages this one keeps rendering without it.
  const metaData = await getMetaData(commonName, "chemical");

  return {
    // Without metaData this is an unknown chemical, and the page used to
    // render as a fully indexable 200 — every nonsense slug became a
    // self-canonicalising page with a real title and description, i.e. an
    // unbounded soft-404 surface. The page still renders (that is deliberate,
    // see the body below), but it does not invite indexing.
    ...(metaData ? {} : { robots: { index: false, follow: false } }),
    title: `${toTitleCase(commonName)} in Foods - Evidence Based Database`,
    description: `Discover which foods contain ${toTitleCase(
      commonName
    )} and how it impacts your health.`,
    alternates: {
      // Unlike the other entity pages this one renders without metaData, so
      // the canonical falls back to the slug-derived name rather than
      // disappearing on the exact pages most likely to be crawled oddly.
      canonical: canonicalUrl("chemical", metaData?.common_name ?? commonName),
      // The same entity as JSON, for anyone who wants the data not the page.
      ...(metaData && {
        types: { "application/json": apiEntityUrl("chemical", metaData.id) },
      }),
    },
  };
}

const ChemicalPage = async ({ params }: ChemicalPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "chemical" as const;

  // Parallel best-effort count fetches for the tab badges. Every counted
  // tab needs one: a tab only mounts when opened, so without a count from
  // here its badge placeholder pulses for the life of the page. These are
  // counts, not content — the tabs still load lazily.
  const [composition, bioPayload, metaPayload, healthCount] =
    await Promise.all([
      getChemicalCompositionData(commonName).catch(() => null),
      getChemicalBioactivities(commonName).catch(() => null),
      getMetaData(commonName, entityType).catch(() => null),
      correlationEvidenceCount(commonName, "chemical"),
    ]);
  const compositionCount = composition
    ? (composition.with_concentrations?.length ?? 0) +
      (composition.without_concentrations?.length ?? 0)
    : null;
  const bioactivitiesCount =
    (bioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

  return (
    <>
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
          },
          health: {
            count: healthCount,
            content: (
              <CorrelationEvidenceTab
                commonName={commonName}
                anchor="chemical"
              />
            ),
          },
          overview: {
            content: (
              <Suspense
                fallback={
                  <EntityOverviewPanelSuspense entityType={entityType} />
                }
              >
                <EntityOverviewPanel
                  commonName={commonName}
                  entityType={entityType}
                />
              </Suspense>
            ),
          },
        })}
      />
    </>
  );
};

ChemicalPage.displayName = "ChemicalPage";

export default ChemicalPage;

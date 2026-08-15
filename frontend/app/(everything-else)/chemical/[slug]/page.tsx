import { Suspense } from "react";
import { Metadata } from "next";

import ChemicalCompositionSection from "@/components/entities/chemical/ChemicalCompositionSection";
import ChemicalCorrelationSection from "@/components/entities/chemical/ChemicalCorrelationSection";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { buildTabs } from "@/components/entities/buildTabs";
import { healthImpactsCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import ChemicalCompositionSectionSuspense from "@/components/entities/chemical/ChemicalCompositionSectionSuspense";
import {
  getChemicalBioactivities,
  getChemicalCompositionData,
  getMetaData,
} from "@/utils/fetching";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface ChemicalPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: ChemicalPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  return {
    title: `${toTitleCase(commonName)} in Foods - Evidence Based Database`,
    description: `Discover which foods contain ${toTitleCase(
      commonName
    )} and how it impacts your health.`,
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
      healthImpactsCount(commonName, "chemical"),
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
            content: <ChemicalCorrelationSection commonName={commonName} />,
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

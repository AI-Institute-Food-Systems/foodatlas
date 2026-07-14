import { Suspense } from "react";
import { Metadata } from "next";

import ChemicalCompositionSection from "@/components/entities/chemical/ChemicalCompositionSection";
import ChemicalCorrelationSection from "@/components/entities/chemical/ChemicalCorrelationSection";
import ChemicalBioactivitiesSection from "@/components/entities/bioactivity/ChemicalBioactivitiesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import ChemicalCompositionSectionSuspense from "@/components/entities/chemical/ChemicalCompositionSectionSuspense";
import EntityPageGate from "@/components/entities/EntityPageGate";
import EntitySubnavbar from "@/components/entities/EntitySubnavbar";
import StickyOnScrollPast from "@/components/entities/StickyOnScrollPast";
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

  // Parallel best-effort count fetches for the tab badges. Health Impacts
  // count would require two paginated requests (positive + negative); we
  // omit it for now (tab renders without a badge).
  const [composition, bioPayload, metaPayload] = await Promise.all([
    getChemicalCompositionData(commonName).catch(() => null),
    getChemicalBioactivities(commonName).catch(() => null),
    getMetaData(commonName, entityType).catch(() => null),
  ]);
  const compositionCount = composition
    ? (composition.with_concentrations?.length ?? 0) +
      (composition.without_concentrations?.length ?? 0)
    : null;
  const bioactivitiesCount =
    (bioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

  return (
    <EntityPageGate entityType={entityType} tabCount={4}>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <StickyOnScrollPast targetId="entity-tab-strip-sentinel">
        <EntitySubnavbar commonName={commonName} entityType={entityType} />
      </StickyOnScrollPast>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId="composition"
        tabs={[
          {
            id: "composition",
            label: "Foods Containing",
            count: compositionCount,
            content: (
              <Suspense fallback={<ChemicalCompositionSectionSuspense />}>
                <ChemicalCompositionSection commonName={commonName} />
              </Suspense>
            ),
          },
          {
            id: "health",
            label: "Health Impacts",
            content: <ChemicalCorrelationSection commonName={commonName} />,
          },
          {
            id: "bioactivities",
            label: "Bioactivities",
            count: bioactivitiesCount,
            content: (
              <ChemicalBioactivitiesSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
          },
          {
            id: "overview",
            label: "IDs & Metadata",
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
        ]}
      />
    </EntityPageGate>
  );
};

ChemicalPage.displayName = "ChemicalPage";

export default ChemicalPage;

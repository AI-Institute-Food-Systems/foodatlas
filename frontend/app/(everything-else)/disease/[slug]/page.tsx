import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import DiseaseBioactivitiesSection from "@/components/entities/disease/DiseaseBioactivitiesSection";
import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import { buildTabs } from "@/components/entities/buildTabs";
import {
  correlationEvidenceCount,
  diseaseBioactivitiesCount,
} from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import { getMetaData } from "@/utils/fetching";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface DiseasePageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: DiseasePageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  const metaData = await getMetaData(commonName, "disease");
  if (!metaData) notFound();

  return {
    title: `${toTitleCase(metaData.common_name)} and Your Health`,
    description: `Evidence-based correlations between ${toTitleCase(
      metaData.common_name
    )} and the foods that contain it.`,
  };
}

const DiseasePage = async ({ params }: DiseasePageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "disease" as const;

  // Counts for every counted tab, in parallel. A tab mounts only when
  // opened, so an unfetched count leaves its badge placeholder pulsing for
  // the life of the page — this page previously fetched none, so all three
  // badges did. Counts only; the tabs still load lazily.
  const [healthCount, bioactivitiesCount] = await Promise.all([
    correlationEvidenceCount(commonName, "disease"),
    diseaseBioactivitiesCount(commonName),
  ]);

  return (
    <>
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
              />
            ),
          },
          bioactivities: {
            count: bioactivitiesCount,
            content: <DiseaseBioactivitiesSection commonName={commonName} />,
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

DiseasePage.displayName = "DiseasePage";
export default DiseasePage;

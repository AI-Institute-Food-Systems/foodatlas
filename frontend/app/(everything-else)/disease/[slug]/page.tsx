import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import DiseaseAssayInferredSection from "@/components/entities/disease/DiseaseAssayInferredSection";
import DiseaseBioactivitiesSection from "@/components/entities/disease/DiseaseBioactivitiesSection";
import DiseaseCorrelationsSection from "@/components/entities/disease/DiseaseCorrelationsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import { buildTabs } from "@/components/entities/buildTabs";
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
            content: <DiseaseCorrelationsSection commonName={commonName} />,
          },
          "assay-inferred": {
            content: <DiseaseAssayInferredSection commonName={commonName} />,
          },
          bioactivities: {
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

import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import DiseaseCorrelationsSection from "@/components/entities/disease/DiseaseCorrelationsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
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
    <div>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId="health"
        tabs={[
          {
            id: "health",
            label: "Health Impacts",
            content: <DiseaseCorrelationsSection commonName={commonName} />,
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
    </div>
  );
};

DiseasePage.displayName = "DiseasePage";
export default DiseasePage;

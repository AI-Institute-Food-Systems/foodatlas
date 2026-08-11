import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import EntityPageGate from "@/components/entities/EntityPageGate";
import {
  getBioactivityChemicals,
  getBioactivityFoods,
  getMetaData,
} from "@/utils/fetching";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface BioactivityPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: BioactivityPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  const metaData = await getMetaData(commonName, "bioactivity");
  if (!metaData) notFound();

  return {
    title: `${toTitleCase(metaData.common_name)} — Bioactivity Profile`,
    description: `Chemical measurements and food sources for the ${toTitleCase(
      metaData.common_name
    )} bioactivity.`,
  };
}

const BioactivityPage = async ({ params }: BioactivityPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "bioactivity" as const;

  const [chemPayload, foodPayload, metaPayload] = await Promise.all([
    getBioactivityChemicals(commonName).catch(() => null),
    getBioactivityFoods(commonName).catch(() => null),
    getMetaData(commonName, entityType).catch(() => null),
  ]);
  const chemicalsCount =
    (chemPayload?.metadata?.total_rows as number | undefined) ?? null;
  const foodsCount =
    (foodPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

  return (
    <EntityPageGate entityType={entityType} tabCount={4}>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId="foods"
        tabs={[
          {
            id: "foods",
            label: "Foods Exhibiting",
            count: foodsCount,
            content: (
              <BioactivityFoodsSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
          },
          {
            id: "chemicals",
            label: "Chemicals Measured",
            count: chemicalsCount,
            content: (
              <BioactivityChemicalsSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
          },
          {
            id: "diseases",
            label: "Diseases",
            content: <BioactivityDiseasesSection commonName={commonName} />,
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

BioactivityPage.displayName = "BioactivityPage";
export default BioactivityPage;

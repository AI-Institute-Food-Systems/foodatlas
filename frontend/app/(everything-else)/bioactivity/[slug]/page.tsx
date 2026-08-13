import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { buildTabs } from "@/components/entities/buildTabs";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
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
    <>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId={DEFAULT_TAB_ID[entityType]}
        tabs={buildTabs(entityType, {
          foods: {
            count: foodsCount,
            content: (
              <BioactivityFoodsSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
          },
          chemicals: {
            count: chemicalsCount,
            content: (
              <BioactivityChemicalsSection
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
          },
          diseases: {
            content: <BioactivityDiseasesSection commonName={commonName} />,
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

BioactivityPage.displayName = "BioactivityPage";
export default BioactivityPage;

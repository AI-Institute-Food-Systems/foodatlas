import { Suspense } from "react";
import { Metadata } from "next";
import { notFound } from "next/navigation";

import FoodCompositionTab from "@/components/entities/food/FoodCompositionTab";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import {
  getFoodBioactivities,
  getFoodCompositionData,
  getFoodMacroAndMicroData,
  getMetaData,
} from "@/utils/fetching";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface FoodPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: FoodPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  const metaData = await getMetaData(commonName, "food");
  if (!metaData) notFound();

  return {
    title: `${toTitleCase(metaData.common_name)} - Food Composition`,
    description: `Nutritional value of ${toTitleCase(
      metaData.common_name
    )}. Use evidence based molecular composition to help inform your food choices.`,
  };
}

const FoodPage = async ({ params }: FoodPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "food" as const;

  // Parallel best-effort count fetches for the tab badges. Failures fall
  // back to null so the badge silently hides instead of breaking the page.
  // Composition uses the same call as the table (default filters: all sources,
  // include unmeasured, no search) so the badge matches "Found N chemicals".
  // Counts from /food/composition/counts double-count multi-class chemicals.
  const [compPayload, nutritionData, bioPayload, metaPayload] = await Promise.all([
    getFoodCompositionData(
      commonName,
      1,
      ["fdc", "foodatlas", "dmd"],
      "",
      { column: "median_concentration", direction: "desc" },
      true,
      [],
      "default"
    ).catch(() => null),
    getFoodMacroAndMicroData(commonName).catch(() => null),
    getFoodBioactivities(commonName).catch(() => null),
    getMetaData(commonName, entityType).catch(() => null),
  ]);
  const anchorId = metaPayload?.id ?? null;
  const compositionCount =
    (compPayload?.metadata?.total_rows as number | undefined) ?? null;
  const nutritionCount = nutritionData
    ? Object.values(nutritionData).reduce((a, arr) => a + arr.length, 0)
    : null;
  const nutritionCategories = nutritionData
    ? Object.entries(nutritionData)
        .filter(([, items]) => items.length > 0)
        .map(([key, items]) => ({ key, count: items.length }))
    : [];
  const bioactivitiesCount =
    (bioPayload?.metadata?.total_rows as number | undefined) ?? null;

  return (
    <div>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId="composition"
        tabs={[
          {
            id: "composition",
            label: "Composition",
            count: compositionCount,
            content: (
              <FoodCompositionTab
                commonName={commonName}
                chemicalsCount={compositionCount}
                nutrientsCount={nutritionCount}
                nutritionCategories={nutritionCategories}
              />
            ),
          },
          {
            id: "bioactivities",
            label: "Bioactivities",
            count: bioactivitiesCount,
            content: (
              <div className="flex flex-col gap-12">
                <FoodBioactivitiesSection
                  commonName={commonName}
                  anchorId={anchorId}
                />
                <div className="border-t-2 border-double border-light-700/60" />
                <FoodInferredBioactivitiesSection commonName={commonName} />
              </div>
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
    </div>
  );
};

FoodPage.displayName = "FoodPage";

export default FoodPage;

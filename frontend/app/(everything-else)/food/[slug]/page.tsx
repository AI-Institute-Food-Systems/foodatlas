import { Suspense } from "react";
import { Metadata } from "next";
import { notFound } from "next/navigation";

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import FoodBioactivitiesTab from "@/components/entities/bioactivity/FoodBioactivitiesTab";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import EntityPageGate from "@/components/entities/EntityPageGate";
import {
  getFoodBioactivities,
  getFoodCompositionData,
  getFoodInferredBioactivities,
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
  // Bioactivities badge sums the direct (food→bioactivity) and inferred
  // (via chemicals-in-food) totals — same shape as the two tables rendered
  // in the tab, so the badge matches what the user actually sees.
  const [compPayload, bioPayload, inferredBioPayload, metaPayload] =
    await Promise.all([
      getFoodCompositionData(
        commonName,
        1,
        ["fdc", "foodatlas"],
        "",
        { column: "median_concentration", direction: "desc" },
        true,
        [],
        "default"
      ).catch(() => null),
      getFoodBioactivities(commonName).catch(() => null),
      getFoodInferredBioactivities(commonName).catch(() => null),
      getMetaData(commonName, entityType).catch(() => null),
    ]);
  const anchorId = metaPayload?.id ?? null;
  const compositionCount =
    (compPayload?.metadata?.total_rows as number | undefined) ?? null;
  const directBio =
    (bioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const inferredBio =
    (inferredBioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const bioactivitiesCount =
    directBio === null && inferredBio === null
      ? null
      : (directBio ?? 0) + (inferredBio ?? 0);

  return (
    <EntityPageGate entityType={entityType} tabCount={3}>
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
            content: <FoodCompositionSection commonName={commonName} />,
          },
          {
            id: "bioactivities",
            label: "Bioactivities",
            count: bioactivitiesCount,
            content: (
              <FoodBioactivitiesTab
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

FoodPage.displayName = "FoodPage";

export default FoodPage;

import { Suspense } from "react";
import { Metadata } from "next";

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import { ALL_SOURCE_VALUES } from "@/components/entities/food/compositionSources";
import FoodBioactivitiesTab from "@/components/entities/bioactivity/FoodBioactivitiesTab";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { requireEntity } from "@/components/entities/requireEntity";
import { buildTabs } from "@/components/entities/buildTabs";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import {
  getFoodBioactivities,
  getFoodCompositionData,
  getFoodInferredBioactivities,
  getMetaData,
} from "@/utils/fetching";
import JsonLd from "@/components/misc/JsonLd";
import TabSnapshot from "@/components/entities/shared/TabSnapshot";
import {
  apiEntityUrl,
  buildMetadata,
  canonicalUrl,
  fitDescription,
  fitTitle,
} from "@/utils/site";
import {
  entityBreadcrumbJsonLd,
  entityJsonLd,
} from "@/utils/structuredData";
import {
  bioactivityListSection,
  inferredBioactivitySection,
} from "@/utils/tabSnapshots";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface FoodPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: FoodPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  // Repeats the layout's check (cached) so a 404 doesn't get the slug as
  // its <title>. Past it, null metaData means the API did not answer; fall
  // back to the slug rather than fail the page.
  await requireEntity("food", slug);
  const metaData = await getMetaData(commonName, "food");
  const name = metaData?.common_name ?? commonName;

  return buildMetadata({
    title: fitTitle(toTitleCase(name), ": Food Composition"),
    description: fitDescription(
      `Chemical composition of ${toTitleCase(name)}: nutrients and other compounds with measured concentrations, each traced to a peer-reviewed source or database.`
    ),
    path: canonicalUrl("food", name),
    // The same entity as JSON, for anyone who wants the data not the page.
    jsonAlternate: metaData ? apiEntityUrl("food", metaData.id) : undefined,
  });
}

const FoodPage = async ({ params }: FoodPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "food" as const;

  // Parallel best-effort count fetches for the tab badges. Failures fall
  // back to null so the badge silently hides instead of breaking the page.
  // Composition uses the same call as the table (default filters: all sources,
  // include unmeasured, no search) so the badge matches "Found N chemicals",
  // and the table renders this response as its first page on the server.
  // (It listed fdc+foodatlas only, so PTFI-only rows were missing from the
  // badge until the client refetched.)
  // Bioactivities badge sums the direct (food→bioactivity) and inferred
  // (via chemicals-in-food) totals — same shape as the two tables rendered
  // in the tab, so the badge matches what the user actually sees.
  const [compPayload, bioPayload, inferredBioPayload, metaPayload] =
    await Promise.all([
      getFoodCompositionData(
        commonName,
        1,
        ALL_SOURCE_VALUES,
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
  // Must come from the same endpoint the inferred table renders, or the tab
  // badge disagrees with the row count underneath it — /food/efficacy counts
  // only pairs with a fittable Hill curve, which is a strict subset.
  const inferredBio =
    (inferredBioPayload?.metadata?.total_rows as number | undefined) ?? null;
  const bioactivitiesCount =
    directBio === null && inferredBio === null
      ? null
      : (directBio ?? 0) + (inferredBio ?? 0);

  // Server-only; rendered outright and in the Overview tab's snapshot slot,
  // so it's in the HTML whether or not the tab was opened. A factory, not
  // one shared element: RSC dedupes a repeated element into a single
  // reference, and SSR then fails on the shared <Suspense> ("reading
  // 'fallback'"), dropping the whole page to client rendering.
  const overview = () => (
    <Suspense fallback={<EntityOverviewPanelSuspense entityType={entityType} />}>
      <EntityOverviewPanel commonName={commonName} entityType={entityType} />
    </Suspense>
  );

  return (
    <>
      {metaPayload && <JsonLd data={entityJsonLd(entityType, metaPayload)} />}
      <JsonLd
        data={entityBreadcrumbJsonLd(
          entityType,
          toTitleCase(metaPayload?.common_name ?? commonName),
          metaPayload?.common_name ?? commonName
        )}
      />
      {/* Rendered in order with the page, from the metadata awaited above.
       * Behind its own Suspense it streamed last, so the h1 (the mobile
       * LCP element) painted only once the whole document was in. */}
      <HeaderSection
        commonName={commonName}
        entityType={entityType}
        metadata={metaPayload}
      />
      <EntityDetailLayout
        entityType={entityType}
        defaultTabId={DEFAULT_TAB_ID[entityType]}
        tabs={buildTabs(entityType, {
          composition: {
            count: compositionCount,
            content: (
              <FoodCompositionSection
                commonName={commonName}
                initialData={compPayload}
              />
            ),
          },
          bioactivities: {
            count: bioactivitiesCount,
            content: (
              <FoodBioactivitiesTab
                commonName={commonName}
                anchorId={anchorId}
              />
            ),
            snapshot: (
              <TabSnapshot
                sections={[
                  bioactivityListSection("Directly measured", "bioactivity", bioPayload),
                  inferredBioactivitySection(inferredBioPayload),
                ]}
              />
            ),
          },
          overview: { content: overview(), snapshot: overview() },
        })}
      />
    </>
  );
};

FoodPage.displayName = "FoodPage";

export default FoodPage;

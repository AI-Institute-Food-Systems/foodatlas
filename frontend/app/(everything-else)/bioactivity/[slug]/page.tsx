import { Metadata } from "next";
import { Suspense } from "react";

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { requireEntity } from "@/components/entities/requireEntity";
import { buildTabs } from "@/components/entities/buildTabs";
import { bioactivityDiseasesCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import {
  getBioactivityChemicals,
  getBioactivityDiseases,
  getBioactivityFoods,
  getMetaData,
} from "@/utils/fetching";
import JsonLd from "@/components/misc/JsonLd";
import type { Metadata as EntityMetadata } from "@/types/Metadata";
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
  bioactivityDiseasesSection,
  bioactivityListSection,
} from "@/utils/tabSnapshots";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface BioactivityPageProps {
  params: { slug: string };
}

// The counts come with the metadata, so they cost no extra request.
const bioactivityDescription = (
  name: string,
  meta: EntityMetadata | null
): string => {
  const title = toTitleCase(name);
  const counts =
    meta?.n_chemicals != null && meta?.n_foods != null
      ? `${meta.n_chemicals.toLocaleString("en-US")} chemicals and ${meta.n_foods.toLocaleString("en-US")} foods`
      : "Chemicals and foods";
  return `${title} bioactivity: ${counts} with measured activity, plus assay values and linked diseases, each traced to its source.`;
};

export async function generateMetadata({
  params,
}: BioactivityPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  // Repeats the layout's check (cached) so a 404 doesn't get the slug as
  // its <title>. Past it, null metaData means the API did not answer; fall
  // back to the slug rather than fail the page.
  await requireEntity("bioactivity", slug);
  const metaData = await getMetaData(commonName, "bioactivity");
  const name = metaData?.common_name ?? commonName;

  return buildMetadata({
    title: fitTitle(toTitleCase(name), " Bioactivity"),
    description: fitDescription(bioactivityDescription(name, metaData)),
    path: canonicalUrl("bioactivity", name),
    // The same entity as JSON, for anyone who wants the data not the page.
    jsonAlternate: metaData ? apiEntityUrl("bioactivity", metaData.id) : undefined,
  });
}

const BioactivityPage = async ({ params }: BioactivityPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "bioactivity" as const;

  const [chemPayload, foodPayload, metaPayload, diseasesCount, diseasesPayload] =
    await Promise.all([
      getBioactivityChemicals(commonName).catch(() => null),
      // No params = the backend's defaults (page 1, measurement_count desc),
      // which are also the table's, so this doubles as its first page.
      getBioactivityFoods(commonName).catch(() => null),
      getMetaData(commonName, entityType).catch(() => null),
      // Without this the Diseases badge placeholder pulses until the tab
      // is opened, since a tab publishes its count only once mounted.
      bioactivityDiseasesCount(commonName),
      // The Diseases tab's snapshot; same URL as the count, so cached.
      getBioactivityDiseases(commonName),
    ]);
  const chemicalsCount =
    (chemPayload?.metadata?.total_rows as number | undefined) ?? null;
  const foodsCount =
    (foodPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

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
          foods: {
            count: foodsCount,
            content: (
              <BioactivityFoodsSection
                commonName={commonName}
                anchorId={anchorId}
                initialPayload={foodPayload}
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
            snapshot: (
              <TabSnapshot
                sections={[
                  bioactivityListSection("Chemicals", "chemical", chemPayload),
                ]}
              />
            ),
          },
          diseases: {
            count: diseasesCount,
            content: <BioactivityDiseasesSection commonName={commonName} />,
            snapshot: (
              <TabSnapshot sections={[bioactivityDiseasesSection(diseasesPayload)]} />
            ),
          },
          overview: { content: overview(), snapshot: overview() },
        })}
      />
    </>
  );
};

BioactivityPage.displayName = "BioactivityPage";
export default BioactivityPage;

// Cache each rendered page at the edge (ISR). An empty generateStaticParams
// builds nothing up front but lets Next cache pages on first request; without
// it every request rendered from scratch. 1 h, not the fetches' 24 h: a page
// generated while the API failed renders its empty states, and only failed
// fetches are retried (Next caches 200s only), so the next hour heals it.
export const revalidate = 3600;
export const generateStaticParams = async () => [];

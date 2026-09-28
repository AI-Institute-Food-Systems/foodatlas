import { Metadata } from "next";
import { Suspense } from "react";

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import EntityDetailLayout from "@/components/entities/EntityDetailLayout";
import { requireEntity } from "@/components/entities/requireEntity";
import { buildTabs } from "@/components/entities/buildTabs";
import { bioactivityDiseasesCount } from "@/utils/tabCounts";
import { DEFAULT_TAB_ID } from "@/components/entities/entityTabs.config";
import EntityOverviewPanel from "@/components/entities/EntityOverviewPanel";
import EntityOverviewPanelSuspense from "@/components/entities/EntityOverviewPanelSuspense";
import {
  getBioactivityChemicals,
  getBioactivityFoods,
  getMetaData,
} from "@/utils/fetching";
import JsonLd from "@/components/misc/JsonLd";
import { apiEntityUrl, canonicalUrl } from "@/utils/site";
import { entityJsonLd } from "@/utils/structuredData";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface BioactivityPageProps {
  params: { slug: string };
}

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

  return {
    title: `${toTitleCase(name)} — Bioactivity Profile`,
    description: `Chemical measurements and food sources for the ${toTitleCase(name)} bioactivity.`,
    // The same entity as JSON, for anyone who wants the data not the page.
    alternates: {
      canonical: canonicalUrl("bioactivity", name),
      ...(metaData && {
        types: { "application/json": apiEntityUrl("bioactivity", metaData.id) },
      }),
    },
  };
}

const BioactivityPage = async ({ params }: BioactivityPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "bioactivity" as const;

  const [chemPayload, foodPayload, metaPayload, diseasesCount] =
    await Promise.all([
      getBioactivityChemicals(commonName).catch(() => null),
      getBioactivityFoods(commonName).catch(() => null),
      getMetaData(commonName, entityType).catch(() => null),
      // Without this the Diseases badge placeholder pulses until the tab
      // is opened, since a tab publishes its count only once mounted.
      bioactivityDiseasesCount(commonName),
    ]);
  const chemicalsCount =
    (chemPayload?.metadata?.total_rows as number | undefined) ?? null;
  const foodsCount =
    (foodPayload?.metadata?.total_rows as number | undefined) ?? null;
  const anchorId = metaPayload?.id ?? null;

  return (
    <>
      {metaPayload && <JsonLd data={entityJsonLd(entityType, metaPayload)} />}
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
            count: diseasesCount,
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

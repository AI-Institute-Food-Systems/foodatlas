import { Metadata } from "next";
import { notFound } from "next/navigation";
import { Suspense } from "react";

import BioactivityChemicalsSection from "@/components/entities/bioactivity/BioactivityChemicalsSection";
import BioactivityFoodsSection from "@/components/entities/bioactivity/BioactivityFoodsSection";
import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import MetainformationSection from "@/components/entities/MetainformationSection";
import MetainformationSuspense from "@/components/entities/MetainformationSuspense";
import { getMetaData } from "@/utils/fetching";
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
    description: `Chemical measurements, food sources, and disease associations for the ${toTitleCase(
      metaData.common_name
    )} bioactivity.`,
  };
}

const BioactivityPage = async ({ params }: BioactivityPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "bioactivity";

  return (
    <div>
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      <div className="mt-12 flex flex-col gap-20">
        <Suspense fallback={<MetainformationSuspense entityType={entityType} />}>
          <MetainformationSection
            commonName={commonName}
            entityType={entityType}
          />
        </Suspense>
        <BioactivityChemicalsSection commonName={commonName} />
        <BioactivityFoodsSection commonName={commonName} />
        <BioactivityDiseasesSection commonName={commonName} />
      </div>
    </div>
  );
};

BioactivityPage.displayName = "BioactivityPage";
export default BioactivityPage;

import { Suspense } from "react";
import { Metadata } from "next";

import ChemicalCompositionSection from "@/components/entities/chemical/ChemicalCompositionSection";
import ChemicalCorrelationSection from "@/components/entities/chemical/ChemicalCorrelationSection";
import MetainformationSection from "@/components/entities/MetainformationSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import MetainformationSuspense from "@/components/entities/MetainformationSuspense";
import ChemicalCompositionSectionSuspense from "@/components/entities/chemical/ChemicalCompositionSectionSuspense";
import { decodeSpace, toTitleCase } from "@/utils/utils";

interface ChemicalPageProps {
  params: { slug: string };
}

export async function generateMetadata({
  params,
}: ChemicalPageProps): Promise<Metadata> {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));

  return {
    title: `${toTitleCase(commonName)} in Foods - Evidence Based Database`,
    description: `Discover which foods contain ${toTitleCase(
      commonName
    )} and how it impacts your health.`,
  };
}

const ChemicalPage = async ({ params }: ChemicalPageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "chemical";

  return (
    <div>
      {/* header */}
      <Suspense fallback={<HeaderSectionSuspense entityType={entityType} />}>
        <HeaderSection commonName={commonName} entityType={entityType} />
      </Suspense>
      {/* content */}
      <div className="mt-12 flex flex-col gap-20">
        {/* meta information */}
        <Suspense
          fallback={<MetainformationSuspense entityType={entityType} />}
        >
          <MetainformationSection
            commonName={commonName}
            entityType={entityType}
          />
        </Suspense>
        {/* composition */}
        <Suspense fallback={<ChemicalCompositionSectionSuspense />}>
          <ChemicalCompositionSection commonName={commonName} />
        </Suspense>
        {/* correlation */}
        <ChemicalCorrelationSection commonName={commonName} />
      </div>
    </div>
  );
};

ChemicalPage.displayName = "ChemicalPage";

export default ChemicalPage;

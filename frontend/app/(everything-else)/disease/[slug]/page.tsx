import { Metadata } from "next";
import { Suspense } from "react";

import DiseaseCorrelationsSection from "@/components/entities/disease/DiseaseCorrelationsSection";
import HeaderSection from "@/components/entities/HeaderSection";
import MetainformationSection from "@/components/entities/MetainformationSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import MetainformationSuspense from "@/components/entities/MetainformationSuspense";
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

  return {
    title: `${toTitleCase(metaData?.common_name)} and Your Health`,
    description: `Evidence-based correlations between ${toTitleCase(
      metaData?.common_name
    )} and the foods that contain it.`,
  };
}

const DiseasePage = async ({ params }: DiseasePageProps) => {
  const { slug } = params;
  const commonName = decodeSpace(decodeURIComponent(slug));
  const entityType = "disease";

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
        {/* correlations */}
        {/* <Suspense fallback={<DiseaseCorrelationsSection commonName={commonName} />}> */}
        <DiseaseCorrelationsSection commonName={commonName} />
        {/* </Suspense> */}
      </div>
    </div>
  );
};

DiseasePage.displayName = "DiseasePage";
export default DiseasePage;

import { Suspense } from "react";
import { Metadata } from "next";

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import HeaderSection from "@/components/entities/HeaderSection";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import MetainformationSection from "@/components/entities/MetainformationSection";
import MetainformationSuspense from "@/components/entities/MetainformationSuspense";
import MacrosAndMicrosSection from "@/components/entities/food/MacrosAndMicrosSection";
import MacrosAndMicrosSuspense from "@/components/entities/food/MacrosAndMicrosSuspense";
import { getMetaData } from "@/utils/fetching";
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
  const entityType = "food";

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
        {/* macros & micros */}
        <Suspense fallback={<MacrosAndMicrosSuspense />}>
          <MacrosAndMicrosSection commonName={commonName} />
        </Suspense>
        {/* composition */}
        <FoodCompositionSection commonName={commonName} />
      </div>
    </div>
  );
};

FoodPage.displayName = "FoodPage";

export default FoodPage;

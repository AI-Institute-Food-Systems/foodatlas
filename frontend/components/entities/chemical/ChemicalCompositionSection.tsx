import dynamic from "next/dynamic";
import NoConcentrationComposition from "@/components/entities/chemical/NoConcentrationComposition";
import Heading from "@/components/basic/Heading";

const ConcentrationCompositionPlot = dynamic(
  () => import("@/components/entities/chemical/ConcentrationCompositionPlot"),
  { ssr: false }
);
import { getChemicalCompositionData, getMetaData } from "@/utils/fetching";
import { capitalizeFirstLetter } from "@/utils/utils";

interface ChemicalCompositionSectionProps {
  commonName: string;
}

const ChemicalCompositionSection = async ({
  commonName,
}: ChemicalCompositionSectionProps) => {
  const compositionData = await getChemicalCompositionData(commonName);
  const metaData = await getMetaData(commonName, "chemical");

  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        {`Foods containing ${capitalizeFirstLetter(metaData?.common_name ?? "")}`}
      </Heading>
      <div className="flex flex-col gap-7">
        {/* with concentration section */}
        <div className="flex flex-col gap-4">
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-base font-medium"
            >
              Known Concentration Value
            </Heading>
            <p className="text-light-500">
              Foods containing this chemical with known concentration
            </p>
          </div>
          <ConcentrationCompositionPlot
            data={compositionData?.with_concentrations}
            chemicalName={metaData?.id}
          />
        </div>
        {/* without concentration section */}
        <div className="flex flex-col gap-4">
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-base font-medium"
            >
              Unknown Concentration Value
            </Heading>
            <p className="text-light-500">
              Foods containing this chemical of unknown concentration
            </p>
          </div>
          <NoConcentrationComposition
            data={compositionData?.without_concentrations}
            chemicalName={metaData?.id}
          />
        </div>
      </div>
    </div>
  );
};

ChemicalCompositionSection.displayName = "ChemicalCompositionSection";

export default ChemicalCompositionSection;

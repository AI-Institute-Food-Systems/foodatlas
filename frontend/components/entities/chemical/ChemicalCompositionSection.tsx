import dynamic from "next/dynamic";
import NoConcentrationComposition from "@/components/entities/chemical/NoConcentrationComposition";
import Heading from "@/components/basic/Heading";

const ConcentrationCompositionPlot = dynamic(
  () => import("@/components/entities/chemical/ConcentrationCompositionPlot"),
  { ssr: false }
);
import { getChemicalCompositionData, getMetaData } from "@/utils/fetching";

interface ChemicalCompositionSectionProps {
  commonName: string;
}

const ChemicalCompositionSection = async ({
  commonName,
}: ChemicalCompositionSectionProps) => {
  const compositionData = await getChemicalCompositionData(commonName);
  const metaData = await getMetaData(commonName, "chemical");

  // When neither section has rows, the two per-section messages would state
  // the absence twice from opposite directions. Say it once instead.
  const withConc = compositionData?.with_concentrations ?? [];
  const withoutConc = compositionData?.without_concentrations ?? [];
  if (withConc.length === 0 && withoutConc.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        <span className="capitalize">{commonName}</span> is not recorded in any
        food in the current data.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-7">
      <div className="flex flex-col gap-7">
        {/* with concentration section */}
        <div className="flex flex-col gap-4">
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-sm font-medium"
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
              className="text-light-300 font-mono text-sm font-medium"
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

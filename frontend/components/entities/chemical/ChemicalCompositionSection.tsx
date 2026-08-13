import ChemicalCompositionTable from "@/components/entities/chemical/ChemicalCompositionTable";
import { getChemicalCompositionData, getMetaData } from "@/utils/fetching";

interface ChemicalCompositionSectionProps {
  commonName: string;
}

const ChemicalCompositionSection = async ({
  commonName,
}: ChemicalCompositionSectionProps) => {
  const compositionData = await getChemicalCompositionData(commonName);
  const metaData = await getMetaData(commonName, "chemical");

  const withConc = compositionData?.with_concentrations ?? [];
  const withoutConc = compositionData?.without_concentrations ?? [];

  // Both buckets empty means the chemical genuinely isn't attested in any
  // food — say that once, rather than rendering an empty table whose
  // filter chrome implies the user filtered the rows away themselves.
  if (withConc.length === 0 && withoutConc.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        <span className="capitalize">{commonName}</span> is not recorded in any
        food in the current data.
      </p>
    );
  }

  return (
    <ChemicalCompositionTable
      withConcentrations={withConc}
      withoutConcentrations={withoutConc}
      chemicalId={metaData?.id}
    />
  );
};

ChemicalCompositionSection.displayName = "ChemicalCompositionSection";

export default ChemicalCompositionSection;

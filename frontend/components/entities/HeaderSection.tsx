import Badge from "@/components/basic/Badge";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import Heading from "@/components/basic/Heading";
import { getMetaData } from "@/utils/fetching";

const colorScheme = {
  food: "text-amber-600 border-amber-600 bg-amber-600/10 shadow-amber-600/50",
  chemical: "text-cyan-600 border-cyan-600 bg-cyan-600/10 shadow-cyan-600/50",
  disease:
    "text-purple-500 border-purple-500 bg-purple-500/10 shadow-purple-500/50",
};

const icon = {
  food: <FoodIcon color="#d97706" />,
  chemical: <ChemicalIcon color="#0891b2" />,
  disease: <DiseaseIcon color="#a855f7" />,
};

interface HeaderSectionProps {
  commonName: string;
  entityType: "food" | "chemical" | "disease";
}

const HeaderSection = async ({
  entityType,
  commonName,
}: HeaderSectionProps) => {
  const data = await getMetaData(commonName, entityType);

  return (
    <div>
      {/* badge & id */}
      <div className="flex items-center gap-3">
        <Badge
          color={colorScheme[entityType]}
          leftIcon={icon[entityType]}
          size="sm"
        >
          {entityType}
        </Badge>
        <div className="border-l h-6 border-light-500" />
        <span className="font-mono font-medium italic text-sm text-light-300">
          FoodAtlas {data.id}
        </span>
      </div>
      {/* name */}
      <div className="mt-5">
        <Heading
          type="h1"
          className="capitalize text-6xl font-semibold break-all"
        >
          {commonName}
        </Heading>
      </div>
    </div>
  );
};

HeaderSection.displayName = "HeaderSection";

export default HeaderSection;

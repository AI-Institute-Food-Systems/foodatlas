import Badge from "@/components/basic/Badge";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import LoadingCard from "@/components/basic/LoadingCard";

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

interface HeaderSectionSuspenseProps {
  entityType: "food" | "chemical" | "disease";
}

const HeaderSectionSuspense = async ({
  entityType,
}: HeaderSectionSuspenseProps) => {
  return (
    <div>
      {/* badge & id */}
      <div className="flex items-center gap-3">
        <Badge
          color={colorScheme[entityType]}
          leftIcon={icon[entityType]}
          size="md"
        >
          {entityType}
        </Badge>
        <div className="border-l h-6 border-light-400" />
        <span className="flex items-center gap-2 font-mono font-medium italic text-sm text-light-300">
          FoodAtlas <LoadingCard className="w-16 h-6" />
        </span>
      </div>
      <div className="mt-5">
        <LoadingCard className="h-[3.9rem] w-56" />
      </div>
    </div>
  );
};

HeaderSectionSuspense.displayName = "HeaderSectionSuspense";

export default HeaderSectionSuspense;

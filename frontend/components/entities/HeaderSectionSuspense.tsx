import Badge from "@/components/basic/Badge";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import BioactivityIcon from "@/components/icons/BioactivityIcon";
import LoadingCard from "@/components/basic/LoadingCard";

const colorScheme = {
  food: "text-amber-600 border-amber-600 bg-amber-600/10 shadow-amber-600/50",
  chemical: "text-cyan-600 border-cyan-600 bg-cyan-600/10 shadow-cyan-600/50",
  disease:
    "text-purple-500 border-purple-500 bg-purple-500/10 shadow-purple-500/50",
  bioactivity:
    "text-emerald-500 border-emerald-500 bg-emerald-500/10 shadow-emerald-500/50",
};

const icon = {
  food: <FoodIcon color="#d97706" />,
  chemical: <ChemicalIcon color="#0891b2" />,
  disease: <DiseaseIcon color="#a855f7" />,
  bioactivity: <BioactivityIcon color="#10b981" />,
};

interface HeaderSectionSuspenseProps {
  entityType: "food" | "chemical" | "disease" | "bioactivity";
}

const HeaderSectionSuspense = ({
  entityType,
}: HeaderSectionSuspenseProps) => {
  return (
    <div>
      <div className="relative flex items-center gap-x-4 gap-y-2 flex-wrap pr-16 md:pr-24">
        <Badge
          color={colorScheme[entityType]}
          leftIcon={icon[entityType]}
          size="md"
        >
          {entityType}
        </Badge>
        <LoadingCard className="h-9 md:h-10 w-56" />
        <span className="absolute right-0 top-1/2 -translate-y-1/2 flex items-baseline gap-1.5 whitespace-nowrap">
          <span className="font-mono italic text-[10px] uppercase tracking-[0.12em] text-light-500">
            FoodAtlas ID
          </span>
          <LoadingCard className="w-14 h-3" />
        </span>
      </div>
    </div>
  );
};

HeaderSectionSuspense.displayName = "HeaderSectionSuspense";

export default HeaderSectionSuspense;

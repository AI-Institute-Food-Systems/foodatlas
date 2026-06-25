import Badge from "@/components/basic/Badge";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import BioactivityIcon from "@/components/icons/BioactivityIcon";
import Heading from "@/components/basic/Heading";
import EntityAmbiguityBanner from "@/components/entities/EntityAmbiguityBanner";
import { getMetaData } from "@/utils/fetching";

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

interface HeaderSectionProps {
  commonName: string;
  entityType: "food" | "chemical" | "disease" | "bioactivity";
}

const HeaderSection = async ({
  entityType,
  commonName,
}: HeaderSectionProps) => {
  const data = await getMetaData(commonName, entityType);

  return (
    <div>
      {/* one-line header band: badge left, entity name beside it, FoodAtlas
       * id pinned right. items-center vertically aligns the smaller badge
       * with the H1's optical center (not horizontally centered). */}
      <div className="relative flex items-center gap-x-4 gap-y-2 flex-wrap pr-16 md:pr-24">
        <Badge
          color={colorScheme[entityType]}
          leftIcon={icon[entityType]}
          size="md"
        >
          {entityType}
        </Badge>
        <Heading
          type="h1"
          className="capitalize text-3xl md:text-4xl font-semibold break-words leading-none"
        >
          {commonName}
        </Heading>
        <span className="absolute right-0 top-1/2 -translate-y-1/2 font-mono italic text-xs text-light-300 whitespace-nowrap">
          {data?.id ?? "—"}
        </span>
      </div>
      <EntityAmbiguityBanner
        entityType={entityType}
        siblings={data?.ambiguity_siblings}
      />
    </div>
  );
};

HeaderSection.displayName = "HeaderSection";

export default HeaderSection;

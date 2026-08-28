import Badge from "@/components/basic/Badge";
import FoodIcon from "@/components/icons/FoodIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import BioactivityIcon from "@/components/icons/BioactivityIcon";
import Skeleton from "@/components/basic/Skeleton";

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
      {/* Two lines: entity badge over the name, FoodAtlas id pinned to the
       * badge's line. The badge and the H1 used to share one row, which
       * read as a label glued to the front of the title and left the name
       * competing with the id for horizontal room on narrow viewports.
       *
       * HeaderSectionSuspense mirrors this markup exactly, including the
       * name-row height. Any change here has to land there too, or the
       * handoff from skeleton to real header moves the page — which is
       * the whole reason the ambiguity warning became a chip. */}
      <div className="flex items-center justify-between gap-4">
        <Badge
          color={colorScheme[entityType]}
          leftIcon={icon[entityType]}
          size="md"
        >
          {entityType}
        </Badge>
        <span className="flex items-baseline gap-1.5 whitespace-nowrap">
          <span className="font-mono italic text-[10px] uppercase tracking-[0.12em] text-light-500">
            FoodAtlas ID
          </span>
          <Skeleton className="w-14 h-3" />
        </span>
      </div>
      <div className="mt-2 flex items-center gap-3 flex-wrap">
        {/* Exactly the H1's box: text-3xl/text-4xl at leading-none is
         * 1.875rem then 2.25rem. h-9/h-10 was 6px taller at both
         * breakpoints, so the title row shrank on handoff. No ambiguity
         * chip here — it is optional, and reserving room for one would
         * move the page on the pages that do not have it. */}
        <Skeleton className="h-[1.875rem] md:h-9 w-56" />
      </div>
    </div>
  );
};

HeaderSectionSuspense.displayName = "HeaderSectionSuspense";

export default HeaderSectionSuspense;

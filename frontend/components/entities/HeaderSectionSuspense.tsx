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
      {/* Left column: the entity badge over the name. Right: the FoodAtlas
       * id, centred against the pair. The badge and the H1 used to share
       * one row, which read as a label glued to the front of the title and
       * left the name competing with the id for horizontal room on narrow
       * viewports.
       *
       * HeaderSectionSuspense mirrors this markup exactly, including the
       * name-row height. Any change here has to land there too, or the
       * handoff from skeleton to real header moves the page. */}
      <div className="flex items-center justify-between">
        <div className="flex flex-col gap-0">
          {/* No placeholder for the ambiguity affordance: it sits on this
           * line beside the Badge, which is taller, so it adds no height
           * to reserve. We could not reserve it anyway — the metadata
           * that says whether this entity is ambiguous has not arrived
           * when the skeleton paints. */}
          <div className="flex items-center gap-2">
            <Badge
              color={colorScheme[entityType]}
              leftIcon={icon[entityType]}
              size="sm"
            >
              {entityType}
            </Badge>
          </div>
          <div className="mt-3 flex items-center gap-3 flex-wrap">
            {/* Exactly the H1's box, which is NOT what the class list
             * suggests: `leading-none` is unprefixed, so from md up the
             * responsive `md:text-4xl` — which carries its own
             * line-height: 2.5rem — wins and the H1 renders 40px, not the
             * 36px "text-4xl at leading-none" implies. Measured: 30px
             * below md, 40px at and above it. Reserving 36 there is how
             * the tab strip and the whole card sat 4px low until the real
             * header arrived. */}
            <Skeleton className="h-[1.875rem] md:h-10 w-56" />
          </div>
        </div>
        <div className="flex flex-col items-end gap-2">
          <span className="flex items-baseline gap-1.5 whitespace-nowrap">
            <span className="font-mono italic text-[10px] uppercase tracking-[0.12em] text-light-500">
              FoodAtlas ID
            </span>
            {/* Width sized from the rendered id, not guessed: mono
             * text-xs is ~7.2px per character and ids are 7 chars for
             * 118k entities, 6 for 90k — 43-50px. w-14 reserved 56 and,
             * because the column is right-aligned, the surplus pushed the
             * "FoodAtlas ID" label 12px left until the real id arrived;
             * w-12 leaves at most 4px either way.
             *
             * h-3 rather than the text's h-4 because the row is
             * items-baseline: a box has no text baseline, so it aligns by
             * its bottom edge. 12px puts that edge where the 16px line's
             * baseline sits; 16px pushes the whole row 2px down. */}
            <Skeleton className="w-12 h-3" />
          </span>
        </div>
      </div>
    </div>
  );
};

HeaderSectionSuspense.displayName = "HeaderSectionSuspense";

export default HeaderSectionSuspense;

import { MdInfoOutline } from "react-icons/md";

import Badge from "@/components/basic/Badge";
import Link from "@/components/basic/Link";
import BioactivityIcon from "@/components/icons/BioactivityIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import FoodIcon from "@/components/icons/FoodIcon";
import { getMetaData } from "@/utils/fetching";
import { encodeSpace } from "@/utils/utils";

import type { EntityType } from "./EntityTabs";

interface Props {
  commonName: string;
  entityType: EntityType;
}

const COLOR: Record<EntityType, string> = {
  food: "text-amber-600 border-amber-600 bg-amber-600/10 shadow-amber-600/50",
  chemical:
    "text-cyan-600 border-cyan-600 bg-cyan-600/10 shadow-cyan-600/50",
  disease:
    "text-purple-500 border-purple-500 bg-purple-500/10 shadow-purple-500/50",
  bioactivity:
    "text-emerald-500 border-emerald-500 bg-emerald-500/10 shadow-emerald-500/50",
};

const ICON: Record<EntityType, React.ReactNode> = {
  food: <FoodIcon color="#d97706" />,
  chemical: <ChemicalIcon color="#0891b2" />,
  disease: <DiseaseIcon color="#a855f7" />,
  bioactivity: <BioactivityIcon color="#10b981" />,
};

// Compact entity bar. Rendered inside <StickyOnScrollPast> so it only
// appears when the primary HeaderSection has scrolled out of view.
// This file just renders the bar's inner content — the wrapper owns
// positioning + show/hide behavior.
const EntitySubnavbar = async ({ commonName, entityType }: Props) => {
  const data = await getMetaData(commonName, entityType);
  if (!data) return null;
  const siblings = data.ambiguity_siblings ?? [];
  const hasAmbiguity = siblings.length > 0;

  return (
    <div
      className={
        "border-b border-light-50/[0.08] bg-[#0a0a09]/70 backdrop-blur-2xl saturate-150 " +
        "px-4 md:px-24"
      }
    >
      <div className="mx-auto max-w-5xl h-10 md:h-11 flex items-center gap-3 min-w-0">
        <Badge color={COLOR[entityType]} leftIcon={ICON[entityType]} size="md">
          {entityType}
        </Badge>
        <span className="capitalize text-sm md:text-base font-semibold text-light-100 truncate min-w-0 flex-1">
          {commonName}
        </span>

        {hasAmbiguity && (
          <div className="relative flex-shrink-0 group">
            <MdInfoOutline
              className="size-4 text-amber-400 cursor-help"
              aria-label={`${siblings.length} other entities share this name`}
            />
            <div className="absolute right-0 top-full mt-1 hidden group-hover:block group-focus-within:block z-50 min-w-56 rounded-md border border-light-50/[0.15] bg-light-950/95 backdrop-blur-xl shadow-xl shadow-black/40 p-3">
              <p className="font-mono italic text-[10px] uppercase tracking-wider text-amber-400 mb-2">
                Also used for
              </p>
              <ul className="flex flex-col gap-1.5 text-xs text-light-200">
                {siblings.map((s) => (
                  <li key={s.foodatlas_id}>
                    <Link
                      href={`/${entityType}/${encodeURIComponent(
                        encodeSpace(s.common_name),
                      )}`}
                    >
                      <span className="capitalize">{s.common_name}</span>
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        )}

        <span className="hidden md:flex items-baseline gap-1.5 flex-shrink-0 whitespace-nowrap">
          <span className="font-mono italic text-[10px] uppercase tracking-[0.12em] text-light-500">
            FoodAtlas ID
          </span>
          <span className="font-mono italic text-xs text-light-300">
            {data.id}
          </span>
        </span>
      </div>
    </div>
  );
};

EntitySubnavbar.displayName = "EntitySubnavbar";

export default EntitySubnavbar;

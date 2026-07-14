import { MdInfoOutline } from "react-icons/md";

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

// Hex values (same tokens the icons + HeaderSection use) so the badge
// styles work via inline `style` — bypasses any Tailwind class
// merging that was making the food (amber) badge render wrong.
const HEX: Record<EntityType, string> = {
  food: "#d97706", // amber-600
  chemical: "#0891b2", // cyan-600
  disease: "#a855f7", // purple-500
  bioactivity: "#10b981", // emerald-500
};

const ICON: Record<EntityType, React.ReactNode> = {
  food: <FoodIcon color={HEX.food} />,
  chemical: <ChemicalIcon color={HEX.chemical} />,
  disease: <DiseaseIcon color={HEX.disease} />,
  bioactivity: <BioactivityIcon color={HEX.bioactivity} />,
};

// Convert a #rrggbb into `rgba(r, g, b, alpha)` for the 10 %-tinted
// background — same visual as `bg-<color>/10` in HeaderSection but
// applied via inline `background` so it survives any class merge.
const withAlpha = (hex: string, alpha: number): string => {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
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
        // Match the primary Navbar's translucency: bg-[#0a0a09]/30,
        // backdrop-blur-2xl, saturate-200.
        "border-b border-light-50/[0.08] bg-[#0a0a09]/30 backdrop-blur-2xl saturate-200 " +
        "px-4 md:px-24"
      }
    >
      <div className="mx-auto max-w-5xl h-10 md:h-11 flex items-center gap-3 min-w-0">
        <div
          className="rounded-full flex items-center gap-1.5 px-2.5 py-1 md:px-3.5 md:py-1 text-[0.72rem] md:text-[0.85rem] font-mono font-medium md:font-semibold capitalize border-[1.5px] whitespace-nowrap shadow-[inset_0_1px_6px_rgba(0,0,0,0.5)]"
          style={{
            color: HEX[entityType],
            borderColor: HEX[entityType],
            backgroundColor: withAlpha(HEX[entityType], 0.1),
          }}
        >
          {ICON[entityType]}
          {entityType}
        </div>
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

import LoadingCard from "@/components/basic/LoadingCard";

import type { EntityType } from "./EntityTabs";

interface Props {
  entityType: EntityType;
}

const STRIPE: Record<EntityType, string> = {
  food: "bg-amber-500",
  chemical: "bg-cyan-600",
  disease: "bg-purple-500",
  bioactivity: "bg-emerald-500",
};

// Skeleton mirrors EntityOverviewPanel's two-card grid layout: identifiers
// card + taxonomy card on lg+ (single card on bioactivity since there's no
// taxonomy). Each card is a thin accent stripe on top + a few placeholder rows.
const EntityOverviewPanelSuspense = ({ entityType }: Props) => {
  const hasTaxonomy = entityType !== "bioactivity";
  return (
    <div
      className={
        hasTaxonomy
          ? "grid grid-cols-1 lg:grid-cols-2 gap-6 items-start"
          : "grid grid-cols-1 gap-6"
      }
    >
      <SkeletonCard stripe={STRIPE[entityType]} />
      {hasTaxonomy && <SkeletonCard stripe={STRIPE[entityType]} />}
    </div>
  );
};

const SkeletonCard = ({ stripe }: { stripe: string }) => (
  <div className="rounded-lg overflow-hidden border-[1.5px] border-light-50/[0.08] bg-light-950 shadow-[inset_0_5px_8px_rgba(255,249,242,0.02)]">
    <div className={`h-[3px] ${stripe}`} aria-hidden />
    <div className="p-5 md:p-6 flex flex-col gap-5">
      <div className="flex flex-col gap-2">
        <LoadingCard className="h-3 w-24" />
        <LoadingCard className="h-8 w-3/4" />
      </div>
      <div className="border-t border-dashed border-light-700/70" />
      <div className="flex flex-col gap-4">
        <LoadingCard className="h-3 w-16" />
        <LoadingCard className="h-4 w-1/2" />
        <LoadingCard className="h-3 w-16" />
        <LoadingCard className="h-4 w-2/3" />
      </div>
    </div>
  </div>
);

EntityOverviewPanelSuspense.displayName = "EntityOverviewPanelSuspense";

export default EntityOverviewPanelSuspense;

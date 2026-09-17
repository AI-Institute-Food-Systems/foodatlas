import Skeleton from "@/components/basic/Skeleton";

import type { EntityType } from "./EntityTabs";

// Skeleton mirrors EntityOverviewPanel's naked two-col layout: the
// panel renders inside a tab Card (EntityTabs wraps it), so we do NOT
// add another card shell — that would double-border. Each column mimics
// OverviewCardCatalog's chip-labeled sections + Field rows.
const EntityOverviewPanelSuspense = ({ entityType }: { entityType: EntityType }) => {
  const hasTaxonomy = entityType !== "bioactivity";
  return (
    <div
      className={
        hasTaxonomy
          ? "grid grid-cols-1 lg:grid-cols-2 gap-x-10 gap-y-8 items-start"
          : "grid grid-cols-1 gap-y-8"
      }
    >
      <IdentifiersSkeleton />
      {hasTaxonomy && <TaxonomySkeleton />}
    </div>
  );
};

// Cream chip poking out the left edge — matches the Section header in
// OverviewCardCatalog. `-ml-3` mirrors the negative offset that puts
// the chip on the container's left margin.
const SectionChipSkeleton = ({ width = "w-20" }: { width?: string }) => (
  // The cream tone rather than the default: this stands in for an
  // actually-cream element, so a dark placeholder would read as a
  // different component rather than as that chip, pending.
  <Skeleton
    tone="cream"
    className={`self-start -ml-3 h-[18px] rounded-r-md ${width}`}
  />
);

// Term/value row — mono uppercase term (w-20 shrink-0) + longer value.
const FieldRowSkeleton = ({ valueClass = "w-1/2" }: { valueClass?: string }) => (
  <div className="flex gap-3 items-center">
    <Skeleton className="h-3 w-16 shrink-0" />
    <Skeleton className={`h-4 ${valueClass}`} />
  </div>
);

const IdentifiersSkeleton = () => (
  <div className="flex flex-col gap-5">
    <section className="flex flex-col gap-3">
      <SectionChipSkeleton width="w-24" />
      <div className="flex flex-col gap-2.5">
        <FieldRowSkeleton valueClass="w-24" />
        <FieldRowSkeleton valueClass="w-32" />
        <FieldRowSkeleton valueClass="w-28" />
      </div>
    </section>
    <section className="flex flex-col gap-3 pt-5 border-t-2 border-double border-light-700/60">
      <SectionChipSkeleton width="w-28" />
      <Skeleton className="h-4 w-2/3" />
    </section>
    <section className="flex flex-col gap-3 pt-5 border-t-2 border-double border-light-700/60">
      <SectionChipSkeleton width="w-20" />
      <div className="flex flex-wrap gap-1">
        <Skeleton className="h-5 w-16 rounded-full" />
        <Skeleton className="h-5 w-20 rounded-full" />
        <Skeleton className="h-5 w-14 rounded-full" />
        <Skeleton className="h-5 w-24 rounded-full" />
      </div>
    </section>
  </div>
);

// TaxonomySection renders a tree — labeled chip up top, then indented
// node lines. Approximate the tree shape with 3-4 lines at increasing
// left offsets so it reads as a hierarchy.
const TaxonomySkeleton = () => (
  <div className="flex flex-col gap-3">
    <SectionChipSkeleton width="w-24" />
    <div className="flex flex-col gap-1.5 mt-2">
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-4 w-32 ml-3" />
      <Skeleton className="h-4 w-28 ml-6" />
      <Skeleton className="h-4 w-40 ml-9" />
    </div>
    <div className="mt-2 flex items-center gap-2 flex-wrap">
      <Skeleton className="h-3 w-24" />
      <Skeleton className="h-6 w-28 rounded-full" />
    </div>
  </div>
);

EntityOverviewPanelSuspense.displayName = "EntityOverviewPanelSuspense";

export default EntityOverviewPanelSuspense;

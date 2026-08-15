import Card from "@/components/basic/Card";
import Skeleton from "@/components/basic/Skeleton";
import { TableSkeleton } from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import {
  DEFAULT_TAB_ID,
  ENTITY_TABS,
  TAB_BADGE_W,
  TAB_STRIP_FITS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

// Literal classes, not interpolated — Tailwind only emits what it can
// see. Ragged widths so the stack reads like filter labels.
const FILTER_SKELETON_WIDTHS = [
  "w-24",
  "w-20",
  "w-28",
  "w-16",
  "w-24",
  "w-20",
] as const;

const SHELL_COLUMNS: SkeletonColumn[] = [
  { key: "name", width: "w-[50%]" },
  { key: "metric-a", width: "w-[25%]", align: "right" },
  { key: "metric-b", width: "w-[25%]", align: "right" },
];

interface Props {
  entityType: EntityType;
}

// Mirrors <EntityDetailLayout /> + the client tables' isLoading render as
// closely as possible, so:
//   1. The route-level fallback gives instant feedback on hard reload.
//   2. When the SSR shell replaces it, the body doesn't visibly change.
//   3. When the client fetch resolves, data replaces the same rows in
//      place — no "loading -> data -> loading -> data" perception.
//
// The tab strip is not a skeleton: ids, labels and ordering are static
// and come from the same config the real page builds its tabs from, so
// the chips render for real (default tab selected, exactly as the SSR
// shell will paint it) and only the pending count badges are placeholders.
// This is what closes the old `tabCount` drift, where this file guessed a
// number that disagreed with the real page on three routes out of four.
//
// If EntityTabs' chip styling changes, mirror it here so the handoff
// stays seamless.
const EntityDetailLayoutSuspense = ({ entityType }: Props) => {
  const tabs = ENTITY_TABS[entityType];
  const defaultTabId = DEFAULT_TAB_ID[entityType];
  const fits = TAB_STRIP_FITS[entityType];

  return (
    <div className="mt-6">
      <section className="min-w-0">
        {/* Select vs chip strip, switched at the width the real strip
          * stops overflowing — see TAB_STRIP_FITS. Assuming `sm` here
          * meant the shell drew a strip wherever the live page was still
          * drawing a select, which on a chemical page was 640px–1200px. */}
        <div className={`${fits.select} mb-2 pl-1`}>
          <div className="w-full font-mono italic text-sm font-medium bg-light-200 text-light-900 rounded-md pl-3 pr-9 py-2 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] text-left">
            {tabs.find((t) => t.id === defaultTabId)?.label ?? tabs[0].label}
          </div>
        </div>

        {/* Wrapper/row split and overflow-x-auto both mirror EntityTabs,
          * so an over-wide strip scrolls here exactly as it does there. */}
        <div className={`${fits.stripBlock} relative overflow-x-auto`}>
          <div
            data-tab-strip
            className="flex w-max items-end gap-1.5 px-3"
          >
          {tabs.map((tab) => {
            const selected = tab.id === defaultTabId;
            return (
              <div
                key={tab.id}
                className={
                  "relative z-10 flex items-center justify-center h-7 md:h-8 px-2 md:px-3 " +
                  "font-mono italic text-[11px] md:text-xs min-w-[6rem] md:min-w-[8rem] font-medium " +
                  "rounded-t-lg border-t-[1.5px] border-x-[1.5px] " +
                  (selected
                    ? "bg-light-200 text-light-900 border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)]"
                    : "bg-light-950/50 border-light-50/[0.15] text-light-300")
                }
              >
                <span className="flex items-center justify-center gap-1.5 min-h-5">
                  <span className="leading-none whitespace-nowrap">
                    {tab.label}
                  </span>
                  {tab.hasCount && (
                    <Skeleton
                      shape="pill"
                      className={`h-[0.95rem] ${TAB_BADGE_W}`}
                    />
                  )}
                </span>
              </div>
              );
            })}
          </div>
        </div>

        <Card>
          {/* The sticky filter sidebar the real tables show from 1440px
            * up, mirrored so wide viewports reserve the same gutter.
            * Absolutely positioned off the Card, exactly as the real one
            * is, so it costs the body no layout. */}
          <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
            <div className="sticky top-4">
              <Card className="gap-3">
                <Skeleton shape="block" className="h-8 w-full rounded-md" />
                {FILTER_SKELETON_WIDTHS.map((w, i) => (
                  <Skeleton key={i} className={`h-3 ${w}`} />
                ))}
              </Card>
            </div>
          </aside>

          {/* Filter chrome: search field + filters button. Hidden at
            * min-[1440px], because that's where the real tables move this
            * chrome out of the card and into a sticky aside — rendering it
            * inline at every width meant wide viewports saw a search bar
            * the loaded page never has. */}
          <div className="mb-4 flex items-center gap-3 min-[1440px]:hidden">
            <div className="flex-1 min-w-0 max-w-xs">
              <Skeleton shape="block" className="h-8 w-full rounded-md" />
            </div>
            <Skeleton shape="block" className="h-8 w-20 rounded-md" />
          </div>

          {/* The shell can't know which tab will render, so this is a
           * deliberately neutral name + two-metric grid — the shape every
           * default tab's table shares. It shows only for the moment
           * before the SSR shell arrives, after which each table's own
           * skeleton takes over with its real column spec. */}
          <TableSkeleton columns={SHELL_COLUMNS} />
        </Card>
      </section>
    </div>
  );
};

EntityDetailLayoutSuspense.displayName = "EntityDetailLayoutSuspense";

export default EntityDetailLayoutSuspense;

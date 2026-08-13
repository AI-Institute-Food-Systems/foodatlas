import Card from "@/components/basic/Card";
import Skeleton from "@/components/basic/Skeleton";
import { TableSkeleton } from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import {
  DEFAULT_TAB_ID,
  ENTITY_TABS,
  type EntityType,
} from "@/components/entities/entityTabs.config";

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

  return (
    <div className="mt-6">
      <section className="min-w-0">
        <div className="sm:hidden mb-2 pl-1">
          <div className="w-full font-mono italic text-sm font-medium bg-light-200 text-light-900 rounded-md pl-3 pr-9 py-2 border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)] text-left">
            {tabs.find((t) => t.id === defaultTabId)?.label ?? tabs[0].label}
          </div>
        </div>

        <div className="hidden sm:flex w-max items-end gap-1.5 pl-3">
          {tabs.map((tab) => {
            const selected = tab.id === defaultTabId;
            return (
              <div
                key={tab.id}
                className={
                  "relative z-10 flex items-center justify-center h-7 md:h-8 px-3 md:px-4 " +
                  "font-mono italic text-[11px] md:text-xs min-w-[7rem] md:min-w-[9.5rem] font-medium " +
                  "rounded-t-lg border-t-[1.5px] border-x-[1.5px] " +
                  (selected
                    ? "bg-light-200 text-light-900 border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.5)]"
                    : "bg-light-950/50 border-light-50/[0.15] text-light-300")
                }
              >
                <span className="flex items-center justify-center gap-1.5 min-h-5">
                  <span className="leading-none">{tab.label}</span>
                  {tab.hasCount && (
                    <Skeleton shape="pill" className="h-[0.95rem] w-6" />
                  )}
                </span>
              </div>
            );
          })}
        </div>

        <Card>
          {/* Filter chrome row: search field + filters button placeholder.
           * Matches the mobile filter row (`!hideChrome && <flex row>`)
           * the client tables render above their skeleton body. */}
          <div className="mb-4 flex items-center gap-3">
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

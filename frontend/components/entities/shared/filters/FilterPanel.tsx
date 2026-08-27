"use client";

// THE filter-panel chrome: left sidebar at min-[1440px], search + trigger row
// below that, and a right-hand drawer behind the trigger.
//
// Why this is a component and not a documented pattern: the aside's classes
// were byte-identical in four files, and the chemical composition sidebar
// STILL landed in the wrong place. `absolute right-full` resolves against the
// nearest positioned ancestor — FoodCompositionSection happened to wrap its
// table in `relative`, ChemicalCompositionTable did not. Copying the classes
// was never enough; the positioning context is half the contract.
//
// So FilterPanel renders that wrapper itself. Callers pass their table as
// children and cannot supply the wrong ancestor, because they no longer
// supply one at all.
//
// Modals are the exception — Modal owns its own sidebar slot outside the
// DialogPanel (see feedback-modal-sidebar-inside-panel: a flex sibling gets
// dismissed on click). They use FilterPanelBody + FilterDrawer directly.

import { ReactNode } from "react";
import { MdClose, MdTune } from "react-icons/md";

import Card from "@/components/basic/Card";
import ResetFiltersButton from "@/components/basic/ResetFiltersButton";

// The panel's scrolling contents: caller's filter groups, then the one
// control that clears them. Reset lives here rather than at the call sites
// because every surface renders this same node into BOTH the sidebar and the
// drawer — putting it at the call site is precisely how one of the two ends
// up without it.
export const FilterPanelBody = ({
  children,
  isDirty,
  onReset,
}: {
  children: ReactNode;
  isDirty: boolean;
  onReset: () => void;
}) => (
  <div className="flex flex-col gap-4">
    {children}
    <ResetFiltersButton isDirty={isDirty} onReset={onReset} />
  </div>
);

// Sub-1440px drawer. Exported for modals, which cannot use FilterPanel's
// sidebar but still need the same drawer.
//
// z-[60] rather than z-50 so it clears an open Modal; on a plain page there
// is nothing at either level for it to fight with, so one value serves both
// and the two stop drifting apart.
export const FilterDrawer = ({
  open,
  onClose,
  children,
}: {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
}) => {
  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-[60] min-[1440px]:hidden"
      role="dialog"
      aria-modal="true"
      aria-label="Filters"
    >
      <button
        type="button"
        aria-label="Close filters"
        onClick={onClose}
        className="absolute inset-0 bg-black/60 cursor-default"
      />
      <aside className="absolute right-0 top-0 h-full w-[85vw] max-w-sm bg-light-950 border-l border-light-700/50 overflow-y-auto flex flex-col gap-4 p-4">
        <div className="flex items-center justify-between">
          <span className="font-mono italic text-sm text-light-300">
            Filters
          </span>
          <button
            type="button"
            aria-label="Close filters"
            onClick={onClose}
            className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
          >
            <MdClose className="w-4 h-4" />
          </button>
        </div>
        {children}
      </aside>
    </div>
  );
};

// The sidebar's geometry, exported so the loading shell reserves exactly
// the box the real sidebar will occupy. The shell used to hand-copy this
// string; a change here would have silently moved one and not the other.
// The aside is inset by one Card padding at BOTH ends so it spans the tab
// card's full frame rather than its content box.
//
// Card is `md:py-4` (16px) plus `border-[1.5px]`, so its content starts and
// ends 17.5px inside the outer edge. This element is positioned against the
// wrapper, which fills that content box — so `top-0 bottom-0` would leave the
// aside a padding short at each end. -17px at the top has always corrected
// that; the bottom was left at `bottom-0`, which is why the sidebar unstuck
// just before the table card ended. The two edges are the same measurement
// and now say so.
//
// Only rendered at min-[1440px], so `md:py-4` is always the padding in play;
// the 12px `py-3` below that breakpoint never applies here.
export const FILTER_SIDEBAR_CLASS =
  "hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] -bottom-[17px] w-48";

// Where the sidebar comes to rest while scrolling. Shared with the loading
// shell for the same reason as the class above.
//
// 4.5rem = 72px = the navbar's 56px (`h-14`, and it is `fixed top-0` with a
// backdrop blur, so the page scrolls under it) plus the 16px of breathing
// room `top-4` originally implied. At top-4 the card pinned 16px from the
// viewport, i.e. 40px BEHIND the navbar, so its heading sat under the blur
// for the whole scroll. Re-derive from `h-14` in Navbar.tsx if that changes.
export const FILTER_STICKY_CLASS = "sticky top-[4.5rem]";

// Search stays OUT of the drawer — typing should never require opening a
// panel first.
export const FilterTriggerRow = ({
  search,
  onOpen,
}: {
  search?: ReactNode;
  onOpen: () => void;
}) => (
  <div className="min-[1440px]:hidden mb-1 flex items-center gap-3">
    <div className="flex-1 min-w-0 max-w-xs">{search}</div>
    <button
      type="button"
      onClick={onOpen}
      className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
    >
      <MdTune className="w-4 h-4" />
      Filters
    </button>
  </div>
);

interface Props {
  // The filter groups. Rendered into the sidebar and the drawer alike.
  filters: ReactNode;
  // Optional search box, shown above the filters in both places.
  search?: ReactNode;
  isDirty: boolean;
  onReset: () => void;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  // The table this panel filters.
  children: ReactNode;
  // Anchors the sidebar, for deep links like #composition.
  id?: string;
  // Suppress the sidebar, trigger and drawer, keeping only the positioning
  // wrapper. For tables whose PARENT hosts the chrome — a tab that filters
  // several tables from one panel would otherwise stack a sidebar per table.
  hideChrome?: boolean;
}

const FilterPanel = ({
  filters,
  search,
  isDirty,
  onReset,
  open,
  onOpenChange,
  children,
  id,
  hideChrome = false,
}: Props) => {
  const body = (
    <FilterPanelBody isDirty={isDirty} onReset={onReset}>
      {search}
      {filters}
    </FilterPanelBody>
  );

  if (hideChrome) {
    return (
      <div id={id} className="relative scroll-mt-8">
        {children}
      </div>
    );
  }

  return (
    // `relative` is the whole point of this wrapper — see the file header.
    <div id={id} className="relative scroll-mt-8">
      {/* Sidebar sits OUTSIDE the table's flow, absolutely positioned to its
       * left via `right-full`, so the table keeps its full centred max-width.
       * mr-10 clears the Card frame; the vertical inset is explained on
       * FILTER_SIDEBAR_CLASS. Only at min-[1440px]+, where the max-w-5xl
       * gutter has room for a w-48 aside; the drawer covers narrower. */}
      <aside className={FILTER_SIDEBAR_CLASS}>
        {/* The aside spans the card's full height, which is what lets this
         * inner box stay stuck until the table card actually ends. */}
        <div className={FILTER_STICKY_CLASS}>
          <Card>{body}</Card>
        </div>
      </aside>

      <FilterTriggerRow search={search} onOpen={() => onOpenChange(true)} />

      {children}

      <FilterDrawer open={open} onClose={() => onOpenChange(false)}>
        {body}
      </FilterDrawer>
    </div>
  );
};

FilterPanel.displayName = "FilterPanel";
FilterPanelBody.displayName = "FilterPanelBody";
FilterDrawer.displayName = "FilterDrawer";
FilterTriggerRow.displayName = "FilterTriggerRow";

export default FilterPanel;

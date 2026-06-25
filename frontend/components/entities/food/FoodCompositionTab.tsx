// Merged Composition tab — hosts the "All chemicals" full table and the
// "Nutrients" category-filtered table behind a single in-card view switch.
//
// The Nutrients chip behaves as one shape with three states:
//   1. idle (no selection or selection cleared): just the Nutrients head pill.
//   2. selected (an explicit category is chosen, not hovering): the selected
//      category MERGES into the head as a single elongated cream pill —
//      same fill color on both sides, with the head sitting on top via z-10
//      so its rounded right curve dominates the seam.
//   3. menu (hovered): the tail expands to show every category as inline
//      segments — selected stays cream, others are dark slabs.
// Clicking the head activates nutrients view without changing the selection.
// Clicking "All" or a category sets the selection and activates if needed.

"use client";

import { Fragment, useEffect, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { twMerge } from "tailwind-merge";

import FoodCompositionSection from "@/components/entities/food/FoodCompositionSection";
import MacrosAndMicrosSection from "@/components/entities/food/MacrosAndMicrosSection";

type View = "chemicals" | "nutrients";
type CategoryInfo = { key: string; count: number };

interface Props {
  commonName: string;
  chemicalsCount?: number | null;
  nutrientsCount?: number | null;
  nutritionCategories?: CategoryInfo[];
}

const VIEW_PARAM = "view";

const FoodCompositionTab = ({
  commonName,
  chemicalsCount,
  nutrientsCount,
  nutritionCategories = [],
}: Props) => {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const view: View =
    (searchParams.get(VIEW_PARAM) ?? "chemicals").toLowerCase() === "nutrients"
      ? "nutrients"
      : "chemicals";

  const [nutritionCategory, setNutritionCategory] = useState<string | null>(
    null
  );
  useEffect(() => {
    if (view !== "nutrients") setNutritionCategory(null);
  }, [view]);

  const setView = (next: View) => {
    if (next === view) return;
    const p = new URLSearchParams(searchParams.toString());
    p.set(VIEW_PARAM, next);
    router.replace(`${pathname}?${p.toString()}`, { scroll: false });
  };

  const totalCount =
    typeof nutrientsCount === "number"
      ? nutrientsCount
      : nutritionCategories.reduce((s, c) => s + c.count, 0);

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-stretch gap-1.5">
        <ModeChip
          label="All chemicals"
          count={chemicalsCount}
          active={view === "chemicals"}
          onClick={() => setView("chemicals")}
        />
        <NutrientsChip
          active={view === "nutrients"}
          totalCount={totalCount}
          categories={nutritionCategories}
          selected={nutritionCategory}
          onActivate={() => setView("nutrients")}
          onSelectAll={() => {
            setNutritionCategory(null);
            if (view !== "nutrients") setView("nutrients");
          }}
          onSelectCategory={(key) => {
            setNutritionCategory(key);
            if (view !== "nutrients") setView("nutrients");
          }}
        />
      </div>

      <div className="border-t border-light-700/40 -mt-3" />

      {view === "nutrients" ? (
        <MacrosAndMicrosSection
          commonName={commonName}
          selectedCategory={nutritionCategory}
        />
      ) : (
        <FoodCompositionSection commonName={commonName} />
      )}
    </div>
  );
};

interface NutrientsChipProps {
  active: boolean;
  totalCount: number;
  categories: CategoryInfo[];
  selected: string | null;
  onActivate: () => void;
  onSelectAll: () => void;
  onSelectCategory: (key: string) => void;
}

const NutrientsChip = ({
  active,
  totalCount,
  categories,
  selected,
  onActivate,
  onSelectAll,
  onSelectCategory,
}: NutrientsChipProps) => {
  // JS-driven hover with a small grace timer — stable hit area when the
  // tail expands under the cursor.
  const [hover, setHover] = useState(false);
  const leaveTimer = useRef<number | null>(null);
  const onEnter = () => {
    if (leaveTimer.current !== null) {
      window.clearTimeout(leaveTimer.current);
      leaveTimer.current = null;
    }
    setHover(true);
  };
  const onLeave = () => {
    if (leaveTimer.current !== null) window.clearTimeout(leaveTimer.current);
    leaveTimer.current = window.setTimeout(() => {
      setHover(false);
      leaveTimer.current = null;
    }, 200);
  };

  const selectedCat = selected
    ? categories.find((c) => c.key === selected) ?? null
    : null;
  const showMenu = hover;
  const showAttached = active && !!selectedCat && !hover;
  const tailOpen = showMenu || showAttached;

  return (
    <div
      onPointerEnter={onEnter}
      onPointerLeave={onLeave}
      className="inline-flex items-stretch font-mono italic text-sm leading-tight"
    >
      {/* HEAD — always visible, z-10 to sit on top of any tail intrusion. */}
      <button
        type="button"
        onClick={onActivate}
        aria-pressed={active}
        className={twMerge(
          "relative z-10 px-3 py-1 inline-flex items-center gap-2 whitespace-nowrap rounded-full border-[1.5px] transition-colors",
          active
            ? "bg-light-200 border-light-200 text-light-900 font-semibold shadow-[inset_0_1px_2px_rgba(255,249,242,0.6)]"
            : "bg-transparent border-light-700/60 text-light-400 font-medium hover:text-light-100 hover:border-light-500"
        )}
      >
        <span>Nutrients</span>
        <CountBadge value={totalCount} on={active ? "cream" : "dark"} />
      </button>

      {/* TAIL — animated container. Slides under the head via -ml-3 only
       * when nutrients is the active view (head's solid cream bg + z-10
       * covers the intrusion). In hover-preview from chemicals view, the
       * head is transparent, so we sit flush. rounded-r-full +
       * overflow-hidden gives the right edge a clean rounded cap, no
       * matter which segment ends up rightmost. */}
      <div
        aria-hidden={!tailOpen}
        className={twMerge(
          "flex items-stretch overflow-hidden rounded-r-full transition-[max-width,margin-left] duration-300 ease-out",
          tailOpen
            ? active
              ? "max-w-[48rem] -ml-3"
              : "max-w-[48rem] ml-0"
            : "max-w-0 ml-0"
        )}
      >
        {/* "All" — only shown inside the menu (i.e., when hovering).
         * Never attaches in compact mode per the spec: when All is
         * selected, the head stands alone. */}
        <Segment
          label="All"
          count={totalCount}
          state={
            !showMenu
              ? "hidden"
              : selected === null
                ? "selected"
                : "menu-item"
          }
          isFirst={showMenu}
          onClick={onSelectAll}
        />
        {categories.map((cat) => {
          const isSelected = selected === cat.key;
          const state: SegmentState = !tailOpen
            ? "hidden"
            : showAttached && isSelected
              ? "selected"
              : showMenu
                ? isSelected
                  ? "selected"
                  : "menu-item"
                : "hidden";
          return (
            <Fragment key={cat.key}>
              <Segment
                label={cat.key}
                count={cat.count}
                state={state}
                // First-visible-after-head needs extra left padding so its
                // text clears the head's z-10 cover (compensates for the
                // tail's -ml-3 in compact-attached mode).
                isFirst={showAttached && isSelected}
                onClick={() => onSelectCategory(cat.key)}
              />
            </Fragment>
          );
        })}
      </div>
    </div>
  );
};

type SegmentState = "hidden" | "selected" | "menu-item";

interface SegmentProps {
  label: string;
  count: number;
  state: SegmentState;
  isFirst?: boolean;
  onClick: () => void;
}

const Segment = ({ label, count, state, isFirst, onClick }: SegmentProps) => {
  const visible = state !== "hidden";
  const padding = !visible
    ? "px-0 py-1"
    : isFirst
      ? "pl-5 pr-2 py-1"
      : "px-2 py-1";
  // SELECTED uses the same solid cream as the head, so when the tail is
  // attached the two look like one continuous pill. MENU-ITEM is a dark
  // slab for clear inactive contrast in the hover menu.
  const stateStyles =
    state === "selected"
      ? "bg-light-200 text-light-900 font-semibold"
      : "bg-light-1000 text-light-400 hover:text-light-100 hover:bg-light-900";
  return (
    <button
      type="button"
      onClick={onClick}
      tabIndex={visible ? 0 : -1}
      aria-pressed={state === "selected"}
      aria-hidden={!visible}
      className={twMerge(
        "inline-flex items-center gap-1 whitespace-nowrap capitalize text-[11px] leading-none overflow-hidden transition-all duration-300 ease-out",
        visible ? "max-w-[14rem]" : "max-w-0",
        padding,
        stateStyles
      )}
    >
      <span>{label}</span>
      <span className="not-italic font-mono text-[10px] tabular-nums opacity-70">
        {count.toLocaleString()}
      </span>
    </button>
  );
};

interface ModeChipProps {
  label: string;
  count?: number | null;
  active: boolean;
  onClick: () => void;
}

const ModeChip = ({ label, count, active, onClick }: ModeChipProps) => (
  <button
    type="button"
    onClick={onClick}
    aria-pressed={active}
    className={twMerge(
      "inline-flex items-baseline gap-2 px-3 py-1 rounded-full border-[1.5px] font-mono italic text-sm leading-tight transition-colors",
      active
        ? "bg-light-200 text-light-900 border-light-200 font-semibold shadow-[inset_0_1px_2px_rgba(255,249,242,0.6)]"
        : "bg-transparent border-light-700/60 text-light-400 font-medium hover:text-light-100 hover:border-light-500"
    )}
  >
    <span>{label}</span>
    {typeof count === "number" && (
      <CountBadge value={count} on={active ? "cream" : "dark"} />
    )}
  </button>
);

const CountBadge = ({
  value,
  on,
}: {
  value: number;
  on: "cream" | "dark";
}) => (
  <span
    className={twMerge(
      "not-italic font-mono text-[0.65rem] tracking-wide px-1.5 py-[1px] rounded-full tabular-nums",
      on === "cream"
        ? "bg-light-900/15 text-light-700"
        : "bg-light-800/80 text-light-400"
    )}
  >
    {value.toLocaleString()}
  </span>
);

FoodCompositionTab.displayName = "FoodCompositionTab";

export default FoodCompositionTab;

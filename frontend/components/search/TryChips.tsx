"use client";

// Static example chips below the search bar — replaces the previous
// rolling cursor placeholder + the SearchInfo helper text. One chip
// per entity type, each routes to the entity page on click. Curated
// (not data-driven) so the landing always shows the same examples and
// doesn't churn.

import { useRouter } from "next/navigation";
import { useContext } from "react";
import { twMerge } from "tailwind-merge";

import BioactivityIcon from "@/components/icons/BioactivityIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import FoodIcon from "@/components/icons/FoodIcon";
import { useNavigationSignal } from "@/context/navigationContext";
import { SearchContext } from "@/context/searchContext";
import { encodeSpace } from "@/utils/utils";

// While the search fly-up is open, the container above uses
// `position: fixed`. If a chip click blurs the input first, `isFocused`
// flips to false, the container drops back to `position: absolute` mid-
// click, the button moves out from under the cursor, and the click
// never lands. We preventDefault on mousedown to keep focus on the
// input so layout stays stable through mouseup + click.

type EntityType = "food" | "chemical" | "bioactivity" | "disease";

const EXAMPLES: {
  type: EntityType;
  label: string;
  slug: string;
  icon: React.ReactNode;
}[] = [
  {
    type: "bioactivity",
    label: "Antioxidant",
    slug: "antioxidant",
    icon: <BioactivityIcon color="#10b981" />,
  },
  {
    type: "food",
    label: "Strawberry",
    slug: "strawberry",
    icon: <FoodIcon color="#f59e0b" />,
  },
  {
    type: "chemical",
    label: "Quercetin",
    slug: "quercetin",
    icon: <ChemicalIcon color="#0891b2" />,
  },
  {
    type: "disease",
    label: "Diabetes mellitus",
    slug: "diabetes mellitus",
    icon: <DiseaseIcon color="#9333ea" />,
  },
];

const TryChips = () => {
  const router = useRouter();
  const { inputRef, setIsVisible } = useContext(SearchContext);
  const { startNav } = useNavigationSignal();

  const go = (type: EntityType, slug: string) => {
    inputRef.current?.blur();
    setIsVisible(false);
    startNav();
    router.push(`/${type}/${encodeURIComponent(encodeSpace(slug))}`);
  };

  return (
    <div className="flex flex-wrap items-center justify-center gap-2 select-none">
      <span className="font-mono italic text-xs uppercase tracking-wider text-light-200 mr-1">
        Try out
      </span>
      {EXAMPLES.map((ex) => (
        <button
          key={ex.type}
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => go(ex.type, ex.slug)}
          className={twMerge(
            "inline-flex items-center gap-1.5 pl-1.5 pr-2.5 py-1 rounded-full border border-white/10",
            "bg-black/40 backdrop-blur-md text-light-100 text-xs font-mono transition-colors",
            "hover:bg-black/60 hover:border-white/25 hover:text-white",
          )}
        >
          <span className="text-sm">{ex.icon}</span>
          {ex.label}
        </button>
      ))}
    </div>
  );
};

TryChips.displayName = "TryChips";
export default TryChips;

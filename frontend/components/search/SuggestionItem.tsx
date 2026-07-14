"use client";

import { useRouter } from "next/navigation";

import BioactivityIcon from "@/components/icons/BioactivityIcon";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import FoodIcon from "@/components/icons/FoodIcon";
import { useNavigationSignal } from "@/context/navigationContext";
import { Suggestion } from "@/types/Suggestion";
import { encodeSpace } from "@/utils/utils";

// Same icon colours as HeaderSection so the type marker in the search
// dropdown reads as the same visual language as the entity page.
const entityIcon: Record<string, React.ReactNode> = {
  food:        <FoodIcon color="#f59e0b" />,
  chemical:    <ChemicalIcon color="#0891b2" />,
  disease:     <DiseaseIcon color="#a855f7" />,
  bioactivity: <BioactivityIcon color="#10b981" />,
};

interface SuggestionItemProps {
  isSelected: boolean;
  suggestion: Suggestion;
  onMouseMove: () => void;
}

const SuggestionItem = ({
  isSelected,
  suggestion,
  onMouseMove,
}: SuggestionItemProps) => {
  const router = useRouter();
  const { startNav } = useNavigationSignal();
  const icon = entityIcon[suggestion.entity_type] ?? null;

  const navigate = () => {
    // Let SearchBar's route-change effect handle isVisible teardown —
    // doing it here fires before navigation and causes the bar to
    // morph back to its compact position mid-fade.
    startNav();
    router.push(
      `/${suggestion.entity_type}/${encodeURIComponent(
        encodeSpace(suggestion.common_name)
      )}`
    );
  };

  const handleClick = (e: React.MouseEvent<HTMLDivElement>) => {
    e.stopPropagation();
    e.preventDefault();
    navigate();
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      e.stopPropagation();
      navigate();
    }
  };

  const hasCommonName = !!suggestion.common_name?.trim();
  const hasScientificName = !!suggestion.scientific_name?.trim();

  return (
    <div
      id="foodatlas-search"
      role="button"
      tabIndex={0}
      aria-label={`Open ${suggestion.entity_type} ${suggestion.common_name}`}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      onMouseMove={onMouseMove}
      className={`flex items-center gap-3 px-4 py-2.5 cursor-pointer border-b border-light-50/[0.06] transition-colors ${
        isSelected ? "bg-light-50/10" : "hover:bg-light-50/5"
      }`}
    >
      {/* Entity icon — left, consistent size */}
      {icon && <span className="text-xl flex-shrink-0">{icon}</span>}

      {/* Name block */}
      <div className="min-w-0 flex-1">
        {hasCommonName && (
          <p className="text-sm text-light-100 capitalize leading-tight truncate">
            {suggestion.common_name}
          </p>
        )}
        {hasScientificName && (
          <p className="text-xs text-light-500 italic font-mono leading-tight truncate mt-0.5">
            {suggestion.scientific_name}
          </p>
        )}
      </div>

      {/* Right: associations count stacked with label */}
      {suggestion.associations != null && (
        <div className="flex flex-col items-end leading-none flex-shrink-0">
          <span className="font-mono text-xs text-light-100 tabular-nums">
            {Number(suggestion.associations).toLocaleString()}
          </span>
          <span className="font-mono italic text-[9px] text-light-500 mt-0.5">
            associations
          </span>
        </div>
      )}
    </div>
  );
};

SuggestionItem.displayName = "SuggestionItem";
export default SuggestionItem;

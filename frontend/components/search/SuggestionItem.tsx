"use client";

import { useContext } from "react";
import { useRouter } from "next/navigation";

import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import FoodIcon from "@/components/icons/FoodIcon";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types/Suggestion";
import { encodeSpace } from "@/utils/utils";

const icon = {
  food: <FoodIcon color="#d97706" />,
  chemical: <ChemicalIcon color="#0891b2" />,
  disease: <DiseaseIcon color="#9333ea" />,
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
  const { setIsVisible } = useContext(SearchContext);

  const handleClick = (e: React.MouseEvent<HTMLDivElement>) => {
    e.stopPropagation();
    e.preventDefault();
    setIsVisible(false);
    router.push(
      `/${suggestion.entity_type}/${encodeURIComponent(
        encodeSpace(suggestion.common_name)
      )}`
    );
  };

  const hasCommonName = suggestion.common_name !== null;
  const hasScientificName = suggestion.scientific_name !== null;

  return (
    <div
      id="foodatlas-search"
      className={`flex gap-6 px-5 py-5 cursor-pointer border-b-light-50/20 border-b ${
        isSelected && "bg-light-50/15 border-light-50/15"
      }`}
      tabIndex={0}
      onClick={handleClick}
      onMouseMove={onMouseMove}
    >
      {/* left */}
      <div className="mt-1 w-8 flex justify-center">
        {/* @ts-ignore */}
        <div className="text-3xl">{icon[suggestion.entity_type]}</div>
      </div>
      {/* middle */}
      <div className="w-full">
        <div>
          {(hasCommonName || hasScientificName) && (
            <p className="text-xs text-light-400 font-mono italic">
              {hasCommonName && "Common Name"}
              {hasCommonName && hasScientificName && (
                <span className="not-italic">｜</span>
              )}
              {hasScientificName && "Scientific Name"}
            </p>
          )}
          <p className="capitalize break-all">
            {hasCommonName && <span>{suggestion.common_name}</span>}
            {hasCommonName && hasScientificName && "｜"}
            {hasScientificName && <span>{suggestion.scientific_name}</span>}
          </p>
        </div>
      </div>
      {/* right */}
      <div className="w-20  flex flex-col text-center">
        <span className="text-xs text-light-400">Associations</span>
        <span className="">{suggestion.associations}</span>
      </div>
    </div>
  );
};

SuggestionItem.displayName = "SuggestionItem";

export default SuggestionItem;

"use client";

import { useContext } from "react";
import { useRouter } from "next/navigation";

import { AutocompleteContext } from "@/context/autocompleteContext";
import Card from "@/components/basic/Card";
import Badge from "@/components/basic/Badge";
import ChemicalIcon from "@/components/icons/ChemicalIcon";
import DiseaseIcon from "@/components/icons/DiseaseIcon";
import FoodIcon from "@/components/icons/FoodIcon";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types/Suggestion";
import { encodeSpace } from "@/utils/utils";

const colorScheme = {
  food: "text-amber-600 border-amber-600 bg-amber-600/10 shadow-amber-600/50",
  chemical: "text-cyan-600 border-cyan-600 bg-cyan-600/10 shadow-cyan-600/50",
  disease:
    "text-purple-500 border-purple-500 bg-purple-500/10 shadow-purple-500/50",
};

const icon = {
  food: <FoodIcon color="#d97706" />,
  chemical: <ChemicalIcon color="#0891b2" />,
  disease: <DiseaseIcon color="#a855f7" />,
};

const highlightMatch = (text: string, searchTerm: string) => {
  if (!searchTerm) return text;
  const regex = new RegExp(`(${searchTerm})`, "gi");
  const parts = text.split(regex);
  return parts.map((part, index) =>
    regex.test(part) ? (
      <span key={index} className="bg-accent-500/50">
        {part}
      </span>
    ) : (
      part
    )
  );
};

interface ResultItemProps {
  suggestion: Suggestion;
}

const ResultItem = ({ suggestion }: ResultItemProps) => {
  const router = useRouter();
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { setIsVisible } = useContext(SearchContext);

  // handle container click
  const handleClick = () => {
    setIsVisible(false);
    router.push(
      `/${suggestion.entity_type}/${encodeURIComponent(
        encodeSpace(suggestion.common_name)
      )}`
    );
  };

  const hasScientificName = suggestion.scientific_name !== null;

  return (
    <Card
      className="hover:bg-zinc-800/80 hover:border-light-50/20 hover:shadow-light-700/20 cursor-pointer transition-colors duration-150 ease-in-out"
      onClick={handleClick}
    >
      <div className="flex flex-col gap-2">
        <div className="flex justify-between items-center">
          {/* badge & id container */}
          <div className="flex items-center gap-2">
            <Badge
              // @ts-ignore
              color={colorScheme[suggestion.entity_type]}
              // @ts-ignore
              leftIcon={icon[suggestion.entity_type]}
              size="sm"
            >
              {suggestion.entity_type}
            </Badge>
            <div className="border-l h-[1rem] border-light-500 hidden md:visible" />
            {/* ids (big screen) */}
            <div className="gap-2 hidden md:flex">
              {/* foodatlas id */}
              <div className="text-[0.7rem] flex gap-1 items-center">
                <span className="italic text-light-400 font-mono leading-tight">
                  FoodAtlas
                </span>
                <span className="break-all text-light-300 leading-tight">
                  {highlightMatch(suggestion.foodatlas_id, autocompleteTerm)}
                </span>
              </div>
              {/* other ids */}
              {suggestion.external_references &&
                Object.values(suggestion.external_references).map(
                  (external_reference) => (
                    <div
                      key={external_reference.display_name}
                      className="text-[0.7rem] flex gap-1 items-center"
                    >
                      <span className="italic text-light-400 font-mono leading-tight">
                        {external_reference.display_name}
                      </span>
                      <span className="break-all text-light-300 leading-tight">
                        {Object.values(external_reference.ids).map((id) =>
                          highlightMatch(id.id, autocompleteTerm)
                        )}
                      </span>
                    </div>
                  )
                )}
            </div>
          </div>
          {/* # connections container */}
          <span className="text-xs text-light-300">
            {suggestion.associations} Associations
          </span>
        </div>
        {/* common & scientific name container */}
        <div className="mt-1.5">
          {/* heading */}
          <p className="text-[0.65rem] text-light-400 font-mono italic">
            Common Name
            {hasScientificName && (
              <span className="not-italic"> & Scientific Name </span>
            )}
          </p>
          {/* common & scientific name */}
          <p className="mt-0.5 capitalize break-all leading-snug">
            {/* common name */}
            <span>
              {highlightMatch(suggestion.common_name, autocompleteTerm)}
            </span>
            {/* scientific name */}
            {hasScientificName && (
              <span>
                ; {highlightMatch(suggestion.scientific_name, autocompleteTerm)}
              </span>
            )}
          </p>
        </div>
        {/* synonyms container */}
        {suggestion.synonyms && suggestion.synonyms.length > 0 && (
          <div>
            {/* heading */}
            <p className="mt-1 text-[0.65rem] text-light-400 font-mono italic">
              Synonyms
            </p>
            {/* container */}
            <div className="mt-0.5">
              {/* synonyms & more indicator */}
              <p className=" capitalize leading-snug">
                {/* synonyms */}
                {suggestion.synonyms
                  .slice(0, 12)
                  .map((synonym) => highlightMatch(synonym, autocompleteTerm))
                  // @ts-ignore
                  .reduce((prev, curr) => [prev, ", ", curr])}
                {/* more indicator */}
                {suggestion.synonyms.slice(12).length > 0 && (
                  <div className="inline px-1">
                    ... {suggestion.synonyms.slice(12).length} more
                  </div>
                )}
              </p>
            </div>
          </div>
        )}
        {/* ids (small screen) */}
        <div className="flex gap-2 md:hidden">
          {/* foodatlas id */}
          <div className="text-[0.7rem] flex gap-1 items-center">
            <span className="italic text-light-400 font-mono leading-tight">
              FoodAtlas
            </span>
            <span className="break-all text-light-300 leading-tight">
              {highlightMatch(suggestion.foodatlas_id, autocompleteTerm)}
            </span>
          </div>
          {/* other ids */}
          {suggestion.external_references &&
            Object.values(suggestion.external_references).map(
              (external_reference) => (
                <div
                  key={external_reference.display_name}
                  className="text-[0.7rem] flex gap-1 items-center"
                >
                  <span className="italic text-light-400 font-mono leading-tight">
                    {external_reference.display_name}
                  </span>
                  <span className="break-all text-light-300 leading-tight">
                    {Object.values(external_reference.ids).map((id) =>
                      highlightMatch(id.id, autocompleteTerm)
                    )}
                  </span>
                </div>
              )
            )}
        </div>
      </div>
    </Card>
  );
};

ResultItem.displayName = "ResultItem";

export default ResultItem;

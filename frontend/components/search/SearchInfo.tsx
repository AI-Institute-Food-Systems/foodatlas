import { MdInfo } from "react-icons/md";

const SearchInfo = () => {
  return (
    <div className="text-light-300 text-sm select-none">
      <span className="flex gap-1">
        <MdInfo className="mt-[0.2rem] shrink-0" />
        <p className="leading-relaxed">
          Search foods, chemicals and diseases by their common/scientific name
          or external ids (FoodOn, ChEBI, PubChem, FDC Nutrient, Mesh, OMIM,
          Disease Ontology)
        </p>
      </span>
    </div>
  );
};

SearchInfo.displayName = "SearchInfo";

export default SearchInfo;

import LoadingCard from "@/components/basic/LoadingCard";
import Heading from "@/components/basic/Heading";

const ChemicalCompositionSectionSuspense = () => {
  return (
    <div className="flex flex-col gap-5">
      <Heading type="h2" variant="boxed">
        Containing Foods
      </Heading>
      <div className="flex flex-col gap-10">
        {/* with concentration section */}
        <div className="flex flex-col gap-5">
          <div>
            <span className="ml-3.5 text-light-200 font-mono">
              Known Concentration Value
            </span>
            <p className="ml-3.5 text-light-500">
              Foods containing this chemical with known concentration
            </p>
          </div>
          <LoadingCard className="min-h-[7.625rem] flex-shrink-0" />
        </div>
        {/* without concentration section */}
        <div className="flex flex-col gap-5">
          <div>
            <span className="ml-3.5 text-light-200 font-mono">
              Unknown Concentration Value
            </span>
            <p className="ml-3.5 text-light-500">
              Foods containing this chemical of unknown concentration
            </p>
          </div>
          <LoadingCard className="min-h-[7.625rem]" />
        </div>
      </div>
    </div>
  );
};

ChemicalCompositionSectionSuspense.displayName =
  "ChemicalCompositionSectionSuspense";

export default ChemicalCompositionSectionSuspense;

import Heading from "@/components/basic/Heading";
import LoadingCard from "@/components/basic/LoadingCard";
import { capitalizeFirstLetter } from "@/utils/utils";

interface MetainformationSuspenseProps {
  entityType: string;
}

const MetainformationSuspense = ({
  entityType,
}: MetainformationSuspenseProps) => {
  return (
    <div className="flex flex-col gap-6">
      <Heading type="h2" variant="boxed">
        {`${capitalizeFirstLetter(entityType)} Overview`}
      </Heading>
      {/* names & ids container */}
      <div className="flex flex-col gap-5">
        <div className="flex flex-col gap-3">
          {/* common name & scientific name container */}
          <div className="flex flex-col gap-3">
            <span className="ml-3.5 text-light-200 font-mono">
              Names & Classification
            </span>
            <div
              className="grid gap-3"
              style={{
                gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              }}
            >
              <LoadingCard className="h-[6.375rem]" />
              <LoadingCard className="h-[6.375rem]" />
              {entityType === "chemical" && (
                <>
                  <LoadingCard className="h-[6.375rem]" />
                  <LoadingCard className="h-[6.375rem]" />
                </>
              )}
            </div>
          </div>
          {/* synonyms */}
          <LoadingCard className="h-[6.375rem]" />
        </div>
        {/* ids container */}
        <div className="flex flex-col gap-3">
          <span className="ml-3.5 text-light-200 font-mono">IDs</span>
          <div
            className="grid gap-3"
            style={{
              gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
            }}
          >
            {/* foodatlas id */}
            <LoadingCard className="h-[6.375rem]" />
            {/* other ids */}
            <LoadingCard className="h-[6.375rem]" />
            {entityType === "chemical" ||
              (entityType === "disease" && (
                <>
                  <LoadingCard className="h-[6.375rem]" />
                  <LoadingCard className="h-[6.375rem]" />
                </>
              ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default MetainformationSuspense;

MetainformationSuspense.displayName = "MetainformationSuspense";

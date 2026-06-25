import CorrelationTable from "@/components/entities/CorrelationTable";
import Heading from "@/components/basic/Heading";
import InfoBanner from "@/components/basic/InfoBanner";
import Link from "@/components/basic/Link";

interface DiseaseCorrelationsSectionProps {
  commonName: string;
}

const DiseaseCorrelationsSection = ({
  commonName,
}: DiseaseCorrelationsSectionProps) => {
  return (
    <div className="flex flex-col gap-7">
      <InfoBanner
        description={
          <div>
            <p>
              Please note that all information below reflects the positive
              (&apos;T&apos;) and Negative (&apos;M&apos;) literature evidence
              in the{" "}
              <Link href="https://ctdbase.org" isExternal>
                Comparative Toxicogenomics Database (CTD)
              </Link>
              . Any chemical can be toxic at high doses; refer to the
              appropriate references for validity of the claims and dosage
              effects.
            </p>
          </div>
        }
      />
      <div className="flex flex-col gap-7">
        <div className="flex flex-col gap-4">
          {/* heading */}
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-sm font-normal"
            >
              Improves
            </Heading>
            <p className="text-light-500">
              Consumption of these chemicals has been shown to either improve
              health outcomes or reduce the risk of disease onset.
            </p>
          </div>
          {/* positive correlations */}
          <CorrelationTable
            commonName={commonName}
            tableLocation={"disease"}
            correlationType={"positive"}
            headers={[{ label: "Chemical" }, { label: "Publication (PMID)" }]}
          />
        </div>
        <div className="flex flex-col gap-4">
          {/* heading */}
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-sm font-normal"
            >
              Worsens
            </Heading>
            <p className="text-light-500">
              Consumption of these chemicals has been shown to either worsen
              health outcomes or increase the risk of disease onset.
            </p>
          </div>
          {/* negative correlations */}
          <CorrelationTable
            commonName={commonName}
            tableLocation={"disease"}
            correlationType={"negative"}
            headers={[{ label: "Chemical" }, { label: "Publication (PMID)" }]}
          />
        </div>
      </div>
    </div>
  );
};

DiseaseCorrelationsSection.displayName = "DiseaseCorrelationsSection";

export default DiseaseCorrelationsSection;

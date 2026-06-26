"use client";

import Heading from "@/components/basic/Heading";
import Card from "@/components/basic/Card";
import CorrelationTable from "@/components/entities/CorrelationTable";
import InfoBanner from "@/components/basic/InfoBanner";
import Link from "@/components/basic/Link";

interface ChemicalCorrelationSectionProps {
  commonName: string;
}

const ChemicalCorrelationSection = ({
  commonName,
}: ChemicalCorrelationSectionProps) => {
  return (
    <div className="flex flex-col gap-7">
      <Heading type="h2" variant="boxed">
        Health Impacts
      </Heading>
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
        {/* positive correlations */}
        <div className="flex flex-col gap-4">
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-base font-medium"
            >
              Improves
            </Heading>
            <p className="text-light-500">
              Diseases for which the consumption of this chemical has been shown
              to either improve health outcomes or reduce the risk of onset.
            </p>
          </div>
          <Card>
            <CorrelationTable
              commonName={commonName}
              tableLocation={"chemical"}
              headers={[{ label: "Chemical" }, { label: "Disease" }, { label: "Publication (PMID)" }]}
              correlationType={"positive"}
            />
          </Card>
        </div>
        {/* negative correlations */}
        <div className="flex flex-col gap-4">
          <div>
            <Heading
              type="h3"
              className="text-light-300 font-mono text-base font-medium"
            >
              Worsens
            </Heading>
            <p className="text-light-500">
              Diseases for which the consumption of this chemical has been shown
              to either worsen health outcomes or increase the risk of onset.
            </p>
          </div>
          <Card>
            <CorrelationTable
              commonName={commonName}
              tableLocation={"chemical"}
              headers={[{ label: "Chemical" }, { label: "Disease" }, { label: "Publication (PMID)" }]}
              correlationType={"negative"}
            />
          </Card>
        </div>
      </div>
    </div>
  );
};

ChemicalCorrelationSection.displayName = "ChemicalCorrelationSection";

export default ChemicalCorrelationSection;

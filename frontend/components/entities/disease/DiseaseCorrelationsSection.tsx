"use client";

// The CTD-literature half of the merged Chemicals tab on disease pages.
// Mirror of ChemicalCorrelationSection; see it for why the Improves/
// Worsens split became a column.

import CorrelationTable from "@/components/entities/CorrelationTable";
import Heading from "@/components/basic/Heading";
import InfoBanner from "@/components/basic/InfoBanner";
import Link from "@/components/basic/Link";
import type { CorrelationDirection } from "@/components/entities/shared/CorrelationRow";

interface DiseaseCorrelationsSectionProps {
  commonName: string;
  direction?: CorrelationDirection;
  search?: string;
  onTotalRowsChange?: (total: number) => void;
}

const DiseaseCorrelationsSection = ({
  commonName,
  direction = "all",
  search = "",
  onTotalRowsChange,
}: DiseaseCorrelationsSectionProps) => (
  <div className="flex flex-col gap-4">
    <div>
      <Heading
        type="h3"
        className="text-light-300 font-mono text-sm font-normal"
      >
        From Literature
      </Heading>
      <p className="text-light-500">
        Chemicals whose consumption has been reported to improve or worsen
        this disease&apos;s outcomes or risk of onset, curated from published
        studies.
      </p>
    </div>
    <InfoBanner
      description={
        <p>
          Please note that all information below reflects the positive
          (&apos;T&apos;) and Negative (&apos;M&apos;) literature evidence in
          the{" "}
          <Link href="https://ctdbase.org" isExternal>
            Comparative Toxicogenomics Database (CTD)
          </Link>
          . Any chemical can be toxic at high doses; refer to the appropriate
          references for validity of the claims and dosage effects.
        </p>
      }
    />
    <CorrelationTable
      commonName={commonName}
      tableLocation="disease"
      direction={direction}
      search={search}
      onTotalRowsChange={onTotalRowsChange}
    />
  </div>
);

DiseaseCorrelationsSection.displayName = "DiseaseCorrelationsSection";

export default DiseaseCorrelationsSection;

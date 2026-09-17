"use client";

// The CTD-literature half of the merged Diseases tab on chemical pages.
//
// No longer owns a tab: it renders one table (both directions, filtered
// from the parent's sidebar) and reports its row count upward, so
// ChemicalDiseasesTab can sum it with the assay-inferred table for a
// single badge. The Improves/Worsens headings that used to split this
// into two tables are now a Direction column plus a sidebar facet.

import Heading from "@/components/basic/Heading";
import CorrelationTable from "@/components/entities/CorrelationTable";
import InfoBanner from "@/components/basic/InfoBanner";
import Link from "@/components/basic/Link";
import type { CorrelationDirection } from "@/components/entities/shared/CorrelationRow";

interface ChemicalCorrelationSectionProps {
  commonName: string;
  direction?: CorrelationDirection;
  search?: string;
  onTotalRowsChange?: (total: number) => void;
}

const ChemicalCorrelationSection = ({
  commonName,
  direction = "all",
  search = "",
  onTotalRowsChange,
}: ChemicalCorrelationSectionProps) => (
  <div className="flex flex-col gap-4">
    {/* Section label + blurb in the same chip-over-serif vocabulary the
      * food page's stacked sections use, so a tab that stacks two sources
      * reads the same way whichever entity you are on. */}
    <div className="flex flex-col gap-2">
      <Heading type="h3" variant="chip" className="self-start">
        From Literature
      </Heading>
      <p className="font-serif italic text-light-400 text-sm">
        Diseases whose outcomes or risk of onset this chemical has been
        reported to improve or worsen, curated from published studies.
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
      tableLocation="chemical"
      direction={direction}
      search={search}
      onTotalRowsChange={onTotalRowsChange}
    />
  </div>
);

ChemicalCorrelationSection.displayName = "ChemicalCorrelationSection";

export default ChemicalCorrelationSection;

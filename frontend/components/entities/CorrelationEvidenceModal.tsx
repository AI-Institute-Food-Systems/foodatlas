"use client";

// Every publication behind one chemical↔disease row.
//
// Split by direction, because a pair reported both ways (~4% of them)
// arrives as one "Mixed" row and a flat list would leave the reader
// unable to tell which papers said which. The single-direction case —
// the other 96% — renders exactly one section and reads as before.

import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { SignBadge } from "@/components/entities/shared/CorrelationRow";
import { useReportRows } from "@/context/reportModeContext";
import { Evidence } from "@/types/Evidence";
import InfoBanner from "../basic/InfoBanner";

type Direction = "positive" | "negative";

const claim = (direction: Direction, chemicalName: string, diseaseName: string) => (
  <>
    Consumption of{" "}
    <span className="font-semibold capitalize">{chemicalName}</span>{" "}
    {direction === "positive"
      ? "improves health outcomes or reduces the risk of"
      : "worsens health outcomes or increases the risk of"}{" "}
    <span className="font-semibold capitalize">{diseaseName}</span> onset.
  </>
);

type CorrelationEvidenceModalProps = {
  improvesEvidences?: Evidence[] | null;
  worsensEvidences?: Evidence[] | null;
  entityType: "chemical" | "disease";
  chemicalName: string;
  diseaseName: string;
  isOpen: boolean;
  onClose: () => void;
};

const CorrelationEvidenceModal = ({
  isOpen,
  onClose,
  entityType,
  improvesEvidences,
  worsensEvidences,
  chemicalName,
  diseaseName,
}: CorrelationEvidenceModalProps) => {
  const reporter = useReportRows();

  const sections: { direction: Direction; evidences: Evidence[] }[] = [
    { direction: "positive" as const, evidences: improvesEvidences ?? [] },
    { direction: "negative" as const, evidences: worsensEvidences ?? [] },
  ].filter((s) => s.evidences.length > 0);
  const isMixed = sections.length > 1;

  const pill = (evidence: Evidence, direction: Direction) => {
    const pmid = evidence.pmid?.id;
    const pmcid = evidence.pmcid?.id;
    const rowProps = reporter.getRowProps({
      kind: "correlation-evidence",
      entityType,
      counterpartName: entityType === "chemical" ? diseaseName : chemicalName,
      pmid: pmid !== undefined ? String(pmid) : undefined,
      pmcid: pmcid !== undefined ? String(pmcid) : undefined,
      referenceUrl: evidence.pmid?.url ?? evidence.pmcid?.url,
    });
    const key = `${direction}-${pmid ?? pmcid}`;
    return reporter.isSelectMode ? (
      <span
        key={key}
        className="inline-flex items-center gap-1 rounded-md px-2 py-0.5"
        {...rowProps}
      >
        {`${pmid ?? pmcid}`}
      </span>
    ) : (
      <Link key={key} href={evidence.pmid?.url ?? evidence.pmcid?.url}>
        {`${pmid ?? pmcid}`}
      </Link>
    );
  };

  return (
    <Modal
      fullHeight
      title="Publications (PMIDs)"
      description={
        <div className="flex flex-col gap-4">
          {isMixed ? (
            <p className="text-light-300">
              The literature reports this pair in{" "}
              <span className="font-semibold">both directions</span>. The
              publications are grouped by what each one found.
            </p>
          ) : (
            <p className="text-light-300">
              The following publications show that{" "}
              {claim(sections[0]?.direction ?? "positive", chemicalName, diseaseName)}
            </p>
          )}
          <InfoBanner
            description={
              <p>
                Please note that all information below reflects the positive
                (&apos;T&apos;) and Negative (&apos;M&apos;) literature
                evidence in the{" "}
                <Link href="https://ctdbase.org" isExternal>
                  Comparative Toxicogenomics Database (CTD)
                </Link>
                . Any chemical can be toxic at high doses; refer to the
                appropriate references for validity of the claims and dosage
                effects.
              </p>
            }
          />
        </div>
      }
      isOpen={isOpen}
      onClose={onClose}
    >
      <div className="flex flex-col gap-6">
        {sections.map(({ direction, evidences }) => (
          <section key={direction} className="flex flex-col gap-2">
            {/* The heading is dropped entirely in the single-direction
             * case — the description above already states the claim, and
             * a lone "Improves" header just repeats it. */}
            {isMixed && (
              <div className="flex items-center gap-2">
                <SignBadge direction={direction} />
                <h3 className="font-mono text-sm text-light-300">
                  {direction === "positive" ? "Improves" : "Worsens"}
                </h3>
                <span className="text-xs text-light-500">
                  {evidences.length.toLocaleString()}
                </span>
              </div>
            )}
            {isMixed && (
              <p className="text-sm text-light-500">
                {claim(direction, chemicalName, diseaseName)}
              </p>
            )}
            <div className="flex gap-2 flex-wrap">
              {evidences.map((evidence) => pill(evidence, direction))}
            </div>
          </section>
        ))}
      </div>
    </Modal>
  );
};

CorrelationEvidenceModal.displayName = "CorrelationEvidenceModal";

export default CorrelationEvidenceModal;

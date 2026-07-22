import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import ReportIssueButton from "@/components/basic/ReportIssueButton";
import { Evidence } from "@/types/Evidence";
import InfoBanner from "../basic/InfoBanner";

type ModalDescriptionProps = {
  correlationType: "positive" | "negative";
  entityType: "chemical" | "disease";
  chemicalName: string;
  diseaseName: string;
};

const ModalDescription = ({
  correlationType,
  entityType,
  chemicalName,
  diseaseName,
}: ModalDescriptionProps) => {
  return entityType === "chemical" ? (
    correlationType === "positive" ? (
      <p className="text-light-300">
        The following publications show that the consumption of{" "}
        <span className="font-semibold capitalize">{chemicalName}</span>{" "}
        either improves health outcomes or reduces the risk of{" "}
        <span className="font-semibold capitalize">{diseaseName}</span>{" "}
        onset.
      </p>
    ) : (
      <p>
        The following publications show that the consumption of{" "}
        <span className="font-semibold capitalize">{chemicalName}</span>{" "}
        either worsens health outcomes or increases the risk of{" "}
        <span className="font-semibold capitalize">{diseaseName}</span>{" "}
        onset.
      </p>
    )
  ) : correlationType === "positive" ? (
    <p>
      The following publications show that the consumption of{" "}
      <span className="font-semibold capitalize">{chemicalName}</span>{" "}
      either improves health outcomes or reduces the risk of{" "}
      <span className="font-semibold capitalize">{diseaseName}</span>{" "}
      onset.
    </p>
  ) : (
    <p>
      The following publications show that the consumption of{" "}
      <span className="font-semibold capitalize">{chemicalName}</span>{" "}
      either worsens health outcomes or increases the risk of{" "}
      <span className="font-semibold capitalize">{diseaseName}</span>{" "}
      onset.
    </p>
  );
};

type CorrelationEvidenceModalProps = {
  evidences: Evidence[] | undefined;
  correlationType: "positive" | "negative";
  entityType: "chemical" | "disease";
  chemicalName: string;
  diseaseName: string;
  isOpen: boolean;
  onClose: () => void;
};

const CorrelationEvidenceModal = ({
  isOpen,
  onClose,
  correlationType,
  entityType,
  evidences,
  chemicalName,
  diseaseName,
}: CorrelationEvidenceModalProps) => {
  // handle modal close
  const handleModalClose = () => {
    onClose();
  };

  return (
    <Modal
      fullHeight
      title={"Publications (PMIDs)"}
      description={
        <div className="flex flex-col gap-4">
          <ModalDescription
            correlationType={correlationType}
            entityType={entityType}
            chemicalName={chemicalName}
            diseaseName={diseaseName}
          />
          <InfoBanner
            description={
              <div>
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
              </div>
            }
          />
        </div>
      }
      isOpen={isOpen}
      onClose={handleModalClose}
    >
      <div className="flex gap-2 flex-wrap">
        {evidences?.map((evidence) => (
          <span
            key={evidence.pmid?.id ?? evidence.pmcid?.id}
            className="inline-flex items-center gap-1"
          >
            <Link href={evidence.pmid?.url ?? evidence.pmcid?.url}>
              {`${evidence.pmid?.id ?? evidence.pmcid?.id}`}
            </Link>
            <ReportIssueButton
              context={{
                kind: "correlation-evidence",
                entityType,
                counterpartName:
                  entityType === "chemical" ? diseaseName : chemicalName,
                pmid:
                  evidence.pmid?.id !== undefined
                    ? String(evidence.pmid.id)
                    : undefined,
                pmcid:
                  evidence.pmcid?.id !== undefined
                    ? String(evidence.pmcid.id)
                    : undefined,
                referenceUrl: evidence.pmid?.url ?? evidence.pmcid?.url,
              }}
              ariaLabel={`Report issue with PMID ${
                evidence.pmid?.id ?? evidence.pmcid?.id
              }`}
            />
          </span>
        ))}
      </div>
    </Modal>
  );
};

CorrelationEvidenceModal.displayName = "CorrelationEvidenceModal";

export default CorrelationEvidenceModal;

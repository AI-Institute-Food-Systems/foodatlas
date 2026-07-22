import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { useTableReporter } from "@/components/basic/useTableReporter";
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
  const reporter = useTableReporter({ targetLabel: "PMID" });

  // handle modal close
  const handleModalClose = () => {
    reporter.exitSelectMode();
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
          <div className="flex justify-end">{reporter.trigger}</div>
        </div>
      }
      isOpen={isOpen}
      onClose={handleModalClose}
    >
      {reporter.banner}
      <div className="flex gap-2 flex-wrap">
        {evidences?.map((evidence) => {
          const pmid = evidence.pmid?.id;
          const pmcid = evidence.pmcid?.id;
          const rowProps = reporter.getRowProps({
            kind: "correlation-evidence",
            entityType,
            counterpartName:
              entityType === "chemical" ? diseaseName : chemicalName,
            pmid: pmid !== undefined ? String(pmid) : undefined,
            pmcid: pmcid !== undefined ? String(pmcid) : undefined,
            referenceUrl: evidence.pmid?.url ?? evidence.pmcid?.url,
          });
          return reporter.isSelectMode ? (
            <span
              key={pmid ?? pmcid}
              className="inline-flex items-center gap-1 rounded-md px-2 py-0.5"
              {...rowProps}
            >
              {`${pmid ?? pmcid}`}
            </span>
          ) : (
            <Link
              key={pmid ?? pmcid}
              href={evidence.pmid?.url ?? evidence.pmcid?.url}
            >
              {`${pmid ?? pmcid}`}
            </Link>
          );
        })}
      </div>
      {reporter.modal}
    </Modal>
  );
};

CorrelationEvidenceModal.displayName = "CorrelationEvidenceModal";

export default CorrelationEvidenceModal;

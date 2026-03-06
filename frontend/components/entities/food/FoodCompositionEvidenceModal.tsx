import FoodAtlasEvidence from "@/components/entities/food/FoodAtlasEvidence";
import FdcEvidence from "@/components/entities/food/FdcEvidence";
import DmdEvidence from "@/components/entities/food/DmdEvidence";
import Modal from "@/components/basic/Modal";
import { FoodEvidence } from "@/types/Evidence";

interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
}

const FoodCompositionEvidenceModal = ({
  foodName,
  chemicalName,
  evidences,
  isOpen,
  onClose,
}: FoodCompositionEvidenceModalProps) => {
  // sort all evidences by their highest converted concentration value
  const sortedEvidences = evidences?.slice().sort((a, b) => {
    const maxValueA = Math.max(
      ...a.extraction.map((e) => e.converted_concentration.value || 0)
    );
    const maxValueB = Math.max(
      ...b.extraction.map((e) => e.converted_concentration.value || 0)
    );
    return maxValueB - maxValueA; // sort in descending order
  });

  const handleModalClose = () => {
    onClose();
  };

  return (
    <Modal
      title="Data Points"
      description={
        <p>
          The following data points indicate that{" "}
          <span className="capitalize font-semibold">{foodName}</span> contains{" "}
          <span className="capitalize font-semibold">{chemicalName}</span>
        </p>
      }
      isOpen={isOpen}
      onClose={handleModalClose}
    >
      <div className="flex flex-col gap-4">
        {sortedEvidences?.map((evidence, id) =>
          evidence.reference.source_name === "FoodAtlas" ? (
            <FoodAtlasEvidence key={id} evidence={evidence} />
          ) : evidence.reference.source_name === "FDC" ? (
            <FdcEvidence key={id} evidence={evidence} />
          ) : evidence.reference.source_name === "DMD" ? (
            <DmdEvidence key={id} evidence={evidence} />
          ) : null
        )}
      </div>
    </Modal>
  );
};

FoodCompositionEvidenceModal.displayName = "FoodCompositionEvidenceModal";

export default FoodCompositionEvidenceModal;

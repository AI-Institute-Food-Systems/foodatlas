"use client";

import { useEffect, useState } from "react";
import { MdCallSplit } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import FoodAtlasEvidence from "@/components/entities/food/FoodAtlasEvidence";
import FdcEvidence from "@/components/entities/food/FdcEvidence";
import DmdEvidence from "@/components/entities/food/DmdEvidence";
import Modal from "@/components/basic/Modal";
import { FoodEvidence, FoodEvidenceExtraction } from "@/types/Evidence";

// On the food page the head (food) side of ambiguity is owned by the entity
// banner; the modal only surfaces ambiguity on the counterpart (chemical) side.
const isCounterpartAmbiguous = (ex: FoodEvidenceExtraction): boolean =>
  (ex.chemical_candidates?.length ?? 0) > 1;

type AmbiguityFilter = "all" | "ambiguous" | "not-ambiguous";

interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
  initialFilter?: AmbiguityFilter;
}

const FILTER_ORDER: AmbiguityFilter[] = ["all", "ambiguous", "not-ambiguous"];

const FoodCompositionEvidenceModal = ({
  foodName,
  chemicalName,
  evidences,
  isOpen,
  onClose,
  initialFilter = "all",
}: FoodCompositionEvidenceModalProps) => {
  const [filter, setFilter] = useState<AmbiguityFilter>(initialFilter);

  useEffect(() => {
    if (isOpen) setFilter(initialFilter);
  }, [isOpen, initialFilter]);

  const cycleFilter = () => {
    setFilter((f) => {
      const idx = FILTER_ORDER.indexOf(f);
      return FILTER_ORDER[(idx + 1) % FILTER_ORDER.length];
    });
  };

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

  // counts at the evidence ("data point") level — matches the row badge count
  const totalCount = sortedEvidences?.length ?? 0;
  const ambiguousCount =
    sortedEvidences?.filter((ev) =>
      ev.extraction.some(isCounterpartAmbiguous)
    ).length ?? 0;
  const notAmbiguousCount = totalCount - ambiguousCount;

  const displayedEvidences =
    filter === "ambiguous"
      ? sortedEvidences?.filter((ev) =>
          ev.extraction.some(isCounterpartAmbiguous)
        )
      : filter === "not-ambiguous"
      ? sortedEvidences?.filter(
          (ev) => !ev.extraction.some(isCounterpartAmbiguous)
        )
      : sortedEvidences;

  const filterLabel =
    filter === "all"
      ? `All (${totalCount})`
      : filter === "ambiguous"
      ? `Only ambiguous (${ambiguousCount})`
      : `Not ambiguous (${notAmbiguousCount})`;

  const handleModalClose = () => {
    onClose();
  };

  return (
    <Modal
      title="Data Points"
      description={
        <div className="flex flex-col gap-3">
          <p>
            The following data points indicate that{" "}
            <span className="capitalize font-semibold">{foodName}</span>{" "}
            contains{" "}
            <span className="capitalize font-semibold">{chemicalName}</span>
          </p>
          {ambiguousCount > 0 && (
            <button
              type="button"
              onClick={cycleFilter}
              className={twMerge(
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
                filter === "all"
                  ? "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20"
                  : filter === "ambiguous"
                  ? "text-amber-300 border-amber-400 bg-amber-500/20 hover:bg-amber-500/30"
                  : "text-light-300 border-light-400 bg-light-400/15 hover:bg-light-400/25"
              )}
              aria-label="Cycle ambiguity filter"
            >
              <MdCallSplit className="size-3.5 rotate-90" />
              {filterLabel}
            </button>
          )}
        </div>
      }
      isOpen={isOpen}
      onClose={handleModalClose}
    >
      <div className="flex flex-col gap-4">
        {displayedEvidences?.map((evidence, id) =>
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

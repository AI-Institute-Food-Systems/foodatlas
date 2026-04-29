"use client";

import { useEffect, useState } from "react";
import { MdCallSplit, MdWarningAmber } from "react-icons/md";
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

const isLowTrust = (ex: FoodEvidenceExtraction): boolean => Boolean(ex.trust_low);

export type EvidenceFilter =
  | "all"
  | "ambiguous"
  | "not-ambiguous"
  | "low-trust";

interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
  initialFilter?: EvidenceFilter;
}

const AMBIGUITY_CYCLE: EvidenceFilter[] = ["all", "ambiguous", "not-ambiguous"];
const LOW_TRUST_CYCLE: EvidenceFilter[] = ["all", "low-trust"];

const FoodCompositionEvidenceModal = ({
  foodName,
  chemicalName,
  evidences,
  isOpen,
  onClose,
  initialFilter = "all",
}: FoodCompositionEvidenceModalProps) => {
  const [filter, setFilter] = useState<EvidenceFilter>(initialFilter);

  useEffect(() => {
    if (isOpen) setFilter(initialFilter);
  }, [isOpen, initialFilter]);

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
  const lowTrustCount =
    sortedEvidences?.filter((ev) => ev.extraction.some(isLowTrust)).length ?? 0;

  const cycleAmbiguityFilter = () => {
    setFilter((f) => {
      const idx = AMBIGUITY_CYCLE.indexOf(f);
      // If we're not on the ambiguity axis, jump to the first non-"all" state.
      if (idx === -1) return AMBIGUITY_CYCLE[1];
      return AMBIGUITY_CYCLE[(idx + 1) % AMBIGUITY_CYCLE.length];
    });
  };

  const cycleLowTrustFilter = () => {
    setFilter((f) => {
      const idx = LOW_TRUST_CYCLE.indexOf(f);
      if (idx === -1) return LOW_TRUST_CYCLE[1];
      return LOW_TRUST_CYCLE[(idx + 1) % LOW_TRUST_CYCLE.length];
    });
  };

  const displayedEvidences =
    filter === "ambiguous"
      ? sortedEvidences?.filter((ev) =>
          ev.extraction.some(isCounterpartAmbiguous)
        )
      : filter === "not-ambiguous"
      ? sortedEvidences?.filter(
          (ev) => !ev.extraction.some(isCounterpartAmbiguous)
        )
      : filter === "low-trust"
      ? sortedEvidences?.filter((ev) => ev.extraction.some(isLowTrust))
      : sortedEvidences;

  const ambiguityLabel =
    filter === "ambiguous"
      ? `Only ambiguous (${ambiguousCount})`
      : filter === "not-ambiguous"
      ? `Not ambiguous (${notAmbiguousCount})`
      : `All (${totalCount})`;

  const lowTrustLabel =
    filter === "low-trust"
      ? `Only low-trust (${lowTrustCount})`
      : `All (${totalCount})`;

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
          <div className="flex flex-wrap gap-2">
            {ambiguousCount > 0 && (
              <button
                type="button"
                onClick={cycleAmbiguityFilter}
                className={twMerge(
                  "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
                  filter === "ambiguous"
                    ? "text-amber-300 border-amber-400 bg-amber-500/20 hover:bg-amber-500/30"
                    : filter === "not-ambiguous"
                    ? "text-light-300 border-light-400 bg-light-400/15 hover:bg-light-400/25"
                    : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20"
                )}
                aria-label="Cycle ambiguity filter"
              >
                <MdCallSplit className="size-3.5 rotate-90" />
                {ambiguityLabel}
              </button>
            )}
            {lowTrustCount > 0 && (
              <button
                type="button"
                onClick={cycleLowTrustFilter}
                className={twMerge(
                  "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
                  filter === "low-trust"
                    ? "text-rose-300 border-rose-400 bg-rose-500/20 hover:bg-rose-500/30"
                    : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20"
                )}
                aria-label="Cycle low-trust filter"
              >
                <MdWarningAmber className="size-3.5" />
                {lowTrustLabel}
              </button>
            )}
          </div>
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

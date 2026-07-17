"use client";

import { useEffect, useMemo, useState } from "react";
import { MdCallSplit, MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import EvidenceTable from "@/components/entities/food/EvidenceTable";
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

  // Extraction-level counts now that the table renders one row per
  // extraction — chip labels stay in sync with what the table actually
  // shows after filtering.
  const { totalCount, ambiguousCount, notAmbiguousCount, lowTrustCount } =
    useMemo(() => {
      let total = 0;
      let ambig = 0;
      let low = 0;
      evidences?.forEach((ev) => {
        ev.extraction.forEach((ex) => {
          total += 1;
          if (isCounterpartAmbiguous(ex)) ambig += 1;
          if (isLowTrust(ex)) low += 1;
        });
      });
      return {
        totalCount: total,
        ambiguousCount: ambig,
        notAmbiguousCount: total - ambig,
        lowTrustCount: low,
      };
    }, [evidences]);

  const cycleAmbiguityFilter = () =>
    setFilter((f) => {
      const idx = AMBIGUITY_CYCLE.indexOf(f);
      if (idx === -1) return AMBIGUITY_CYCLE[1];
      return AMBIGUITY_CYCLE[(idx + 1) % AMBIGUITY_CYCLE.length];
    });

  const cycleLowTrustFilter = () =>
    setFilter((f) => {
      const idx = LOW_TRUST_CYCLE.indexOf(f);
      if (idx === -1) return LOW_TRUST_CYCLE[1];
      return LOW_TRUST_CYCLE[(idx + 1) % LOW_TRUST_CYCLE.length];
    });

  // Filter is applied at the extraction level so the table's row set
  // exactly matches the active chip's count. Evidences with no rows
  // remaining after the extraction filter are dropped so their paper
  // header doesn't dangle empty in the expanded row.
  const displayedEvidences = useMemo(() => {
    if (!evidences || filter === "all") return evidences;
    const predicate = (ex: FoodEvidenceExtraction) => {
      if (filter === "ambiguous") return isCounterpartAmbiguous(ex);
      if (filter === "not-ambiguous") return !isCounterpartAmbiguous(ex);
      if (filter === "low-trust") return isLowTrust(ex);
      return true;
    };
    return evidences
      .map((ev) => ({
        ...ev,
        extraction: ev.extraction.filter(predicate),
      }))
      .filter((ev) => ev.extraction.length > 0);
  }, [evidences, filter]);

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

  return (
    <Modal
      fullHeight
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
            <button
              type="button"
              onClick={cycleAmbiguityFilter}
              disabled={ambiguousCount === 0}
              aria-disabled={ambiguousCount === 0 || undefined}
              className={twMerge(
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
                filter === "ambiguous"
                  ? "text-amber-300 border-amber-400 bg-amber-500/20 hover:bg-amber-500/30"
                  : filter === "not-ambiguous"
                  ? "text-light-300 border-light-400 bg-light-400/15 hover:bg-light-400/25"
                  : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20",
                ambiguousCount === 0 &&
                  "opacity-40 cursor-not-allowed hover:bg-transparent"
              )}
              aria-label="Cycle ambiguity filter"
            >
              <MdCallSplit className="size-3.5 rotate-90" />
              {ambiguityLabel}
            </button>
            <button
              type="button"
              onClick={cycleLowTrustFilter}
              disabled={lowTrustCount === 0}
              aria-disabled={lowTrustCount === 0 || undefined}
              className={twMerge(
                "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
                filter === "low-trust"
                  ? "text-rose-300 border-rose-400 bg-rose-500/20 hover:bg-rose-500/30"
                  : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20",
                lowTrustCount === 0 &&
                  "opacity-40 cursor-not-allowed hover:bg-transparent"
              )}
              aria-label="Cycle low-trust filter"
            >
              <MdWarningAmber className="size-3.5" />
              {lowTrustLabel}
            </button>
          </div>
        </div>
      }
      isOpen={isOpen}
      onClose={onClose}
    >
      <EvidenceTable
        evidences={displayedEvidences}
        chemicalName={chemicalName}
      />
    </Modal>
  );
};

FoodCompositionEvidenceModal.displayName = "FoodCompositionEvidenceModal";

export default FoodCompositionEvidenceModal;

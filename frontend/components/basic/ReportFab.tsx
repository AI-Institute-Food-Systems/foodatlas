"use client";

import { MdClose, MdOutlineFlag } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import ReportIssueModal from "@/components/basic/ReportIssueModal";
import { useReportModeState } from "@/context/reportModeContext";
import type { ReportContext } from "@/types/Report";

// Sentinel used to satisfy the modal's strict `context` prop while
// closed. Never surfaced because the modal renders isOpen=false.
const SENTINEL_CONTEXT: ReportContext = {
  kind: "food-composition-row",
  entityType: "food",
};

// Global floating action button, mounted once at the app root by the
// (everything-else) layout. Two visual states:
//   - idle: bottom-right circular button with a flag icon
//   - active: rounded pill with instructions + Cancel affordance,
//     signalling that every table's rows are now selectable
//
// The banner and the report modal both live here — the rest of the app
// only interacts with the flow via useReportRows() (row-level props)
// and, indirectly, the ReportModeProvider's setter.
const ReportFab = () => {
  const {
    isSelectMode,
    toggleSelectMode,
    exitSelectMode,
    activeContext,
    closeModal,
  } = useReportModeState();

  // The Modal component uses z-50 for its backdrop. To let users start
  // a report from inside a table modal (BioactivityMeasurementsModal,
  // FoodCompositionEvidenceModal, CorrelationEvidenceModal), the FAB
  // must sit above that backdrop. Hide it entirely while our own report
  // modal is open — otherwise it would float over the modal's panel.
  const showFab = activeContext === null;

  return (
    <>
      <div
        className={twMerge(
          // Above the Modal wrapper's z-50 backdrop.
          "fixed z-[60] bottom-4 right-4",
          "pb-[env(safe-area-inset-bottom)] pr-[env(safe-area-inset-right)]",
          !showFab && "hidden",
        )}
      >
        {isSelectMode ? (
          <div
            role="status"
            aria-live="polite"
            className={twMerge(
              "flex items-center gap-3 rounded-full pl-4 pr-1 py-1",
              "border border-amber-400/60 bg-amber-500/15 text-amber-100",
              "shadow-lg shadow-black/40 backdrop-blur",
            )}
          >
            <span className="text-sm font-mono italic">
              Click any row to report it{" "}
              <span className="text-amber-200/70 not-italic">(Esc)</span>
            </span>
            <button
              type="button"
              onClick={exitSelectMode}
              aria-label="Cancel report selection"
              className={twMerge(
                "w-8 h-8 rounded-full flex items-center justify-center",
                "text-amber-100 hover:bg-amber-500/25 transition-colors",
              )}
            >
              <MdClose className="size-4" />
            </button>
          </div>
        ) : (
          <button
            type="button"
            onClick={toggleSelectMode}
            aria-label="Report an issue with a data point"
            title="Report an issue"
            className={twMerge(
              "flex items-center gap-2 rounded-full pl-3 pr-4 py-2",
              "border border-light-600/60 bg-light-950/85 text-light-100",
              "hover:bg-light-900 hover:border-light-500",
              "shadow-lg shadow-black/40 backdrop-blur",
              "transition-colors",
            )}
          >
            <MdOutlineFlag className="size-4" aria-hidden />
            <span className="text-xs font-mono italic">Report an issue</span>
          </button>
        )}
      </div>

      <ReportIssueModal
        isOpen={activeContext !== null}
        onClose={closeModal}
        context={activeContext ?? SENTINEL_CONTEXT}
      />
    </>
  );
};

ReportFab.displayName = "ReportFab";
export default ReportFab;

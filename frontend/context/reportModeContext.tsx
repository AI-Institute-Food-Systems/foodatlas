"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

import type { ReportContext } from "@/types/Report";

// Global state for the "Report an issue" flow. One provider high in the
// tree owns:
//   - whether select-row mode is active (drives every table's rows)
//   - the last-selected context (drives the report modal)
// A single floating trigger + banner + modal live in <ReportFab />,
// consuming this context. Tables consume `useReportRows()` to get the
// row-level props (onClick, className, role) — nothing else.

interface ReportModeState {
  isSelectMode: boolean;
  toggleSelectMode: () => void;
  exitSelectMode: () => void;
  selectForReport: (ctx: ReportContext) => void;
  // Modal state — exposed so <ReportFab /> can wire it into
  // <ReportIssueModal />. Not intended for consumption elsewhere.
  activeContext: ReportContext | null;
  closeModal: () => void;
}

const ReportModeCtx = createContext<ReportModeState | null>(null);

export const ReportModeProvider = ({
  children,
}: {
  children: React.ReactNode;
}) => {
  const [isSelectMode, setSelectMode] = useState(false);
  const [activeContext, setActiveContext] = useState<ReportContext | null>(
    null,
  );

  // Escape cancels select mode — matches the standard modal-dismiss
  // shortcut so it feels transparent.
  useEffect(() => {
    if (!isSelectMode) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSelectMode(false);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [isSelectMode]);

  const value = useMemo<ReportModeState>(
    () => ({
      isSelectMode,
      toggleSelectMode: () => setSelectMode((v) => !v),
      exitSelectMode: () => setSelectMode(false),
      selectForReport: (ctx) => {
        setActiveContext(ctx);
        setSelectMode(false);
      },
      activeContext,
      closeModal: () => setActiveContext(null),
    }),
    [isSelectMode, activeContext],
  );

  return (
    <ReportModeCtx.Provider value={value}>{children}</ReportModeCtx.Provider>
  );
};

const useReportMode = (): ReportModeState => {
  const ctx = useContext(ReportModeCtx);
  if (!ctx) {
    throw new Error(
      "useReportMode must be used inside <ReportModeProvider>",
    );
  }
  return ctx;
};

// Public: the modal + FAB consume this to render themselves.
export const useReportModeState = useReportMode;

// Public: tables consume this to make their rows selectable during
// global report-select mode. Returns the same props shape the old
// per-table hook returned; `disabled` skips the affordance for rows
// where it doesn't make sense (e.g. bioactivity rows with 0 assays).
//
// This variant is provider-tolerant: if a table is rendered outside
// <ReportModeProvider> (e.g. in a unit test or a preview docs page),
// getRowProps becomes a no-op instead of throwing. The FAB is what
// pins the provider; tables shouldn't need to know or care.
export const useReportRows = () => {
  const ctx = useContext(ReportModeCtx);
  const isSelectMode = ctx?.isSelectMode ?? false;
  const selectForReport = ctx?.selectForReport;

  const getRowProps = useCallback(
    (rowContext: ReportContext, opts: { disabled?: boolean } = {}) => {
      if (!isSelectMode || opts.disabled || !selectForReport) return {};
      return {
        onClick: (e: React.MouseEvent) => {
          // preventDefault covers reportable elements that wrap an
          // <a> (external IDs, taxonomy nodes, ontology parents) — the
          // anchor's navigation would otherwise fire before we can
          // open the report modal.
          e.preventDefault();
          e.stopPropagation();
          selectForReport(rowContext);
        },
        className:
          "cursor-pointer relative outline-1 outline-dashed outline-amber-500/50 -outline-offset-1 hover:bg-amber-500/10 hover:outline-amber-400 transition-colors",
        role: "button" as const,
        tabIndex: 0,
        onKeyDown: (e: React.KeyboardEvent) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            selectForReport(rowContext);
          }
        },
        "aria-label": "Report an issue with this row",
      };
    },
    [isSelectMode, selectForReport],
  );

  return { isSelectMode, getRowProps };
};

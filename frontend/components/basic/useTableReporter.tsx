"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { MdOutlineFlag } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Chip from "@/components/basic/Chip";
import ReportIssueModal from "@/components/basic/ReportIssueModal";
import type { ReportContext } from "@/types/Report";

// A per-table "Report an issue" flow, packaged so every surface can
// wire it up in three touches:
//   1. Drop `trigger` in the table toolbar.
//   2. Render `banner` above the row list (it renders null when off).
//   3. On each row, spread `getRowProps(context)` — the row goes into
//      selectable mode when `isSelectMode` is true; clicking it opens
//      the report modal keyed to that row's context.
//
// The modal is rendered inside `banner` for convenience, so callers
// don't have to remember a fourth mount point.
//
// A single Escape key press cancels select mode, matching the muscle
// memory users have from other pick-a-row UIs.

interface TableReporterOptions {
  // Purely cosmetic — flips the trigger label so multi-list surfaces
  // ("Composition" vs "Data points") can disambiguate what the user
  // will be selecting.
  targetLabel?: string;
}

export function useTableReporter(options: TableReporterOptions = {}) {
  const { targetLabel = "row" } = options;
  const [isSelectMode, setSelectMode] = useState(false);
  const [context, setContext] = useState<ReportContext | null>(null);

  const exitSelectMode = useCallback(() => setSelectMode(false), []);
  const toggleSelectMode = useCallback(
    () => setSelectMode((v) => !v),
    [],
  );

  // Escape cancels the select mode. Matches the standard modal
  // dismissal shortcut so it feels transparent.
  useEffect(() => {
    if (!isSelectMode) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSelectMode(false);
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [isSelectMode]);

  const selectRow = useCallback((ctx: ReportContext) => {
    setContext(ctx);
    setSelectMode(false);
  }, []);

  const trigger = useMemo(
    () => (
      <Chip
        icon={<MdOutlineFlag className="size-3" />}
        label={isSelectMode ? "Cancel selection" : "Report an issue"}
        tone={isSelectMode ? "amber" : "outline"}
        size="md"
        onClick={toggleSelectMode}
        title={
          isSelectMode
            ? `Cancel: click again or press Esc`
            : `Click a ${targetLabel} to report an issue with it`
        }
        aria-pressed={isSelectMode}
      />
    ),
    [isSelectMode, toggleSelectMode, targetLabel],
  );

  const banner = useMemo(
    () =>
      isSelectMode ? (
        <div
          role="status"
          className="mb-3 flex items-center justify-between gap-3 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-100"
        >
          <span>
            Click any {targetLabel} to report an issue with it.{" "}
            <span className="text-amber-200/70">
              (Esc to cancel)
            </span>
          </span>
          <Chip
            label="Cancel"
            tone="outline"
            size="sm"
            onClick={exitSelectMode}
          />
        </div>
      ) : null,
    [isSelectMode, exitSelectMode, targetLabel],
  );

  // Returns props to spread on each row. When select mode is off, the
  // row keeps its default interaction; when on, the row becomes a
  // primary click target that opens the report modal with its context.
  //
  // Rows with `disabled` (e.g. rows whose bioactivity has zero assays)
  // should skip the report affordance — pass disabled=true.
  const getRowProps = useCallback(
    (
      rowContext: ReportContext,
      opts: { disabled?: boolean } = {},
    ) => {
      if (!isSelectMode || opts.disabled) return {};
      return {
        onClick: (e: React.MouseEvent) => {
          // In select mode we own the click — stop it from also
          // triggering the row's default action (opening a nested
          // modal, navigating, etc.).
          e.stopPropagation();
          selectRow(rowContext);
        },
        className: twMerge(
          "cursor-pointer relative",
          // Amber outline echoes the banner + trigger tone so the
          // selectable-ness of each row reads at a glance.
          "outline-1 outline-dashed outline-amber-500/50 -outline-offset-1",
          "hover:bg-amber-500/10 hover:outline-amber-400",
          "transition-colors",
        ),
        // Screen readers announce each row as a button in select mode
        // so keyboard users understand what a click will do.
        role: "button" as const,
        tabIndex: 0,
        onKeyDown: (e: React.KeyboardEvent) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            selectRow(rowContext);
          }
        },
        "aria-label": `Report an issue with this ${targetLabel}`,
      };
    },
    [isSelectMode, selectRow, targetLabel],
  );

  // Sentinel is safe: the modal only opens when `context !== null`, and
  // when it does open we pass the real context. This just lets the type
  // stay strict on ReportIssueModal's `context: ReportContext` prop.
  const modal = (
    <ReportIssueModal
      isOpen={context !== null}
      onClose={() => setContext(null)}
      context={context ?? SENTINEL_CONTEXT}
    />
  );

  return {
    isSelectMode,
    trigger,
    banner,
    modal,
    getRowProps,
    selectRow,
    exitSelectMode,
  };
}

// Used only to satisfy ReportIssueModal's non-optional context prop
// while it's closed. Never surfaces to the user because isOpen=false.
const SENTINEL_CONTEXT: ReportContext = {
  kind: "food-composition-row",
  entityType: "food",
};

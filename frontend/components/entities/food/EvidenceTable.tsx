"use client";

// Dense evidence table for the food composition modal. Replaces the
// per-source Card list (FoodAtlasEvidence + FdcEvidence) with a single
// flat row-per-extraction table that mirrors the density + interaction
// patterns of the bioactivity Measurements modal:
//
// - one row per FoodEvidenceExtraction (flattened across FDC + FoodAtlas
//   evidence arrays; source badge disambiguates)
// - sortable ordering by converted-concentration value (desc default)
// - inline chips for method + trust warning + external paper link
// - expandable row surfaces the paper premise with the extraction terms
//   highlighted (only meaningful for FoodAtlas-Extraction rows)
// - responsive: desktop table falls back to a card list <md, sharing
//   the same source-of-truth column/spec

import { Fragment, useEffect, useMemo, useState } from "react";
import {
  MdChevronRight,
  MdKeyboardArrowLeft,
  MdKeyboardArrowRight,
  MdKeyboardDoubleArrowLeft,
  MdKeyboardDoubleArrowRight,
  MdOpenInNew,
  MdWarningAmber,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Chip from "@/components/basic/Chip";
import { useTableReporter } from "@/components/basic/useTableReporter";
import { AmbiguityIcon } from "@/components/basic/Ambiguity";
import {
  FoodEvidence,
  FoodEvidenceExtraction,
} from "@/types/Evidence";
import { formatConcentrationValueAlt } from "@/utils/utils";
import { greekVariants, matchesWithGreek } from "@/utils/greekLetters";

// A flat "one row per extraction" view. Keeps a back-pointer to its
// parent FoodEvidence so we can render the paper premise + reference
// on demand (expand row) without denormalising them into the row shape.
export type EvidenceRow = {
  extraction: FoodEvidenceExtraction;
  evidence: FoodEvidence;
  // Stable key that survives filter + sort passes.
  key: string;
};

interface Props {
  evidences: FoodEvidence[] | undefined;
  // Modal context — used to decide when to mute the Chemical column
  // (extracted name matches the pivot chemical) and to highlight the
  // premise.
  chemicalName: string;
}

// Numeric key for sort — falls back to -Infinity so rows without a
// numeric converted value settle at the bottom under "desc".
const convertedValue = (r: EvidenceRow): number => {
  const v = r.extraction.converted_concentration?.value;
  return typeof v === "number" && Number.isFinite(v) ? v : -Infinity;
};

const flattenEvidences = (
  evidences: FoodEvidence[] | undefined
): EvidenceRow[] => {
  if (!evidences?.length) return [];
  const rows: EvidenceRow[] = [];
  evidences.forEach((ev, evIdx) => {
    ev.extraction.forEach((ex, exIdx) => {
      rows.push({
        extraction: ex,
        evidence: ev,
        key: ex.attestation_id ?? `${ev.reference.id}-${evIdx}-${exIdx}`,
      });
    });
  });
  return rows;
};

const PAGE_SIZE = 20;

const EvidenceTable = ({ evidences, chemicalName }: Props) => {
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const reporter = useTableReporter({ targetLabel: "data point" });
  const toggle = (k: string) =>
    setExpandedKey((prev) => (prev === k ? null : k));

  const buildRowContext = (r: EvidenceRow) => ({
    kind: "food-composition-evidence" as const,
    entityType: "food" as const,
    attestationId: r.extraction.attestation_id,
    extractedChemical: r.extraction.extracted_chemical_name ?? undefined,
    extractedFood: r.extraction.extracted_food_name ?? undefined,
    concentration:
      typeof r.extraction.converted_concentration?.value === "number"
        ? String(r.extraction.converted_concentration.value)
        : undefined,
    referenceUrl: r.evidence.reference.url,
  });

  const rows = useMemo(() => {
    const flat = flattenEvidences(evidences);
    // Highest converted concentration first; ties fall back to source
    // so rows from the same paper stay adjacent.
    return flat.sort((a, b) => {
      const diff = convertedValue(b) - convertedValue(a);
      if (diff !== 0) return diff;
      return a.evidence.reference.id.localeCompare(b.evidence.reference.id);
    });
  }, [evidences]);

  const totalPages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
  // Filter change (or expand-collapse of last row of a filtered subset)
  // can shrink the row set below the current page — snap back to page 1
  // so we don't render an empty page.
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(1);
  }, [currentPage, totalPages]);
  // Any filter-driven change to the row set also collapses the active
  // expand — the previously expanded row may not even be present.
  useEffect(() => {
    setExpandedKey(null);
  }, [rows]);

  const pagedRows = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE;
    return rows.slice(start, start + PAGE_SIZE);
  }, [rows, currentPage]);

  if (rows.length === 0) {
    return (
      <div className="flex-1 min-h-0 flex items-center justify-center">
        <div className="rounded-md border border-dashed border-light-700/60 px-6 py-4 text-center text-sm text-light-400">
          No evidence available.
        </div>
      </div>
    );
  }

  return (
    // `flex-1 min-h-0` lets us take exactly the space the fullHeight
    // Modal offers; the inner scroll region keeps overflow inside the
    // modal instead of pushing the body past the panel bottom.
    <div className="flex-1 min-h-0 flex flex-col">
      <div className="flex items-center justify-end mb-2 shrink-0">
        {reporter.trigger}
      </div>
      {reporter.banner}
      <div className="flex-1 min-h-0 overflow-y-auto">
      {/* Desktop table -------------------------------------------------- */}
      <div className="hidden md:block">
        <table className="w-full table-fixed text-xs">
          <colgroup>
            <col className="w-[10%]" />
            <col className="w-[26%]" />
            <col className="w-[26%]" />
            <col className="w-[16%]" />
            <col className="w-[22%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr className="border-b border-light-700">
              {[
                "Source",
                "Chemical",
                "Concentration",
                "Method",
                "Reference",
              ].map((h) => (
                <th
                  key={h}
                  className="h-9 leading-none py-1.5 px-2 first:pl-0 last:pr-0 select-none uppercase text-[11px] font-medium"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {pagedRows.map((r) => {
              const expandable = r.evidence.premise?.length > 0;
              const isExpanded = expandedKey === r.key;
              const rowReportProps = reporter.getRowProps(buildRowContext(r));
              return (
                <Fragment key={r.key}>
                  <tr
                    onClick={
                      reporter.isSelectMode
                        ? rowReportProps.onClick
                        : expandable
                        ? () => toggle(r.key)
                        : undefined
                    }
                    aria-expanded={expandable ? isExpanded : undefined}
                    className={twMerge(
                      "transition-colors border-b border-light-800/60",
                      expandable && "cursor-pointer hover:bg-light-900/40",
                      isExpanded && "bg-light-900/50",
                      rowReportProps.className,
                    )}
                    role={rowReportProps.role}
                    tabIndex={rowReportProps.tabIndex}
                    onKeyDown={rowReportProps.onKeyDown}
                    aria-label={rowReportProps["aria-label"]}
                  >
                    <td className="py-2 pr-2 align-top">
                      <SourceBadge source={r.evidence.reference.source_name} />
                    </td>
                    <td className="py-2 px-2 align-top">
                      <ChemicalCell
                        extraction={r.extraction}
                        pivotChemical={chemicalName}
                      />
                    </td>
                    <td className="py-2 px-2 align-top">
                      <ConcentrationCell extraction={r.extraction} />
                    </td>
                    <td className="py-2 px-2 align-top">
                      <MethodChip method={r.extraction.method} />
                    </td>
                    <td className="py-2 pl-2 align-top">
                      <RowActions
                        row={r}
                        expandable={expandable}
                        expanded={isExpanded}
                        onToggle={() => toggle(r.key)}
                      />
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr>
                      <td
                        colSpan={5}
                        className="py-3 px-3 bg-light-900/30 border-l-2 border-l-accent-600 border-b border-light-700/40"
                      >
                        <ExpandedPremise row={r} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Mobile card list ---------------------------------------------- */}
      <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
        {pagedRows.map((r) => {
          const expandable = r.evidence.premise?.length > 0;
          const isExpanded = expandedKey === r.key;
          const rowReportProps = reporter.getRowProps(buildRowContext(r));
          return (
            <div
              key={r.key}
              {...rowReportProps}
              className={twMerge(
                "w-full py-3 flex flex-col gap-2 text-sm",
                rowReportProps.className,
              )}
            >
              <div className="w-full flex items-center justify-between gap-2 flex-wrap">
                <div className="flex items-center gap-2 min-w-0">
                  <SourceBadge source={r.evidence.reference.source_name} />
                  <span className="capitalize text-light-100 truncate">
                    {r.extraction.extracted_chemical_name || "—"}
                  </span>
                  <AmbiguityIcon
                    chemicalCandidates={r.extraction.chemical_candidates}
                  />
                  {r.extraction.trust_low && <TrustWarning />}
                </div>
                <RowActions
                  row={r}
                  expandable={expandable}
                  expanded={isExpanded}
                  onToggle={() => toggle(r.key)}
                />
              </div>
              <div className="w-full flex items-baseline justify-between gap-2">
                <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                  Concentration
                </span>
                <ConcentrationCell
                  extraction={r.extraction}
                  align="right"
                />
              </div>
              <div className="w-full flex items-center justify-between gap-2">
                <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                  Method
                </span>
                <MethodChip method={r.extraction.method} />
              </div>
              {isExpanded && (
                <div className="w-full pt-2 border-t border-l-2 border-l-accent-600 border-light-700/40 pl-3 pr-2 pb-1">
                  <ExpandedPremise row={r} />
                </div>
              )}
            </div>
          );
        })}
      </div>
      </div>
      {totalPages > 1 && (
        <div className="mt-3 shrink-0 max-w-xl w-full mx-auto flex items-center justify-between text-light-300">
          <Button
            isIconOnly
            isSquared
            isDisabled={currentPage === 1}
            onClick={() => setCurrentPage(1)}
            aria-label="First page"
          >
            <MdKeyboardDoubleArrowLeft />
          </Button>
          <Button
            isIconOnly
            isSquared
            isDisabled={currentPage === 1}
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            aria-label="Previous page"
          >
            <MdKeyboardArrowLeft />
          </Button>
          <span className="w-40 text-center text-sm">
            Page {currentPage} of {totalPages}
          </span>
          <Button
            isIconOnly
            isSquared
            isDisabled={currentPage === totalPages}
            onClick={() =>
              setCurrentPage((p) => Math.min(totalPages, p + 1))
            }
            aria-label="Next page"
          >
            <MdKeyboardArrowRight />
          </Button>
          <Button
            isIconOnly
            isSquared
            isDisabled={currentPage === totalPages}
            onClick={() => setCurrentPage(totalPages)}
            aria-label="Last page"
          >
            <MdKeyboardDoubleArrowRight />
          </Button>
        </div>
      )}
      {reporter.modal}
    </div>
  );
};

EvidenceTable.displayName = "EvidenceTable";
export default EvidenceTable;

// -------- Cells + badges (kept inline; only used by this table) ---------

const SourceBadge = ({ source }: { source: string }) => {
  const isFdc = source === "FDC";
  return (
    <span
      className={twMerge(
        "inline-flex items-center justify-center rounded-full border px-2 py-[0.05rem] text-[10px] font-mono tracking-wide whitespace-nowrap",
        isFdc
          ? "text-sky-400 border-sky-500/60 bg-sky-500/10"
          : "text-accent-400 border-accent-500/60 bg-accent-500/10"
      )}
      title={isFdc ? "USDA FDC database" : "FoodAtlas literature extraction"}
    >
      {isFdc ? "FDC" : "FoodAtlas"}
    </span>
  );
};

const ChemicalCell = ({
  extraction,
  pivotChemical,
}: {
  extraction: FoodEvidenceExtraction;
  pivotChemical: string;
}) => {
  const name = extraction.extracted_chemical_name;
  if (!name) return <span className="text-light-600">—</span>;
  // Same-as-pivot rows are muted so eyes skim past them to the rows
  // whose extraction differs (aliases, ambiguous mappings, etc).
  const isPivot = name.trim().toLowerCase() === pivotChemical.trim().toLowerCase();
  return (
    <span
      className={twMerge(
        "inline-flex items-center gap-1 align-middle capitalize break-words",
        isPivot ? "text-light-400" : "text-light-100"
      )}
    >
      {name}
      <AmbiguityIcon chemicalCandidates={extraction.chemical_candidates} />
      {extraction.trust_low && <TrustWarning />}
    </span>
  );
};

const ConcentrationCell = ({
  extraction,
  align = "left",
}: {
  extraction: FoodEvidenceExtraction;
  align?: "left" | "right";
}) => {
  const raw = extraction.extracted_concentration;
  const c = extraction.converted_concentration;
  const converted =
    c?.unit && c?.value
      ? `${formatConcentrationValueAlt(c.value)} ${c.unit}`
      : null;
  return (
    <div className={twMerge("flex flex-col", align === "right" && "items-end")}>
      <span className="text-light-200 font-mono tabular-nums whitespace-nowrap">
        {raw ?? "—"}
      </span>
      {converted && (
        <span className="text-[10px] text-light-500 font-mono tabular-nums whitespace-nowrap">
          {converted}
        </span>
      )}
    </div>
  );
};

const MethodChip = ({ method }: { method: string | null | undefined }) => {
  if (!method) return <span className="text-light-600">—</span>;
  return (
    <span className="inline-flex items-center rounded border border-light-700/60 bg-light-900/40 px-1.5 py-[0.1rem] font-mono text-[10px] uppercase tracking-wider text-light-300 break-words">
      {method}
    </span>
  );
};

const TrustWarning = () => (
  <MdWarningAmber
    className="size-3.5 text-rose-400 shrink-0"
    aria-label="Low-trust extraction"
    title="Low-trust extraction"
  />
);

const RowActions = ({
  row,
  expandable,
  expanded,
  onToggle,
}: {
  row: EvidenceRow;
  expandable: boolean;
  expanded: boolean;
  onToggle: () => void;
}) => {
  const isFdc = row.evidence.reference.source_name === "FDC";
  const linkLabel = isFdc ? "View source" : "View paper";
  return (
    <div className="flex items-center justify-end gap-2 flex-wrap">
      <a
        href={row.evidence.reference.url}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1 text-xs font-mono italic text-light-300 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
        onClick={(e) => e.stopPropagation()}
        aria-label={linkLabel}
        title={row.evidence.reference.display_name}
      >
        {linkLabel}
        <MdOpenInNew className="size-3 shrink-0" aria-hidden />
      </a>
      {expandable && (
        <Chip
          icon={
            <MdChevronRight
              className={twMerge(
                "size-3.5 transition-transform duration-150",
                expanded && "rotate-90"
              )}
            />
          }
          label={expanded ? "Hide" : "Premise"}
          tone={expanded ? "cream" : "outline"}
          size="sm"
          onClick={(e) => {
            e.stopPropagation();
            onToggle();
          }}
          aria-label={expanded ? "Hide premise" : "Show premise"}
          aria-pressed={expanded}
        />
      )}
    </div>
  );
};

const ExpandedPremise = ({ row }: { row: EvidenceRow }) => {
  const { evidence } = row;
  const highlighted = useMemo(
    () => highlightPremise(evidence),
    [evidence]
  );
  return (
    <div className="flex flex-col gap-1.5">
      <p className="text-xs leading-snug font-serif italic text-light-300">
        &ldquo;{highlighted}&rdquo;
      </p>
      {evidence.reference.display_name && (
        <p className="text-[10px] font-mono italic text-light-500">
          — {evidence.reference.display_name}
        </p>
      )}
    </div>
  );
};

// Splits the premise on any of the extraction's terms (with greek
// variants) and colour-codes chemical / food / concentration matches.
// Same routine the old FoodAtlasEvidence used, hoisted here so the
// flat row's expanded view carries the same highlighting behaviour.
const highlightPremise = (evidence: FoodEvidence): React.ReactNode => {
  const terms = evidence.extraction.flatMap((e) =>
    [
      e.extracted_chemical_name,
      e.extracted_food_name,
      e.extracted_concentration,
    ].flatMap((name) => greekVariants(name))
  );
  if (terms.length === 0) return evidence.premise;
  const regex = new RegExp(`(${terms.join("|")})`, "gi");
  return evidence.premise.split(regex).map((part, index) => {
    const match = evidence.extraction.find(
      (e) =>
        matchesWithGreek(part, e.extracted_food_name) ||
        matchesWithGreek(part, e.extracted_chemical_name) ||
        matchesWithGreek(part, e.extracted_concentration)
    );
    if (matchesWithGreek(part, match?.extracted_food_name)) {
      return (
        <span key={index} className="text-amber-500 bg-amber-500/10">
          {part}
        </span>
      );
    }
    if (matchesWithGreek(part, match?.extracted_chemical_name)) {
      return (
        <span key={index} className="text-cyan-400 bg-cyan-500/10">
          {part}
        </span>
      );
    }
    if (matchesWithGreek(part, match?.extracted_concentration)) {
      return (
        <span key={index} className="text-teal-400 bg-teal-500/10">
          {part}
        </span>
      );
    }
    return part;
  });
};

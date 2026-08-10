"use client";

// Presentation half of the disease Bioactivities tab. Rows arrive already
// ordered by the API (dietary-backed first, then by dose margin), so this
// component never re-sorts — it only renders and paginates the tail.

import { MdKeyboardArrowDown, MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Link from "@/components/basic/Link";
import Tooltip from "@/components/basic/Tooltip";
import { formatEfficacyFraction } from "@/components/entities/bioactivity/efficacy";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import type { DiseaseBioactivityChemical, DietaryDose } from "@/types";

interface Props {
  rows: DiseaseBioactivityChemical[];
  visibleCount: number;
  onShowAll: () => void;
}

const entityHref = (kind: string, name: string) =>
  `/${kind}/${encodeURIComponent(encodeSpace(name))}`;

// log10(dietary dose ÷ AC50). Positive means the dose clears the active
// threshold; the bigger the number, the more headroom.
const formatMargin = (value: number | null | undefined) => {
  if (value == null || Number.isNaN(value)) return "—";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}`;
};

const DoseCell = ({ dietary }: { dietary: DietaryDose | null }) => {
  if (!dietary) {
    return (
      <span className="text-light-600 text-xs font-mono italic">
        not in food
      </span>
    );
  }
  const suspect = dietary.conc_quality_flag === "suspect_high";
  return (
    <div className="flex flex-col gap-0.5">
      <Link href={entityHref("food", dietary.food_name)} isExternal={false}>
        <span className="capitalize">{dietary.food_name}</span>
      </Link>
      <span className="inline-flex items-center gap-1 font-mono text-[11px] text-light-500 tabular-nums">
        {formatConcentrationValueAlt(
          dietary.food_conc_mg_per_100g ?? undefined
        )}{" "}
        mg/100g
        {suspect && (
          <Tooltip
            content={
              <p>
                Upstream flagged this concentration
                <br /> as implausibly high (&gt;10% of the
                <br /> food by mass). The efficacy below
                <br /> is derived from it.
              </p>
            }
          >
            <MdWarningAmber
              className="size-3 text-amber-400"
              aria-label="Concentration flagged as implausibly high"
            />
          </Tooltip>
        )}
      </span>
    </div>
  );
};

const EfficacyChip = ({ dietary }: { dietary: DietaryDose | null }) => {
  if (!dietary || dietary.efficacy_fraction == null) {
    return <span className="text-light-600">—</span>;
  }
  const above = dietary.conc_vs_ac50 === "above";
  return (
    <span className="inline-flex items-baseline gap-2">
      <span
        className={twMerge(
          "font-mono italic uppercase tracking-wider text-[10px] px-1.5 py-[1px] rounded-full border",
          above
            ? "text-emerald-300 border-emerald-500/70 bg-emerald-500/10"
            : "text-light-400 border-light-700 bg-light-800/40"
        )}
      >
        {dietary.conc_vs_ac50 ?? "—"}
      </span>
      <span className="font-mono tabular-nums text-xs text-light-300">
        {formatEfficacyFraction(dietary.efficacy_fraction)}
      </span>
    </span>
  );
};

const DiseaseBioactivityTable = ({ rows, visibleCount, onShowAll }: Props) => {
  const visible = rows.slice(0, visibleCount);
  const hiddenCount = rows.length - visible.length;

  return (
    <div className="flex flex-col gap-4">
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[16%]" />
            <col className="w-[26%]" />
            <col className="w-[8%]" />
            <col className="w-[22%]" />
            <col className="w-[16%]" />
            <col className="w-[12%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>Bioactivity</Th>
              <Th>Chemical</Th>
              <Th
                align="right"
                title="Bridging assays linking this chemical and bioactivity to the disease"
              >
                Assays
              </Th>
              <Th title="The food where this chemical's dietary dose comes closest to its active threshold">
                Best dietary source
              </Th>
              <Th title="Fraction of maximal response at that food's concentration">
                Efficacy
              </Th>
              <Th
                align="right"
                title="log10(dietary dose ÷ AC50) — how far the dose clears the active threshold"
              >
                Dose vs AC50
              </Th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {visible.map((row) => (
              <tr key={`${row.bioactivity_foodatlas_id}-${row.chemical_foodatlas_id}`}>
                <td className="py-1.5 pr-4">
                  <div className="flex min-h-9 items-center capitalize">
                    <Link
                      href={entityHref("bioactivity", row.bioactivity_name)}
                      isExternal={false}
                    >
                      {row.bioactivity_name}
                    </Link>
                  </div>
                </td>
                <td className="py-1.5 px-4">
                  <div className="flex min-h-9 items-center capitalize break-words">
                    <Link
                      href={entityHref("chemical", row.chemical_name)}
                      isExternal={false}
                    >
                      {row.chemical_name}
                    </Link>
                  </div>
                </td>
                <td className="py-1.5 px-4 text-right tabular-nums text-light-200">
                  {row.n_assays.toLocaleString()}
                </td>
                <td className="py-1.5 px-4">
                  <DoseCell dietary={row.dietary} />
                </td>
                <td className="py-1.5 px-4">
                  <EfficacyChip dietary={row.dietary} />
                </td>
                <td className="py-1.5 px-4 text-right font-mono tabular-nums text-xs text-light-300">
                  {formatMargin(row.dietary?.dose_over_ac50_log)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile cards */}
      <div className="md:hidden divide-y divide-light-800">
        {visible.map((row) => (
          <div
            key={`${row.bioactivity_foodatlas_id}-${row.chemical_foodatlas_id}`}
            className="py-3 flex flex-col gap-2"
          >
            <div className="flex items-baseline justify-between gap-2 capitalize">
              <Link
                href={entityHref("chemical", row.chemical_name)}
                isExternal={false}
              >
                {row.chemical_name}
              </Link>
              <Link
                href={entityHref("bioactivity", row.bioactivity_name)}
                isExternal={false}
              >
                <span className="text-[11px] font-mono italic text-light-400">
                  {row.bioactivity_name}
                </span>
              </Link>
            </div>
            <CardRow label="Assays">
              <span className="tabular-nums text-light-200">
                {row.n_assays.toLocaleString()}
              </span>
            </CardRow>
            <CardRow label="Best dietary source">
              <DoseCell dietary={row.dietary} />
            </CardRow>
            {row.dietary && (
              <>
                <CardRow label="Efficacy">
                  <EfficacyChip dietary={row.dietary} />
                </CardRow>
                <CardRow label="Dose vs AC50">
                  <span className="tabular-nums text-light-300">
                    {formatMargin(row.dietary.dose_over_ac50_log)}
                  </span>
                </CardRow>
              </>
            )}
          </div>
        ))}
      </div>

      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={onShowAll}
          className="self-center inline-flex items-center gap-1 text-xs font-mono italic text-light-400 hover:text-light-100 transition-colors"
        >
          Show all {rows.length.toLocaleString()} rows
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

const Th = ({
  children,
  align,
  title,
}: {
  children: React.ReactNode;
  align?: "right";
  title?: string;
}) => (
  <th
    title={title}
    className={twMerge(
      "h-9 border-b border-light-700 py-1.5 px-4 uppercase text-xs font-medium",
      align === "right" && "text-right",
      !align && "text-left"
    )}
  >
    {children}
  </th>
);

const CardRow = ({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) => (
  <div className="flex items-baseline justify-between gap-3 text-[11px] font-mono">
    <span className="text-light-500 shrink-0">{label}</span>
    <span className="text-right">{children}</span>
  </div>
);

DiseaseBioactivityTable.displayName = "DiseaseBioactivityTable";
export default DiseaseBioactivityTable;

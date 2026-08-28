"use client";

// Row renderers for the CTD literature correlation table, split out of
// CorrelationTable so that file stays under the 300-line rule once it
// grew a direction column.
//
// Direction now comes from the row itself (`relationship_id`) rather
// than from which query produced it: the merged Diseases/Chemicals tab
// renders one table carrying both r3 and r4 rows, with the direction as
// a column and a sidebar facet, instead of a table per direction.

import { MdAdd, MdDescription, MdRemove } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Chip from "@/components/basic/Chip";
import EntitySiblingIcon from "@/components/basic/EntitySiblingIcon";
import Link from "@/components/basic/Link";
import { encodeSpace } from "@/utils/utils";
import { ChemicalCorrelation } from "@/types";

// "all" is a filter value, not a row value — a row is always one or the
// other. `rowDirection` narrows it.
export type CorrelationDirection = "all" | "positive" | "negative";

// r4 helps reduce the disease, r3 worsens it. Mirrors RELATION_IDS in
// backend/api/src/repositories/_correlation.py.
//
// "mixed" is a real answer, not a fallback: ~4% of pairs have been
// reported both ways, and collapsing that to one direction would pick a
// side the literature doesn't. It reads as a caveat on the row rather
// than a claim.
export type RowDirection = "positive" | "negative" | "mixed";

export const rowDirection = (
  row: Pick<ChemicalCorrelation, "relationship_ids">
): RowDirection => {
  const ids = row.relationship_ids ?? [];
  const improves = ids.includes("r4");
  const worsens = ids.includes("r3");
  if (improves && worsens) return "mixed";
  return worsens ? "negative" : "positive";
};

// The row's publications. Prefers the server-computed union — it dedupes
// papers cited for both directions — and falls back to concatenating the
// two split arrays so a row that arrives without it still renders instead
// of taking the whole tab down.
export const rowEvidences = (row: ChemicalCorrelation) =>
  row.evidences ?? [
    ...(row.improves_evidences ?? []),
    ...(row.worsens_evidences ?? []),
  ];

const entityHref = (kind: "chemical" | "disease", name: string) =>
  `/${kind}/${encodeURIComponent(encodeSpace(name))}`;

// Whether this row's evidence comes from a chemical other than the one
// whose page we're on. True only on ChEBI class pages ("polyphenol" rolls
// up 124 different source chemicals); on a leaf page like "caffeine" every
// row names the page itself, which is why the column is hidden there.
export const hasDistinctSource = (
  row: Pick<ChemicalCorrelation, "source_chemical_name">,
  commonName: string
): boolean =>
  Boolean(
    row.source_chemical_name &&
      row.source_chemical_name.toLowerCase() !== commonName.toLowerCase()
  );

const BADGE_SHAPE =
  "w-[1.2rem] h-[1.2rem] shrink-0 flex justify-center items-center rounded-full border-[1.5px] shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold";

export const SignBadge = ({ direction }: { direction: RowDirection }) =>
  direction === "mixed" ? (
    // Amber and "±" rather than a third colour with its own meaning —
    // this is the two existing signs held together, and reads that way
    // next to a lime + and a red −.
    <div
      className={`${BADGE_SHAPE} border-amber-500 text-amber-400 bg-amber-500/10 shadow-amber-700/50 text-[0.7rem] leading-none`}
      aria-hidden
    >
      ±
    </div>
  ) : direction === "negative" ? (
    <div className="w-[1.2rem] h-[1.2rem] shrink-0 flex justify-center items-center rounded-full border-[1.5px] border-red-600 text-red-600 bg-red-600/10 shadow-red-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
      <MdRemove />
    </div>
  ) : (
    <div className="w-[1.2rem] h-[1.2rem] shrink-0 flex justify-center items-center rounded-full border-[1.5px] border-lime-600 text-lime-600 bg-lime-600/10 shadow-lime-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
      <MdAdd />
    </div>
  );

export const DIRECTION_LABEL: Record<RowDirection, string> = {
  positive: "Improves",
  negative: "Worsens",
  mixed: "Mixed",
};

// The word next to the badge. Spelled out because a bare +/- circle was
// read as "add"/"remove" rather than as a claim direction.
export const DirectionCell = ({
  direction,
}: {
  direction: RowDirection;
}) => (
  <div className="flex gap-2.5 min-h-9 items-center">
    <SignBadge direction={direction} />
    <span className="text-light-400">{DIRECTION_LABEL[direction]}</span>
  </div>
);

// One button rather than three inline PMIDs plus an overflow chip. The
// preview was three arbitrary ids out of up to several hundred — not a
// sample anyone could act on, and it made the column's width depend on
// how many digits a PMID happens to have. The modal already listed every
// one; this just makes it the only way in.
export const EvidenceButton = ({
  row,
  onOpen,
}: {
  row: ChemicalCorrelation;
  onOpen: () => void;
}) => {
  const n = rowEvidences(row).length;
  return (
    <Chip
      icon={<MdDescription className="size-3" />}
      label={`See ${n.toLocaleString()} publication${n === 1 ? "" : "s"}`}
      tone="outline"
      size="md"
      onClick={onOpen}
      disabled={n === 0}
    />
  );
};

interface RowProps {
  row: ChemicalCorrelation;
  // The entity type each row LINKS to: "disease" on a chemical page.
  peer: "chemical" | "disease";
  // Whether the source-chemical column is rendered at all. Decided once
  // per page from the whole result set, so the column doesn't appear and
  // disappear between pages.
  showSource: boolean;
  commonName: string;
  rowProps: Record<string, unknown> & { className?: string };
  onShowMore: () => void;
}

export const CorrelationDesktopRow = ({
  row,
  peer,
  showSource,
  commonName,
  rowProps,
  onShowMore,
}: RowProps) => (
  <tr {...rowProps}>
    <td className="py-1.5 pr-4">
      <DirectionCell direction={rowDirection(row)} />
    </td>
    <td className="py-1.5 px-4">
      <div className="flex gap-2.5 min-h-9 capitalize items-center">
        <Link
          className="capitalize"
          href={entityHref(peer, row.name)}
          isExternal={false}
        >
          {row.name}
        </Link>
        {peer === "chemical" && (
          <EntitySiblingIcon
            siblings={row.ambiguity_siblings}
            entityKind="chemical"
          />
        )}
      </div>
    </td>
    {showSource && (
      <td className="py-1.5 px-4">
        <div className="flex min-h-9 capitalize items-center">
          <Link
            className="capitalize"
            href={entityHref(
              "chemical",
              row.source_chemical_name ?? commonName
            )}
            isExternal={false}
          >
            {row.source_chemical_name ?? commonName}
          </Link>
        </div>
      </td>
    )}
    <td className="py-1.5 pl-4">
      <div className="flex min-h-9 items-center justify-end">
        <EvidenceButton row={row} onOpen={onShowMore} />
      </div>
    </td>
  </tr>
);

// Mobile card. Primary line is direction + the linked peer entity;
// evidence collapses to one button so the row never overflows. The
// source chemical only earns a line when it differs from the page.
export const CorrelationCard = ({
  row,
  peer,
  showSource,
  commonName,
  rowProps,
  onShowMore,
}: RowProps) => (
  <div
    {...rowProps}
    className={twMerge("w-full py-3 flex flex-col gap-2", rowProps.className)}
  >
    <div className="w-full flex items-center gap-2 flex-wrap capitalize">
      <SignBadge direction={rowDirection(row)} />
      <Link
        className="capitalize"
        href={entityHref(peer, row.name)}
        isExternal={false}
      >
        {row.name}
      </Link>
      {peer === "chemical" && (
        <EntitySiblingIcon
          siblings={row.ambiguity_siblings}
          entityKind="chemical"
        />
      )}
    </div>
    {showSource && hasDistinctSource(row, commonName) && (
      <div className="w-full flex items-baseline justify-between gap-2 text-sm">
        <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
          Via
        </span>
        <Link
          className="capitalize text-right"
          href={entityHref("chemical", row.source_chemical_name ?? commonName)}
          isExternal={false}
        >
          {row.source_chemical_name}
        </Link>
      </div>
    )}
    <div className="w-full flex items-center justify-between gap-2 text-sm">
      <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
        Publications
      </span>
      <EvidenceButton row={row} onOpen={onShowMore} />
    </div>
  </div>
);

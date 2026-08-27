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
export const rowDirection = (
  row: Pick<ChemicalCorrelation, "relationship_id">
): "positive" | "negative" =>
  row.relationship_id === "r3" ? "negative" : "positive";

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

export const SignBadge = ({
  direction,
}: {
  direction: "positive" | "negative";
}) =>
  direction === "negative" ? (
    <div className="w-[1.2rem] h-[1.2rem] shrink-0 flex justify-center items-center rounded-full border-[1.5px] border-red-600 text-red-600 bg-red-600/10 shadow-red-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
      <MdRemove />
    </div>
  ) : (
    <div className="w-[1.2rem] h-[1.2rem] shrink-0 flex justify-center items-center rounded-full border-[1.5px] border-lime-600 text-lime-600 bg-lime-600/10 shadow-lime-800/50 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6) font-bold">
      <MdAdd />
    </div>
  );

// The word next to the badge. Spelled out because a bare +/- circle was
// read as "add"/"remove" rather than as a claim direction.
export const DirectionCell = ({
  direction,
}: {
  direction: "positive" | "negative";
}) => (
  <div className="flex gap-2.5 min-h-9 items-center">
    <SignBadge direction={direction} />
    <span className="text-light-400">
      {direction === "negative" ? "Worsens" : "Improves"}
    </span>
  </div>
);

const EvidenceLinks = ({
  row,
  onShowMore,
}: {
  row: ChemicalCorrelation;
  onShowMore: () => void;
}) => (
  <div className="flex gap-2 justify-end items-center flex-nowrap">
    {row.evidences.slice(0, 3).map((evidence) => (
      <Link
        className="whitespace-nowrap"
        key={evidence.pmid?.id ?? evidence.pmcid?.id}
        href={evidence.pmid?.url ?? evidence.pmcid?.url}
        isExternal
      >
        {evidence.pmid?.id ?? evidence.pmcid?.id}
      </Link>
    ))}
    {row.evidences.length > 3 && (
      <Chip
        icon={<MdDescription className="size-3" />}
        label={`${row.evidences.length - 3} more...`}
        tone="outline"
        size="md"
        onClick={onShowMore}
      />
    )}
  </div>
);

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
    <td className="py-1.5 pl-4">
      <div className="flex min-h-9 capitalize items-center justify-end">
        <EvidenceLinks row={row} onShowMore={onShowMore} />
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
        Evidence
      </span>
      <Chip
        icon={<MdDescription className="size-3" />}
        label={`${row.evidences.length} PMID${
          row.evidences.length === 1 ? "" : "s"
        }`}
        tone="outline"
        size="md"
        onClick={onShowMore}
      />
    </div>
  </div>
);

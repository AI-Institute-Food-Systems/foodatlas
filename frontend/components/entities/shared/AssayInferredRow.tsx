"use client";

// Row and card presentation for the assay-inferred association tables.
// Split out of AssayInferredAssociationsTable so that component stays a
// container (fetch, paginate, publish tab count) and this file owns the
// six columns and their mobile equivalent.

import { MdArrowForward, MdBiotech, MdMyLocation } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import { CardRow, CountCell } from "@/components/entities/shared/EvidenceTable";
import LiteratureBadge from "@/components/entities/shared/LiteratureBadge";
import SignalChips from "@/components/entities/shared/SignalChips";
import { encodeSpace } from "@/utils/utils";
import type { AssayInferredAssociation } from "@/types";

// Which side of the pair the *other* entity is on — determines which
// name/id to render and where its detail page lives.
export type PeerDirection = "disease" | "chemical";

export const peerId = (row: AssayInferredAssociation, peer: PeerDirection) =>
  peer === "disease" ? row.disease_foodatlas_id : row.chemical_foodatlas_id;

export const peerName = (row: AssayInferredAssociation, peer: PeerDirection) =>
  peer === "disease" ? row.disease_name : row.chemical_name;

const peerHref = (row: AssayInferredAssociation, peer: PeerDirection) =>
  `/${peer}/${encodeURIComponent(encodeSpace(peerName(row, peer)))}`;

// Pairs the assay-side direction with the literature verdict. The badge is
// null — and so renders nothing — for the ~97.5% of pairs the literature
// doesn't cover, which keeps the column from filling with "unknown".
const SignalCell = ({ row }: { row: AssayInferredAssociation }) => (
  <span className="inline-flex flex-wrap items-baseline gap-1">
    <SignalChips relationships={row.relationships} />
    <LiteratureBadge
      relationships={row.relationships}
      literatureDirections={row.literature_directions}
    />
  </span>
);

// Counts come from the row's own arrays for targets, but from n_assays
// for assays — the stored assay list is capped upstream while the count
// is not, so the button must promise the real total.
const CountButton = ({
  n,
  noun,
  icon,
  onOpen,
}: {
  n: number;
  noun: string;
  icon: React.ReactNode;
  onOpen: () => void;
}) =>
  n === 0 ? null : (
    <Chip
      icon={icon}
      label={`See ${n.toLocaleString()} ${noun}${n === 1 ? "" : "s"}`}
      tone="outline"
      size="md"
      onClick={onOpen}
    />
  );

type RowProps = {
  row: AssayInferredAssociation;
  peer: PeerDirection;
  reportProps: Record<string, unknown>;
  onOpenTargets: () => void;
  onOpenAssays: () => void;
};

export const PeerRow = ({
  row,
  peer,
  reportProps,
  onOpenTargets,
  onOpenAssays,
}: RowProps) => (
  <tr {...reportProps}>
    <td className="py-1.5 pr-4">
      <div className="flex min-h-9 items-center capitalize">
        <Link href={peerHref(row, peer)} isExternal={false}>
          {peerName(row, peer)}
        </Link>
      </div>
    </td>
    <td className="py-1.5 px-4 text-right">
      <CountCell value={row.n_assays} />
    </td>
    <td className="py-1.5 px-4">
      <SignalCell row={row} />
    </td>
    <td className="py-1.5 px-4">
      <CountButton
        n={row.targets?.length ?? 0}
        noun="target"
        icon={<MdMyLocation className="size-3" />}
        onOpen={onOpenTargets}
      />
    </td>
    <td className="py-1.5 px-4">
      <CountButton
        n={row.n_assays}
        noun="assay"
        icon={<MdBiotech className="size-3" />}
        onOpen={onOpenAssays}
      />
    </td>
  </tr>
);

export const PeerCard = ({
  row,
  peer,
  reportProps,
  onOpenTargets,
  onOpenAssays,
}: RowProps) => (
  <div className="py-3 flex flex-col gap-2" {...reportProps}>
    <div className="flex items-baseline justify-between gap-2 capitalize">
      <Link href={peerHref(row, peer)} isExternal={false}>
        {peerName(row, peer)}
      </Link>
      <MdArrowForward className="w-3.5 h-3.5 text-light-500 shrink-0" />
    </div>
    <CardRow label="Assays">
      <CountCell value={row.n_assays} />
    </CardRow>
    <div>
      <SignalCell row={row} />
    </div>
    {!!row.targets?.length && (
      <CardRow label="Target">
        <CountButton
          n={row.targets.length}
          noun="target"
          icon={<MdMyLocation className="size-3" />}
          onOpen={onOpenTargets}
        />
      </CardRow>
    )}
    {!!row.assays?.length && (
      <CardRow label="Assays">
        <CountButton
          n={row.n_assays}
          noun="assay"
          icon={<MdBiotech className="size-3" />}
          onOpen={onOpenAssays}
        />
      </CardRow>
    )}
  </div>
);

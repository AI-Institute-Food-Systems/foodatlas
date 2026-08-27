"use client";

// Row and card presentation for the assay-inferred association tables.
// Split out of AssayInferredAssociationsTable so that component stays a
// container (fetch, paginate, publish tab count) and this file owns the
// six columns and their mobile equivalent.

import { MdArrowForward } from "react-icons/md";

import Link from "@/components/basic/Link";
import AssayEvidenceLinks from "@/components/entities/shared/AssayEvidenceLinks";
import { CardRow, CountCell } from "@/components/entities/shared/EvidenceTable";
import LiteratureBadge from "@/components/entities/shared/LiteratureBadge";
import SignalChips from "@/components/entities/shared/SignalChips";
import TargetGeneChips from "@/components/entities/shared/TargetGeneChips";
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

type RowProps = {
  row: AssayInferredAssociation;
  peer: PeerDirection;
  reportProps: Record<string, unknown>;
};

export const PeerRow = ({ row, peer, reportProps }: RowProps) => (
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
    <td className="py-1.5 px-4 text-right">
      <CountCell value={row.n_active_measurements} tone="text-emerald-300" />
    </td>
    <td className="py-1.5 px-4">
      <SignalCell row={row} />
    </td>
    <td className="py-1.5 px-4">
      <TargetGeneChips targets={row.targets} />
    </td>
    <td className="py-1.5 px-4">
      <AssayEvidenceLinks assays={row.assays} totalCount={row.n_assays} />
    </td>
  </tr>
);

export const PeerCard = ({ row, peer, reportProps }: RowProps) => (
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
    <CardRow label="Active">
      <CountCell value={row.n_active_measurements} tone="text-emerald-300" />
    </CardRow>
    <div>
      <SignalCell row={row} />
    </div>
    {!!row.targets?.length && (
      <CardRow label="Target">
        <TargetGeneChips targets={row.targets} visible={2} />
      </CardRow>
    )}
    {!!row.assays?.length && (
      <CardRow label="Evidence">
        <AssayEvidenceLinks
          assays={row.assays}
          totalCount={row.n_assays}
          visible={1}
        />
      </CardRow>
    )}
  </div>
);

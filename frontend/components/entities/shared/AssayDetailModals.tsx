"use client";

// Full lists behind the Target and Assays columns of the assay-inferred
// table.
//
// Both columns used to render two or three items inline plus a "+N"
// tooltip. A tooltip is the wrong home for the tail: it can't be reached
// on touch, can't be copied out of, and the visible items were an
// arbitrary slice rather than the important ones. Same treatment as the
// Publications column on the literature table above — the cell is a
// button that says how many, the modal holds all of them.

import { MdMyLocation } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import AssayIcon from "@/components/icons/AssayIcon";
import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import { targetUrl } from "@/components/entities/shared/TargetGeneChips";
import { assayExternalUrl } from "@/utils/utils";
import type { AssayTarget } from "@/types";

// The cell that opens one of these. Shared so every table that shows
// targets or assays states its count the same way.
//
// The count is passed in rather than derived from the list: for assays
// they differ, because the stored list is capped at 25 upstream while
// n_assays is not. The button must promise the real total.
export const DetailCountButton = ({
  n,
  noun,
  onOpen,
}: {
  n: number;
  noun: "target" | "assay";
  onOpen: () => void;
}) =>
  n === 0 ? null : (
    <Chip
      icon={
        noun === "assay" ? <AssayIcon /> : <MdMyLocation className="size-3" />
      }
      label={`See ${n.toLocaleString()} ${noun}${n === 1 ? "" : "s"}`}
      tone="outline"
      size="md"
      onClick={onOpen}
    />
  );

interface TargetsModalProps {
  targets: AssayTarget[];
  peerName: string;
  isOpen: boolean;
  onClose: () => void;
}

export const AssayTargetsModal = ({
  targets,
  peerName,
  isOpen,
  onClose,
}: TargetsModalProps) => (
  <Modal
    fullHeight
    title="Protein targets"
    description={
      <p className="text-light-300">
        The proteins the bridging assays measure for{" "}
        <span className="font-semibold capitalize">{peerName}</span> — what
        the association actually runs through, rather than an assertion that
        one exists.
      </p>
    }
    isOpen={isOpen}
    onClose={onClose}
  >
    <ul className="flex flex-col divide-y divide-light-800">
      {targets.map((target) => {
        const url = targetUrl(target.id);
        return (
          <li
            key={target.id}
            className="py-2 flex items-baseline justify-between gap-4"
          >
            {/* The label is free text and runs long, so it wraps here
             * rather than truncating as it must in the table cell. */}
            <span className="text-sm text-light-300">
              {target.label ?? target.id}
            </span>
            {url ? (
              <Link
                href={url}
                className="text-[11px] font-mono whitespace-nowrap shrink-0"
              >
                {target.id}
              </Link>
            ) : (
              <span className="text-[11px] font-mono text-light-500 whitespace-nowrap shrink-0">
                {target.id}
              </span>
            )}
          </li>
        );
      })}
    </ul>
  </Modal>
);

AssayTargetsModal.displayName = "AssayTargetsModal";

interface AssaysModalProps {
  assays: string[];
  // Total behind the row. Larger than assays.length when the materializer
  // capped the list (ASSAY_CAP = 25) — say so rather than implying the
  // list is exhaustive.
  totalCount?: number;
  peerName: string;
  isOpen: boolean;
  onClose: () => void;
}

export const AssaysModal = ({
  assays,
  totalCount,
  peerName,
  isOpen,
  onClose,
}: AssaysModalProps) => {
  const total = totalCount ?? assays.length;
  const capped = total > assays.length;
  return (
    <Modal
      fullHeight
      title="Source assays"
      description={
        <p className="text-light-300">
          The assays behind the association with{" "}
          <span className="font-semibold capitalize">{peerName}</span>. Each
          links to its source record, so the claim can be audited directly.
          {capped && (
            <>
              {" "}
              Showing {assays.length.toLocaleString()} of{" "}
              {total.toLocaleString()} — the stored list is capped.
            </>
          )}
        </p>
      }
      isOpen={isOpen}
      onClose={onClose}
    >
      <div className="flex gap-2 flex-wrap">
        {assays.map((assay) => {
          const ext = assayExternalUrl(assay);
          return ext ? (
            <Link key={assay} href={ext.url} className="font-mono text-xs">
              {assay}
            </Link>
          ) : (
            <span key={assay} className="font-mono text-xs text-light-400">
              {assay}
            </span>
          );
        })}
      </div>
    </Modal>
  );
};

AssaysModal.displayName = "AssaysModal";

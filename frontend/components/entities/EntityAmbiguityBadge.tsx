"use client";

// The ambiguity affordance on an entity page: a chip in the header band,
// with the explanation behind it.
//
// It replaces a full-width amber banner that sat BELOW the header band.
// HeaderSectionSuspense renders the band and nothing else, so when the
// real header streamed in with a banner attached it inserted a block and
// pushed the tab strip and the whole page down. Living inside the band
// fixes that: the row's height is set by the H1, which is taller than a
// chip, so the chip costs no vertical space whether it is there or not.
//
// What the badge is warning about — see EntityAmbiguityBadgeModal below
// and materializer._build_sibling_map. Short version: it is about
// attribution, not naming, and the clusters are transitive.

import { MdCallSplit } from "react-icons/md";
import { useState } from "react";

import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import Modal from "@/components/basic/Modal";
import Tooltip from "@/components/basic/Tooltip";
import { AmbiguitySibling } from "@/types/Metadata";
import { encodeSpace } from "@/utils/utils";

interface Props {
  entityType: "food" | "chemical" | "disease" | "bioactivity";
  siblings: AmbiguitySibling[] | undefined | null;
}

// Above this a cluster is almost certainly a transitive chain rather than
// a genuine name collision, and saying so is the honest caveat.
const CHAINED_CLUSTER = 8;

const EntityAmbiguityBadge = ({ entityType, siblings }: Props) => {
  const [isOpen, setIsOpen] = useState(false);
  if (!Array.isArray(siblings) || siblings.length === 0) return null;

  const chained = siblings.length > CHAINED_CLUSTER;

  return (
    <>
      <Tooltip content="Some evidence here may belong to another entity — click for detail">
        <Chip
          tone="amber"
          size="sm"
          icon={<MdCallSplit className="size-2.5 rotate-90" />}
          label="Ambiguous term"
          onClick={() => setIsOpen(true)}
          aria-label={`Ambiguous term — ${siblings.length} related ${entityType}${
            siblings.length === 1 ? "" : "s"
          }`}
        />
      </Tooltip>

      <Modal
        fullHeight
        title="Ambiguous term"
        description={
          <div className="flex flex-col gap-3 text-light-300">
            <p>
              Sources used a name we could not resolve to a single{" "}
              {entityType}. Evidence on this page may describe one of the{" "}
              {entityType}s below instead, and the same rows may appear on
              their pages too.
            </p>
            {/* Only the chained caveat earns a second paragraph. The
             * sentence that used to lead it restated the one above. */}
            {chained && (
              <p className="text-light-500">
                These {siblings.length.toLocaleString()} were grouped by
                chained overlaps — one name linking to a second, that second
                to a third — so the more distant entries may never have shared
                a name with this {entityType}.
              </p>
            )}
          </div>
        }
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
      >
        {/* The full list, however long. The old banner rendered every
         * sibling inline in a one-line note; the largest chemical cluster
         * is 401. A scrollable modal is the right home for that. */}
        <ul className="flex flex-col divide-y divide-light-800">
          {siblings.map((sibling) => (
            <li key={sibling.foodatlas_id} className="py-2">
              <Link
                href={`/${entityType}/${encodeURIComponent(
                  encodeSpace(sibling.common_name)
                )}`}
                isExternal={false}
              >
                <span className="capitalize">{sibling.common_name}</span>
              </Link>
            </li>
          ))}
        </ul>
      </Modal>
    </>
  );
};

EntityAmbiguityBadge.displayName = "EntityAmbiguityBadge";

export default EntityAmbiguityBadge;

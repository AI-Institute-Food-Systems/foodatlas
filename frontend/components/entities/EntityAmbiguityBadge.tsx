"use client";

// The ambiguity affordance on an entity page: a text link on the entity
// badge's line, with the explanation behind it.
//
// It replaces a full-width amber banner that sat BELOW the header band.
// HeaderSectionSuspense renders the band and nothing else, so when the
// real header streamed in with a banner attached it inserted a block and
// pushed the tab strip and the whole page down. Living on the badge line
// fixes that: the line's height is set by the entity Badge, which is
// taller than this text, so the affordance costs no vertical space
// whether it is there or not. `whitespace-nowrap` is what keeps that
// true — a label that wrapped would add a line the skeleton has not
// reserved.
//
// Styled as a link rather than a warning chip: the amber sits on the
// glyph alone, and the label underlines on hover like every other
// inline action in the app.
//
// What it is warning about — see the modal copy below and
// materializer._build_sibling_map. Short version: it is about
// attribution, not naming, and the clusters are transitive.

import { MdCallSplit } from "react-icons/md";
import { useState } from "react";

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
      <Tooltip
        content="Some evidence here may belong to another entity — click for detail"
        placement="bottom"
      >
        <button
          type="button"
          onClick={() => setIsOpen(true)}
          aria-label={`Ambiguous entity — ${siblings.length} related ${entityType}${
            siblings.length === 1 ? "" : "s"
          }`}
          className="inline-flex items-center gap-1 whitespace-nowrap font-mono italic text-[0.7rem] text-light-300 underline-offset-4 transition-colors hover:text-light-100 hover:underline"
        >
          <MdCallSplit
            aria-hidden="true"
            className="size-3 rotate-90 text-amber-400"
          />
          {/* Glyph-only under 640px. The badge line also carries the
           * FoodAtlas id, and at 320px the label pushed it past the
           * viewport — a page-wide horizontal scroll on exactly the
           * ambiguous pages. The tooltip and aria-label still say what
           * it is, and the modal is one tap away either way. */}
          <span className="hidden sm:inline">Ambiguous entity</span>
        </button>
      </Tooltip>

      <Modal
        fullHeight
        title="Ambiguous entities"
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

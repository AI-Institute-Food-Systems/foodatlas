"use client";

// What `ambiguity_siblings` actually records, and why the old wording was
// wrong.
//
// It is NOT nomenclature. It does not mean "this name is also used for
// quercetin-7-olate" the way a dictionary would. It is an artefact of
// extraction: `materializer._build_sibling_map` reads the head/tail
// `candidates` on each attestation — the set of entities the LUT returned
// for one raw name in one source — and treats co-occurrence in that set as
// ambiguity. So the real claim is about attribution: some evidence on this
// page came from text the resolver could not pin to exactly this entity.
//
// It is also a TRANSITIVE closure. `_components_from_candidates` builds an
// adjacency graph and expands it to connected components, so A~B and B~C
// makes A and C siblings even though no source ever confused them
// directly. That is why the clusters get large: of the 4,780 chemicals
// with siblings, 806 have more than 20 and 709 have more than 100, topping
// out at 401 (phosphatidylcholine and friends). Foods max out at 8;
// diseases never have any.
//
// The old copy therefore did three things wrong: it described synonymy
// rather than misattribution, it said "you may be looking for one of
// those" — which is advice about navigation when the real point is a
// caveat on the data in front of you — and it rendered EVERY sibling
// inline, so 709 chemical pages carried a 400-link comma list in a banner
// sized for one line.

import { MdInfoOutline } from "react-icons/md";
import { Fragment, useState } from "react";

import Link from "@/components/basic/Link";
import { AmbiguitySibling } from "@/types/Metadata";
import { encodeSpace } from "@/utils/utils";

interface EntityAmbiguityBannerProps {
  entityType: "food" | "chemical" | "disease" | "bioactivity";
  siblings: AmbiguitySibling[] | undefined | null;
}

// Enough to recognise the cluster you are in without the banner becoming
// the page. Past this the count carries more than the names do.
const INLINE_LIMIT = 4;

// Above this the cluster is almost certainly transitive rather than a
// genuine name collision, and saying so is the honest caveat.
const CHAINED_CLUSTER = 8;

const EntityAmbiguityBanner = ({
  entityType,
  siblings,
}: EntityAmbiguityBannerProps) => {
  const [expanded, setExpanded] = useState(false);
  if (!Array.isArray(siblings) || siblings.length === 0) return null;

  const shown = expanded ? siblings : siblings.slice(0, INLINE_LIMIT);
  const hidden = siblings.length - shown.length;
  const chained = siblings.length > CHAINED_CLUSTER;

  return (
    <div
      role="note"
      className="mt-3 flex gap-1.5 items-start rounded border border-amber-500/40 bg-amber-500/10 px-2 py-1 text-xs text-amber-100"
    >
      <MdInfoOutline className="size-3.5 mt-0.5 text-amber-400 flex-shrink-0" />
      <p className="leading-snug">
        {/* Lead with the consequence for the reader, not the observation
         * about the name. What they need to know is that rows on this page
         * may not be about this entity. */}
        <span className="font-semibold">
          Some evidence here may belong to another {entityType}.
        </span>{" "}
        Sources used a name we could not resolve to a single {entityType}, so
        rows on this page may describe{" "}
        {shown.map((s, i) => (
          <Fragment key={s.foodatlas_id}>
            <Link
              href={`/${entityType}/${encodeURIComponent(
                encodeSpace(s.common_name)
              )}`}
              isExternal={false}
            >
              <span className="capitalize">{s.common_name}</span>
            </Link>
            {i < shown.length - 2 ? ", " : i === shown.length - 2 ? ", or " : ""}
          </Fragment>
        ))}
        {hidden > 0 && (
          <>
            {", or "}
            <button
              type="button"
              onClick={() => setExpanded(true)}
              className="underline underline-offset-2 hover:text-amber-50"
            >
              {hidden.toLocaleString()} other
              {hidden === 1 ? "" : "s"}
            </button>
          </>
        )}
        {" — and may appear on their pages too."}
        {chained && (
          <>
            {" "}
            <span className="text-amber-200/80">
              These {siblings.length.toLocaleString()} were grouped by chained
              overlaps, so the more distant ones may never have shared a name
              with this {entityType}.
            </span>
          </>
        )}
      </p>
    </div>
  );
};

EntityAmbiguityBanner.displayName = "EntityAmbiguityBanner";

export default EntityAmbiguityBanner;

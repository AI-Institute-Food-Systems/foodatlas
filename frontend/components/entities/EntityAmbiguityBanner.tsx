import { MdInfoOutline } from "react-icons/md";
import { Fragment } from "react";

import Link from "@/components/basic/Link";
import { AmbiguitySibling } from "@/types/Metadata";
import { encodeSpace } from "@/utils/utils";

interface EntityAmbiguityBannerProps {
  entityType: "food" | "chemical" | "disease" | "bioactivity";
  siblings: AmbiguitySibling[] | undefined | null;
}

// TEMPORARY — disabled while the loading-skeleton sequence is being debugged.
// The banner only appears once the header's data arrives, so it pops in late
// and pushes everything below it down, which makes it impossible to tell a
// skeleton→data handoff from a layout shift. Delete these two lines to
// restore; nothing else was changed and no call site was touched.
const TEMPORARILY_HIDDEN = true;

const EntityAmbiguityBanner = ({
  entityType,
  siblings,
}: EntityAmbiguityBannerProps) => {
  if (TEMPORARILY_HIDDEN) return null;
  if (!Array.isArray(siblings) || siblings.length === 0) return null;
  return (
    <div
      role="note"
      className="mt-3 flex gap-1.5 items-start rounded border border-amber-500/40 bg-amber-500/10 px-2 py-1 text-xs text-amber-100"
    >
      <MdInfoOutline className="size-3.5 mt-0.5 text-amber-400 flex-shrink-0" />
      <p className="leading-snug">
        This name is also used for{" "}
        {siblings.map((s, i) => (
          <Fragment key={s.foodatlas_id}>
            <Link
              href={`/${entityType}/${encodeURIComponent(
                encodeSpace(s.common_name)
              )}`}
              isExternal={false}
            >
              <span className="capitalize">{s.common_name}</span>
            </Link>
            {i < siblings.length - 2
              ? ", "
              : i === siblings.length - 2
              ? ", and "
              : ""}
          </Fragment>
        ))}
        . You may be looking for one of those.
      </p>
    </div>
  );
};

EntityAmbiguityBanner.displayName = "EntityAmbiguityBanner";

export default EntityAmbiguityBanner;

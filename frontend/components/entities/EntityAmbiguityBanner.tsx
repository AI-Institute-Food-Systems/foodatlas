import { MdInfoOutline } from "react-icons/md";
import { Fragment } from "react";

import Link from "@/components/basic/Link";
import { AmbiguitySibling } from "@/types/Metadata";
import { encodeSpace } from "@/utils/utils";

interface EntityAmbiguityBannerProps {
  entityType: "food" | "chemical" | "disease";
  siblings: AmbiguitySibling[] | undefined | null;
}

const EntityAmbiguityBanner = ({
  entityType,
  siblings,
}: EntityAmbiguityBannerProps) => {
  if (!Array.isArray(siblings) || siblings.length === 0) return null;
  return (
    <div
      role="note"
      className="mt-4 flex gap-2 items-start rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-100"
    >
      <MdInfoOutline className="size-4 mt-0.5 text-amber-400 flex-shrink-0" />
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

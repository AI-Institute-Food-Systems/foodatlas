"use client";

import { MdCallSplit } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Tooltip from "@/components/basic/Tooltip";
import { AmbiguitySibling } from "@/types/Metadata";

interface EntitySiblingIconProps {
  siblings: AmbiguitySibling[] | null | undefined;
  entityKind: "food" | "chemical";
  className?: string;
}

const EntitySiblingIcon = ({
  siblings,
  entityKind,
  className,
}: EntitySiblingIconProps) => {
  if (!Array.isArray(siblings) || siblings.length === 0) return null;
  const names = siblings.map((s) => s.common_name).join(", ");
  return (
    <Tooltip
      content={
        <span className="whitespace-normal max-w-xs block">
          <span className="text-amber-400 font-semibold">
            Ambiguous {entityKind}
          </span>
          <span className="text-light-300">{" "}— this name also refers to:{" "}</span>
          <span className="capitalize">{names}</span>
        </span>
      }
    >
      <MdCallSplit
        aria-label={`Ambiguous ${entityKind}`}
        className={twMerge("text-amber-500 size-4 rotate-90", className)}
      />
    </Tooltip>
  );
};

EntitySiblingIcon.displayName = "EntitySiblingIcon";

export default EntitySiblingIcon;

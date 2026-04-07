"use client";

import { useState } from "react";
import { MdInfoOutline, MdKeyboardArrowDown } from "react-icons/md";

import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import { encodeSpace } from "@/utils/utils";

interface NoConcentrationRow {
  id: string;
  name: string;
  evidence_count: number;
}

interface NoConcentrationCompositionProps {
  data: NoConcentrationRow[] | undefined | null;
}

const NoConcentrationComposition = ({
  data,
}: NoConcentrationCompositionProps) => {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <Card>
      {data && data.length > 0 ? (
        <div className="flex flex-col gap-3">
          {/* collapsible header */}
          <button
            className="flex items-center gap-2 text-light-300 hover:text-light-100 transition duration-200 cursor-pointer"
            onClick={() => setIsExpanded((prev) => !prev)}
          >
            <MdKeyboardArrowDown
              className={`size-5 transition-transform duration-200 ${
                isExpanded ? "rotate-0" : "-rotate-90"
              }`}
            />
            <span className="text-sm">
              {data.length} food{data.length === 1 ? "" : "s"} with
              unknown concentration
            </span>
          </button>
          {/* expanded content */}
          {isExpanded && (
            <div className="flex gap-2 flex-wrap font-light">
              {data.map((row) => (
                <span key={row.id} className="flex items-baseline gap-1">
                  <Link
                    className="capitalize"
                    href={`/food/${encodeURIComponent(encodeSpace(row.name))}`}
                    isExternal={false}
                  >
                    {row.name}
                  </Link>
                  {row.evidence_count > 0 && (
                    <span className="text-xs text-light-500">
                      ({row.evidence_count})
                    </span>
                  )}
                </span>
              ))}
            </div>
          )}
        </div>
      ) : (
        <div className="h-16 flex items-center justify-center text-light-300 gap-2">
          <MdInfoOutline /> No foods found
        </div>
      )}
    </Card>
  );
};

NoConcentrationComposition.displayName = "NoConcentrationComposition";

export default NoConcentrationComposition;

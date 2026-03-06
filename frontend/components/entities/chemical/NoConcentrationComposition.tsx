"use client";

import { MdInfoOutline } from "react-icons/md";

import Link from "@/components/basic/Link";
import Card from "@/components/basic/Card";
import { encodeSpace } from "@/utils/utils";

interface NoConcentrationCompositionProps {
  data: any | undefined | null;
}

const NoConcentrationComposition = ({
  data,
}: NoConcentrationCompositionProps) => {
  return (
    <Card>
      {/* <div className="relative flex items-center">
        <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
        <input
          className="pl-9 w-60 h-9 text-sm rounded-lg border border-light-50/5 bg-light-800 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
          type="text"
          placeholder="Search foods"
          onChange={handleSearchChange}
        />
      </div> */}
      {data?.length > 0 ? (
        <div className="flex gap-2 flex-wrap font-light">
          {/* @ts-ignore */}
          {data.map((row) => (
            <Link
              key={row.id}
              className="capitalize"
              href={`/food/${encodeURIComponent(encodeSpace(row.name))}`}
              isExternal={false}
            >
              {row.name}
            </Link>
          ))}
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

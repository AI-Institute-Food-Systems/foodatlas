"use client";

import { useCallback, useMemo } from "react";

import { getFoodBioactivities } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import BioactivityTable, {
  NameLinkCell,
  TOP_MEASUREMENT_SORT_KEY,
  TopMeasurementCell,
  ViewAssaysCell,
  type SortableColumn,
} from "@/components/entities/bioactivity/BioactivityTable";

interface Props {
  commonName: string;
  anchorId?: string | null;
  // Optional external control: forwarded to BioactivityTable so a
  // parent (FoodBioactivitiesTab) can drive search + source-kind
  // filtering + chrome-suppression for BOTH direct + inferred tables
  // from one shared sidebar.
  externalSearch?: string;
  externalSourceKind?: string;
  hideChrome?: boolean;
}

const FoodBioactivitiesSection = ({
  commonName,
  anchorId,
  externalSearch,
  externalSourceKind,
  hideChrome,
}: Props) => {
  const fetcher = useCallback(
    (params: BioactivityListParams) => getFoodBioactivities(commonName, params),
    [commonName]
  );

  const columns = useMemo<SortableColumn[]>(
    () => [
      {
        key: "name",
        label: "Bioactivity",
        align: "left",
        width: "w-[40%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/bioactivity/" />,
      },
      {
        key: TOP_MEASUREMENT_SORT_KEY,
        label: "Top measurement",
        align: "right",
        width: "w-[35%]",
        sortable: true,
        render: (row) => <TopMeasurementCell row={row} />,
      },
      {
        key: "assays",
        label: "Assays",
        align: "right",
        width: "w-[25%]",
        render: (row, ctx) => <ViewAssaysCell row={row} ctx={ctx} />,
      },
    ],
    []
  );

  return (
    <div className="flex flex-col gap-6">
      {/* Header — mirrors the chip+blurb pattern on the inferred section
       * so the two are visually parallel and the diff is obvious. */}
      <div className="flex flex-col gap-2">
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded-r-md px-2.5 py-0.5 font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase -ml-3">
          Directly measured
        </span>
        <p className="font-serif italic text-light-400 text-sm">
          Bioactivities {commonName} (or an extract of it) was tested for in
          an assay. These are direct food-level measurements — the food
          itself was the test material.
        </p>
      </div>
      <BioactivityTable
        tableId={`food-bioactivities-${commonName}`}
        direction="food-bioactivities"
        pivotName={commonName}
        fetcher={fetcher}
        columns={columns}
        searchPlaceholder="Search bioactivities"
        emptyMessage="No bioactivities recorded for this food yet"
        externalSearch={externalSearch}
        externalSourceKind={externalSourceKind}
        hideChrome={hideChrome}
        modalConfig={{
          anchorLabel: commonName,
          headIsRow: false,
          relationship: "r5",
          anchorId,
        }}
      />
    </div>
  );
};

FoodBioactivitiesSection.displayName = "FoodBioactivitiesSection";
export default FoodBioactivitiesSection;

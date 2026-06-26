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
}

const FoodBioactivitiesSection = ({ commonName, anchorId }: Props) => {
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
    <BioactivityTable
      tableId={`food-bioactivities-${commonName}`}
      direction="food-bioactivities"
      pivotName={commonName}
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search bioactivities"
      emptyMessage="No bioactivities recorded for this food yet"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: false,
        relationship: "r5",
        anchorId,
      }}
    />
  );
};

FoodBioactivitiesSection.displayName = "FoodBioactivitiesSection";
export default FoodBioactivitiesSection;

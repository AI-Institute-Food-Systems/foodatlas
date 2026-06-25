"use client";

import { useCallback, useMemo } from "react";

import { getFoodBioactivities } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import BioactivityTable, {
  NameLinkCell,
  TopMeasurementCell,
  ViewAssaysCell,
  type SortableColumn,
} from "@/components/entities/bioactivity/BioactivityTable";

interface Props {
  commonName: string;
}

const FoodBioactivitiesSection = ({ commonName }: Props) => {
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
        key: "top",
        label: "Top measurement",
        align: "right",
        width: "w-[35%]",
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
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search bioactivities"
      emptyMessage="No bioactivities recorded for this food yet"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: false,
        relationship: "r5",
      }}
    />
  );
};

FoodBioactivitiesSection.displayName = "FoodBioactivitiesSection";
export default FoodBioactivitiesSection;

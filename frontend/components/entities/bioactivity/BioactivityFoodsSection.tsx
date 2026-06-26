"use client";

import { useCallback, useMemo } from "react";

import { getBioactivityFoods } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import BioactivityTable, {
  NameLinkCell,
  TopMeasurementCell,
  ViewAssaysCell,
  type SortableColumn,
} from "@/components/entities/bioactivity/BioactivityTable";

interface Props {
  commonName: string;
  anchorId?: string | null;
}

const BioactivityFoodsSection = ({ commonName, anchorId }: Props) => {
  const fetcher = useCallback(
    (params: BioactivityListParams) => getBioactivityFoods(commonName, params),
    [commonName]
  );

  const columns = useMemo<SortableColumn[]>(
    () => [
      {
        key: "name",
        label: "Food",
        align: "left",
        width: "w-[40%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/food/" />,
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
      tableId={`bioactivity-foods-${commonName}`}
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search foods"
      emptyMessage="No foods exhibit this bioactivity yet"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: true,
        relationship: "r5",
        anchorId,
      }}
    />
  );
};

BioactivityFoodsSection.displayName = "BioactivityFoodsSection";
export default BioactivityFoodsSection;

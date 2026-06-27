"use client";

import { useCallback, useMemo } from "react";

import { getBioactivityFoods } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import BioactivityTable, {
  EvidenceTypeCell,
  NameLinkCell,
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
        key: "evidence_type",
        label: "Evidence",
        align: "right",
        width: "w-[30%]",
        render: (row) => <EvidenceTypeCell row={row} />,
      },
      {
        key: "assays",
        label: "Assays",
        align: "right",
        width: "w-[30%]",
        render: (row, ctx) => <ViewAssaysCell row={row} ctx={ctx} />,
      },
    ],
    []
  );

  return (
    <BioactivityTable
      tableId={`bioactivity-foods-${commonName}`}
      direction="bioactivity-foods"
      pivotName={commonName}
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

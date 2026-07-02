"use client";

import { useCallback, useMemo } from "react";

import { getBioactivityChemicals } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import type { BioactivityChemicalRow } from "@/types";
import BioactivityTable, {
  CategoryCell,
  NameLinkCell,
  NumberCell,
  TOP_MEASUREMENT_SORT_KEY,
  TopMeasurementCell,
  ViewAssaysCell,
  type SortableColumn,
} from "@/components/entities/bioactivity/BioactivityTable";

interface Props {
  commonName: string;
  anchorId?: string | null;
}

const BioactivityChemicalsSection = ({ commonName, anchorId }: Props) => {
  const fetcher = useCallback(
    (params: BioactivityListParams) =>
      getBioactivityChemicals(commonName, params),
    [commonName]
  );

  const columns = useMemo<SortableColumn[]>(
    () => [
      {
        key: "name",
        label: "Chemical",
        align: "left",
        width: "w-[22%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/chemical/" />,
      },
      {
        key: "category",
        label: "Category",
        align: "left",
        width: "w-[16%]",
        render: (row) => (
          <CategoryCell
            value={(row as BioactivityChemicalRow).chemical_classification}
          />
        ),
      },
      {
        key: "active_count",
        label: "Active",
        align: "right",
        width: "w-[9%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).active_count} />
        ),
      },
      {
        key: "inactive_count",
        label: "Inactive",
        align: "right",
        width: "w-[9%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).inactive_count} />
        ),
      },
      {
        key: "n_foods",
        label: "# Foods",
        align: "right",
        width: "w-[10%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).n_foods ?? 0} />
        ),
      },
      {
        key: TOP_MEASUREMENT_SORT_KEY,
        label: "Top measurement",
        align: "right",
        width: "w-[20%]",
        render: (row) => <TopMeasurementCell row={row} />,
      },
      {
        key: "measurement_count",
        label: "Assays",
        align: "right",
        width: "w-[14%]",
        sortable: true,
        render: (row, ctx) => <ViewAssaysCell row={row} ctx={ctx} />,
      },
    ],
    []
  );

  return (
    <BioactivityTable
      tableId={`bioactivity-chemicals-${commonName}`}
      direction="bioactivity-chemicals"
      pivotName={commonName}
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search chemicals"
      emptyMessage="No chemical-bioactivity measurements available yet"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: true,
        relationship: "r6",
        anchorId,
      }}
    />
  );
};

BioactivityChemicalsSection.displayName = "BioactivityChemicalsSection";
export default BioactivityChemicalsSection;

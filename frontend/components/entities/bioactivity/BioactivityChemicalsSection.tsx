"use client";

import { useCallback, useMemo } from "react";

import { getBioactivityChemicals } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import type { BioactivityChemicalRow } from "@/types";
import BioactivityTable, {
  NameLinkCell,
  NumberCell,
  TopMeasurementCell,
  ViewAssaysCell,
  type SortableColumn,
} from "@/components/entities/bioactivity/BioactivityTable";

interface Props {
  commonName: string;
}

const BioactivityChemicalsSection = ({ commonName }: Props) => {
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
        width: "w-[28%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/chemical/" />,
      },
      {
        key: "active_count",
        label: "Active",
        align: "right",
        width: "w-[14%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).active_count} />
        ),
      },
      {
        key: "inactive_count",
        label: "Inactive",
        align: "right",
        width: "w-[14%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).inactive_count} />
        ),
      },
      {
        key: "top",
        label: "Top measurement",
        align: "right",
        width: "w-[28%]",
        render: (row) => <TopMeasurementCell row={row} />,
      },
      {
        key: "assays",
        label: "Assays",
        align: "right",
        width: "w-[16%]",
        render: (row, ctx) => <ViewAssaysCell row={row} ctx={ctx} />,
      },
    ],
    []
  );

  return (
    <BioactivityTable
      tableId={`bioactivity-chemicals-${commonName}`}
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search chemicals"
      emptyMessage="No chemical-bioactivity measurements available yet"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: true,
        relationship: "r6",
      }}
    />
  );
};

BioactivityChemicalsSection.displayName = "BioactivityChemicalsSection";
export default BioactivityChemicalsSection;

"use client";

import { useCallback, useMemo } from "react";

import { getBioactivityChemicals } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import type { BioactivityChemicalRow } from "@/types";
import BioactivityTable, {
  EvidenceTypeCell,
  NameLinkCell,
  NumberCell,
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
        width: "w-[30%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/chemical/" />,
      },
      {
        key: "active_count",
        label: "Active",
        align: "right",
        width: "w-[10%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).active_count} />
        ),
      },
      {
        key: "inactive_count",
        label: "Inactive",
        align: "right",
        width: "w-[10%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).inactive_count} />
        ),
      },
      {
        key: "n_foods",
        label: "# Foods",
        align: "right",
        width: "w-[12%]",
        sortable: true,
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).n_foods ?? 0} />
        ),
      },
      {
        key: "evidence_type",
        label: "Evidence",
        align: "right",
        width: "w-[18%]",
        render: (row) => <EvidenceTypeCell row={row} />,
      },
      {
        key: "assays",
        label: "Assays",
        align: "right",
        width: "w-[20%]",
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

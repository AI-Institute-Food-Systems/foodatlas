"use client";

import { useCallback, useMemo } from "react";

import { getChemicalBioactivities } from "@/utils/fetching";
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

const ChemicalBioactivitiesSection = ({ commonName, anchorId }: Props) => {
  const fetcher = useCallback(
    (params: BioactivityListParams) =>
      getChemicalBioactivities(commonName, params),
    [commonName]
  );

  const columns = useMemo<SortableColumn[]>(
    () => [
      {
        key: "name",
        label: "Bioactivity",
        align: "left",
        width: "w-[28%]",
        sortable: true,
        render: (row) => <NameLinkCell row={row} hrefPrefix="/bioactivity/" />,
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
        key: "evidence_type",
        label: "Evidence",
        align: "right",
        width: "w-[24%]",
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
      tableId={`chemical-bioactivities-${commonName}`}
      direction="chemical-bioactivities"
      pivotName={commonName}
      fetcher={fetcher}
      columns={columns}
      searchPlaceholder="Search bioactivities"
      emptyMessage="No bioactivity measurements recorded for this chemical"
      modalConfig={{
        anchorLabel: commonName,
        headIsRow: false,
        relationship: "r6",
        anchorId,
      }}
    />
  );
};

ChemicalBioactivitiesSection.displayName = "ChemicalBioactivitiesSection";
export default ChemicalBioactivitiesSection;

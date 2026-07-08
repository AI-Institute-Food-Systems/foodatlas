"use client";

import { useCallback, useMemo } from "react";

import { getChemicalBioactivities } from "@/utils/fetching";
import type { BioactivityListParams } from "@/utils/fetching";
import type { BioactivityChemicalRow } from "@/types";
import BioactivityTable, {
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
        sortLabels: { asc: "Bioactivity A–Z", desc: "Bioactivity Z–A" },
        render: (row) => <NameLinkCell row={row} hrefPrefix="/bioactivity/" />,
      },
      {
        key: "active_count",
        label: "Active",
        align: "right",
        width: "w-[14%]",
        sortable: true,
        sortLabels: { asc: "Fewest active", desc: "Most active" },
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
        sortLabels: { asc: "Fewest inactive", desc: "Most inactive" },
        render: (row) => (
          <NumberCell value={(row as BioactivityChemicalRow).inactive_count} />
        ),
      },
      {
        key: TOP_MEASUREMENT_SORT_KEY,
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

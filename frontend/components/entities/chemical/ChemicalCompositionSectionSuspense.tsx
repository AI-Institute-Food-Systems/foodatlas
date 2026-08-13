import { TableSkeleton } from "@/components/basic/TableSkeleton";
import { COLUMNS, COLUMN_LABELS } from "@/utils/chemicalComposition";

// Stands in for ChemicalCompositionTable. It is driven by the SAME COLUMNS
// spec the real table renders from, so the placeholder grid and the loaded
// grid are the same geometry — the section can't re-slice when data lands.
// Headers are known statically here, so they render for real rather than as
// placeholder bars.
const ChemicalCompositionSectionSuspense = () => (
  <TableSkeleton columns={COLUMNS} headerLabels={COLUMN_LABELS} />
);

ChemicalCompositionSectionSuspense.displayName =
  "ChemicalCompositionSectionSuspense";

export default ChemicalCompositionSectionSuspense;

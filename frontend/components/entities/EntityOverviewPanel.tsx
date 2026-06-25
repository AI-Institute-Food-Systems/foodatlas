import TaxonomySection from "@/components/entities/TaxonomySection";
import { getMetaData } from "@/utils/fetching";

import OverviewCardCatalog from "./overview/OverviewCardCatalog";
import type { EntityType } from "./EntityTabs";

interface Props {
  commonName: string;
  entityType: EntityType;
}

// Renders the entity's metadata as a single tab panel — meant to be the
// content of an "Overview" apothecary tab. No outer Card wrapper of its
// own; the tab's Card frame is the container. Identifiers + taxonomy sit
// in a 2-col grid on lg+; bioactivity (no taxonomy) collapses to a single
// column.
const EntityOverviewPanel = async ({ commonName, entityType }: Props) => {
  const data = await getMetaData(commonName, entityType);
  if (!data) return null;

  const hasTaxonomy = entityType !== "bioactivity";

  return (
    <div
      className={
        hasTaxonomy
          ? "grid grid-cols-1 lg:grid-cols-2 gap-x-10 gap-y-8 items-start"
          : "grid grid-cols-1 gap-6"
      }
    >
      <OverviewCardCatalog entityType={entityType} data={data} naked />
      {hasTaxonomy && (
        <TaxonomySection
          commonName={commonName}
          entityType={entityType}
          naked
        />
      )}
    </div>
  );
};

EntityOverviewPanel.displayName = "EntityOverviewPanel";

export default EntityOverviewPanel;

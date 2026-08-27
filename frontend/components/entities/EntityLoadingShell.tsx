import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import type { EntityType } from "@/components/entities/entityTabs.config";

interface Props {
  entityType: EntityType;
}

// The whole above-the-fold shell an entity route shows between the click
// and the first server byte. Every `loading.tsx` is this one line, so the
// four routes cannot drift apart — and because both halves derive their
// tabs from entityTabs.config, neither can drift from the real page.
const EntityLoadingShell = ({ entityType }: Props) => (
  <div>
    <HeaderSectionSuspense entityType={entityType} />
    <EntityDetailLayoutSuspense entityType={entityType} />
  </div>
);

EntityLoadingShell.displayName = "EntityLoadingShell";

export default EntityLoadingShell;

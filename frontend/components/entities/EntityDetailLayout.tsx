import Card from "@/components/basic/Card";
import EntityTabs, {
  EntityType,
  TabSpec,
} from "@/components/entities/EntityTabs";
import { TabCountsProvider } from "@/context/tabCountsContext";

interface Props {
  entityType: EntityType;
  tabs: TabSpec[];
  defaultTabId: string;
}

const EntityDetailLayout = ({
  entityType,
  tabs,
  defaultTabId,
}: Props) => {
  return (
    <div className="mt-6">
      <section className="min-w-0">
        <TabCountsProvider>
          {tabs.length === 1 ? (
            <Card>{tabs[0].content}</Card>
          ) : (
            <EntityTabs
              entityType={entityType}
              tabs={tabs}
              defaultTabId={defaultTabId}
            />
          )}
        </TabCountsProvider>
      </section>
    </div>
  );
};

EntityDetailLayout.displayName = "EntityDetailLayout";

export default EntityDetailLayout;

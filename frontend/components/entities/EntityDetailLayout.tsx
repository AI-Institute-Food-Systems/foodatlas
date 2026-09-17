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
  // mt-10, not mt-6: the header block and the card stack are separate
  // things, and the tighter gap read as one continuous run.
  return (
    <div className="mt-10">
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

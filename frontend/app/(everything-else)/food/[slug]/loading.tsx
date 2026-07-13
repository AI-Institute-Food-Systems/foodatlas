import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";

const Loading = () => (
  <div>
    <HeaderSectionSuspense entityType="food" />
    <EntityDetailLayoutSuspense tabCount={3} />
  </div>
);

Loading.displayName = "FoodLoading";

export default Loading;

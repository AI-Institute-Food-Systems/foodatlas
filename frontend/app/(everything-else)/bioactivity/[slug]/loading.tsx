import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";

const Loading = () => (
  <div>
    <HeaderSectionSuspense entityType="bioactivity" />
    <EntityDetailLayoutSuspense tabCount={3} />
  </div>
);

Loading.displayName = "BioactivityLoading";

export default Loading;

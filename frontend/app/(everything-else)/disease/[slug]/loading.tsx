import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";

const Loading = () => (
  <div>
    <HeaderSectionSuspense entityType="disease" />
    <EntityDetailLayoutSuspense tabCount={2} />
  </div>
);

Loading.displayName = "DiseaseLoading";

export default Loading;

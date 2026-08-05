import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";

const Loading = () => (
  <div>
    <HeaderSectionSuspense entityType="chemical" />
    <EntityDetailLayoutSuspense tabCount={4} />
  </div>
);

Loading.displayName = "ChemicalLoading";

export default Loading;

"use client";

import { PageReadyProvider, usePageReady } from "@/context/pageReadyContext";
import EntityDetailLayoutSuspense from "@/components/entities/EntityDetailLayoutSuspense";
import HeaderSectionSuspense from "@/components/entities/HeaderSectionSuspense";
import type { EntityType } from "@/components/entities/EntityTabs";

interface Props {
  entityType: EntityType;
  tabCount: number;
  children: React.ReactNode;
}

// Gates the real entity page content behind the PageReadyContext's
// `ready` flag. While pending, we render the same skeleton `loading.tsx`
// uses AND keep the real content mounted but display:hidden — the
// hidden client components still run their mount-time fetches, and
// `useLoadingGate(isLoading)` inside each one signals completion. Once
// all loaders complete, we swap: skeleton off, real content visible.
// The user sees skeleton → data, no header/tab pop mid-load.
const GateContent = ({ entityType, tabCount, children }: Props) => {
  const { ready } = usePageReady();
  return (
    <>
      {!ready && (
        <div>
          <HeaderSectionSuspense entityType={entityType} />
          <EntityDetailLayoutSuspense tabCount={tabCount} />
        </div>
      )}
      <div className={ready ? "" : "hidden"} aria-hidden={!ready}>
        {children}
      </div>
    </>
  );
};

const EntityPageGate = (props: Props) => (
  <PageReadyProvider>
    <GateContent {...props} />
  </PageReadyProvider>
);

EntityPageGate.displayName = "EntityPageGate";

export default EntityPageGate;

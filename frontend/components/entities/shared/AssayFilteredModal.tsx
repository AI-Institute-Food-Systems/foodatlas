"use client";

// The assay-inferred table's three detail modals (assays, targets,
// activities), opened on the page's Signal and Activity filters and
// editable from there. Lists and counts come from the pair's assay groups
// (see assayGroups.ts), the same helpers the row's buttons use.

import { useMemo, useState } from "react";

import Card from "@/components/basic/Card";
import {
  AssayActivitiesModal,
  AssaysModal,
  AssayTargetsModal,
} from "@/components/entities/shared/AssayDetailModals";
import {
  filteredActivities,
  filteredAssayCount,
  filteredAssays,
  filteredTargets,
  type AssayFilters,
} from "@/components/entities/shared/assayGroups";
import ActivityFilterGroup from "@/components/entities/shared/filters/ActivityFilterGroup";
import SignalFilterGroup, {
  SIGNALS,
} from "@/components/entities/shared/filters/SignalFilterGroup";
import type { AssayInferredAssociation } from "@/types/AssayInferred";

export type AssayModalKind = "assays" | "targets" | "activities";

/** How many items a modal of `kind` lists under `f` — the row button's number. */
export const assayItemCount = (
  kind: AssayModalKind,
  row: AssayInferredAssociation,
  f: AssayFilters
): number =>
  kind === "assays"
    ? filteredAssayCount(row, f)
    : kind === "targets"
      ? filteredTargets(row, f).length
      : filteredActivities(row, f).length;

interface Props {
  kind: AssayModalKind;
  row: AssayInferredAssociation;
  peerName: string;
  initialFilters: AssayFilters;
  onClose: () => void;
}

const toggle = (list: string[], key: string) =>
  list.includes(key) ? list.filter((k) => k !== key) : [...list, key];

// Mounted per open (the caller renders it only while open), so the
// initial filters are read once, on open.
const AssayFilteredModal = ({
  kind,
  row,
  peerName,
  initialFilters,
  onClose,
}: Props) => {
  const [signals, setSignals] = useState(initialFilters.signals);
  const [activities, setActivities] = useState(initialFilters.activities);
  const f = useMemo(() => ({ signals, activities }), [signals, activities]);

  const signalCounts = useMemo(
    () =>
      Object.fromEntries(
        SIGNALS.map(({ key }) => [
          key,
          assayItemCount(kind, row, { ...f, signals: [key] }),
        ])
      ),
    [kind, row, f]
  );
  const activityCounts = useMemo(
    () =>
      Object.fromEntries(
        (row.bioactivities ?? []).map((a) => [
          a,
          assayItemCount(kind, row, { ...f, activities: [a] }),
        ])
      ),
    [kind, row, f]
  );

  const sidebar = (
    <Card className="px-4 py-4 gap-5">
      <SignalFilterGroup
        selected={signals}
        counts={signalCounts}
        onToggle={(k) => setSignals((p) => toggle(p, k))}
        onClear={() => setSignals([])}
      />
      <ActivityFilterGroup
        selected={activities}
        counts={activityCounts}
        onToggle={(k) => setActivities((p) => toggle(p, k))}
        onClear={() => setActivities([])}
      />
    </Card>
  );

  if (kind === "assays") {
    return (
      <AssaysModal
        assays={filteredAssays(row, f)}
        totalCount={filteredAssayCount(row, f)}
        peerName={peerName}
        isOpen
        onClose={onClose}
        sidebar={sidebar}
      />
    );
  }
  if (kind === "targets") {
    return (
      <AssayTargetsModal
        targets={filteredTargets(row, f)}
        peerName={peerName}
        isOpen
        onClose={onClose}
        sidebar={sidebar}
      />
    );
  }
  return (
    <AssayActivitiesModal
      activities={filteredActivities(row, f)}
      peerName={peerName}
      isOpen
      onClose={onClose}
      sidebar={sidebar}
    />
  );
};

export default AssayFilteredModal;

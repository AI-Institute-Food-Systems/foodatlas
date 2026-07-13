import Card from "@/components/basic/Card";
import LoadingCard from "@/components/basic/LoadingCard";

interface Props {
  // Number of tab chips in the desktop strip. Matches the real page so
  // the skeleton doesn't visibly shift when the real tabs mount.
  tabCount?: number;
}

// Mirrors <EntityDetailLayout /> + the client tables' isLoading render
// as closely as possible so:
//   1. The route-level Suspense fallback (this file) gives instant
//      feedback on hard reload.
//   2. When the SSR shell replaces it, the body doesn't visibly change
//      (both are the same 20-row `<table>` skeleton the client tables
//      themselves render during isLoading).
//   3. When the client fetch resolves, data replaces the same rows in
//      place — no "loading → data → loading → data" perception.
//
// If EntityTabs, BioactivityTable, or FoodCompositionSection styling
// changes (tab shape, row structure, filter chrome layout), mirror the
// change here too so the loading→SSR handoff stays seamless.
const EntityDetailLayoutSuspense = ({ tabCount = 3 }: Props) => {
  return (
    <div className="mt-6">
      <section className="min-w-0">
        <div className="sm:hidden mb-2 pl-1">
          <LoadingCard className="h-10 w-full rounded-md" />
        </div>

        <div className="hidden sm:flex items-end gap-1.5 pl-3">
          {Array.from({ length: tabCount }).map((_, i) => (
            <div
              key={i}
              className="h-8 min-w-[9.5rem] rounded-t-lg border-t-[1.5px] border-x-[1.5px] border-light-50/[0.15] bg-light-950/50"
            />
          ))}
        </div>

        <Card>
          {/* Filter chrome row: search field + filters button placeholder.
           * Matches the mobile filter row (`!hideChrome && <flex row>`)
           * the client tables render above their skeleton body. */}
          <div className="mb-4 flex items-center gap-3">
            <div className="flex-1 min-w-0 max-w-xs">
              <LoadingCard className="h-8 w-full rounded-md" />
            </div>
            <LoadingCard className="h-8 w-20 rounded-md" />
          </div>

          {/* Desktop: 20 skeleton rows in a table — same `<tr><td
           * className="w-full py-1.5"><div className="h-9 flex
           * items-center"><LoadingCard className="h-5" />` pattern the
           * tables use. */}
          <div className="hidden md:block">
            <table className="w-full">
              <tbody>
                {Array.from({ length: 20 }).map((_, i) => (
                  <tr key={i}>
                    <td className="w-full py-1.5">
                      <div className="h-9 flex items-center">
                        <LoadingCard className="h-5 w-full" />
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Mobile: 8 divided loading cards — matches the tables'
           * mobile card list skeleton. */}
          <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="w-full py-3">
                <LoadingCard className="h-5" />
              </div>
            ))}
          </div>
        </Card>
      </section>
    </div>
  );
};

EntityDetailLayoutSuspense.displayName = "EntityDetailLayoutSuspense";

export default EntityDetailLayoutSuspense;

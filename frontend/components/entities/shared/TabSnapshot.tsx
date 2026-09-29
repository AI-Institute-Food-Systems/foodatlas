// The server-rendered stand-in for a tab nobody has opened yet.
//
// EntityTabs mounts a tab's real content only once it's opened, so page
// load pays for one tab's fetches. That left every other tab out of the
// server HTML, and the tables inside them load through /_proxy-api, which
// robots.txt disallows — so crawlers never saw those rows or followed
// their links. This renders each table's first page as plain rows and
// links inside the (hidden) unopened panel. No state, no effects, no
// fetches of its own: the page hands it payloads it already loaded.
// Opening the tab swaps in the interactive table.

import Link from "next/link";

import type {
  SnapshotCell,
  SnapshotSection,
} from "@/utils/tabSnapshots";

const Cell = ({ cell }: { cell: SnapshotCell }) =>
  typeof cell === "object" && cell !== null ? (
    <Link href={cell.href}>{cell.label}</Link>
  ) : (
    <>{cell}</>
  );

const TabSnapshot = ({ sections }: { sections: SnapshotSection[] }) => {
  const shown = sections.filter((s) => s.rows.length > 0);
  if (shown.length === 0) return null;
  return (
    <div data-tab-snapshot>
      {shown.map((section) => (
        <section key={section.heading}>
          <h3>{section.heading}</h3>
          {/* A list rather than a table: nobody sees these rows, so they
            * have no use for the report-a-row wiring every visible table body
            * carries. Column names prefix the cells instead. */}
          <ul>
            {section.rows.map((row, i) => (
              <li key={i}>
                {row.map((cell, j) => (
                  <span key={j}>
                    {j > 0 && ` · ${section.columns[j] ?? ""}: `}
                    <Cell cell={cell} />
                  </span>
                ))}
              </li>
            ))}
          </ul>
        </section>
      ))}
    </div>
  );
};

export default TabSnapshot;

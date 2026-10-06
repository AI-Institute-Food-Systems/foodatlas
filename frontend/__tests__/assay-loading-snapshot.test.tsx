// The disease page lands on the Chemicals tab, whose lab-assay table loads
// in the browser. Its first rows ship as hidden server HTML instead, so
// crawlers see them; the snapshot must leave once the real rows arrive.

import { render, screen, waitFor } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import AssayInferredAssociationsTable from "@/components/entities/AssayInferredAssociationsTable";
import TabSnapshot from "@/components/entities/shared/TabSnapshot";
import { PaginationsProvider } from "@/context/paginationsContext";
import { assayInferredSection } from "@/utils/tabSnapshots";

const assay = (chemical: string, n_assays: number) => ({
  chemical_name: chemical,
  chemical_foodatlas_id: `c-${chemical}`,
  disease_name: "diabetes",
  disease_foodatlas_id: "d1",
  n_assays,
  n_active_measurements: n_assays,
  relationships: ["therapeutic"],
  target_genes: [],
  targets: [],
  assays: [],
  bioactivities: ["anticancer"],
});

const payload = { data: [assay("berberine", 5)], metadata: { row_count: 1 } };

const table = (fetcher: () => Promise<typeof payload>) => (
  <PaginationsProvider>
    <AssayInferredAssociationsTable
      commonName="diabetes"
      peer="chemical"
      fetcher={fetcher}
      loadingSnapshot={
        <TabSnapshot sections={[assayInferredSection("chemical", payload)]} />
      }
    />
  </PaginationsProvider>
);

describe("lab-assay table loading snapshot", () => {
  it("puts the rows and links in the server HTML, hidden", () => {
    const html = renderToString(table(() => new Promise(() => {})));
    expect(html).toMatch(/<div hidden="">.*data-tab-snapshot/);
    expect(html).toContain('href="/chemical/berberine"');
  });

  it("drops the snapshot once the client rows load", async () => {
    const { container } = render(table(() => Promise.resolve(payload)));
    await waitFor(() =>
      expect(screen.getAllByText("berberine").length).toBeGreaterThan(0)
    );
    expect(container.querySelector("[data-tab-snapshot]")).toBeNull();
  });
});

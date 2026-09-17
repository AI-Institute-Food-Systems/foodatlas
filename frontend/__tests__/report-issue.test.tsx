import {
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import {
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

// next/navigation isn't mounted in the vitest jsdom env; Button uses
// useRouter for its built-in <Link> shortcut, so stub it — same pattern
// as bioactivity-sections.test.tsx.
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

// HeadlessUI Listbox uses ResizeObserver internally; jsdom doesn't
// ship one, so stub with a no-op.
class MockResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(globalThis as any).ResizeObserver = MockResizeObserver;

import ReportFab from "@/components/basic/ReportFab";
import {
  ReportModeProvider,
  useReportRows,
} from "@/context/reportModeContext";
import type { ReportContext } from "@/types/Report";

// Minimal harness that exercises the whole global-FAB flow: the FAB
// mounts once (like it does in providers.tsx), and a tiny list of rows
// consumes useReportRows() to become selectable when the FAB flips on.
const Harness = ({ rows }: { rows: ReportContext[] }) => {
  const { getRowProps } = useReportRows();
  return (
    <>
      <ul>
        {rows.map((ctx, i) => (
          <li
            key={i}
            data-testid={`row-${i}`}
            {...getRowProps(ctx)}
          >
            {ctx.kind}
          </li>
        ))}
      </ul>
      <ReportFab />
    </>
  );
};

const renderHarness = (rows: ReportContext[]) =>
  render(
    <ReportModeProvider>
      <Harness rows={rows} />
    </ReportModeProvider>,
  );

const CTX_A: ReportContext = {
  kind: "food-composition-row",
  entityType: "food",
  entitySlug: "onion",
  chemicalName: "quercetin",
  dataPointCount: 12,
};

const CTX_B: ReportContext = {
  kind: "food-composition-evidence",
  entityType: "food",
  attestationId: "ATT-9",
  extractedChemical: "quercetin",
  extractedFood: "onion",
  concentration: "42",
  referenceUrl: "https://example.com/paper",
};

describe("Global Report FAB + useReportRows flow", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("clicking the FAB enters select mode; rows become selectable", () => {
    renderHarness([CTX_A, CTX_B]);

    // FAB visible as an idle button; no select-mode status banner yet.
    expect(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );

    // FAB switches to the amber status pill.
    expect(screen.getByRole("status")).toHaveTextContent(
      /click any row to report/i,
    );
    // Each row is now announced as a button.
    expect(
      screen.getAllByRole("button", {
        name: /report an issue with this row/i,
      }),
    ).toHaveLength(2);
  });

  it("clicking a row in select mode opens the modal with that row's context", () => {
    renderHarness([CTX_A, CTX_B]);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );
    fireEvent.click(screen.getByTestId("row-1"));

    expect(
      screen.getByRole("heading", { name: /report an issue/i }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByText(/what gets sent with this report/i));
    expect(screen.getByText("ATT-9")).toBeInTheDocument();
    expect(screen.getByText("attestationId")).toBeInTheDocument();
  });

  it("submits without an email and shows the success banner", async () => {
    const fetchMock = vi
      .spyOn(global, "fetch")
      .mockResolvedValue(
        new Response(JSON.stringify({ success: true }), { status: 200 }),
      );

    renderHarness([CTX_A]);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );
    fireEvent.click(screen.getByTestId("row-0"));

    fireEvent.change(
      screen.getByPlaceholderText(/'Si' extracted as 'ser-ile peptide'/i),
      { target: { value: "wrong extraction" } },
    );

    fireEvent.click(screen.getByRole("button", { name: /send report/i }));

    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        /we'll take a look/i,
      ),
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/report/issue/send");
    const body = JSON.parse((init as RequestInit).body as string);
    expect(body.category).toBe("Extraction error");
    expect(body.description).toBe("wrong extraction");
    expect(body.email).toBeUndefined();
    expect(body.context).toEqual(CTX_A);
  });

  it("passes the reporter's email through when provided", async () => {
    const fetchMock = vi
      .spyOn(global, "fetch")
      .mockResolvedValue(
        new Response(JSON.stringify({ success: true }), { status: 200 }),
      );

    renderHarness([CTX_A]);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );
    fireEvent.click(screen.getByTestId("row-0"));

    fireEvent.change(
      screen.getByPlaceholderText(/'Si' extracted as 'ser-ile peptide'/i),
      { target: { value: "duplicate row" } },
    );
    fireEvent.change(
      screen.getByPlaceholderText(/you@example.com/i),
      { target: { value: "reporter@example.org" } },
    );

    fireEvent.click(screen.getByRole("button", { name: /send report/i }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const body = JSON.parse(
      (fetchMock.mock.calls[0][1] as RequestInit).body as string,
    );
    expect(body.email).toBe("reporter@example.org");
  });

  it("shows an error banner when the endpoint returns non-2xx", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ error: "boom" }), { status: 500 }),
    );

    renderHarness([CTX_A]);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );
    fireEvent.click(screen.getByTestId("row-0"));
    fireEvent.change(
      screen.getByPlaceholderText(/'Si' extracted as 'ser-ile peptide'/i),
      { target: { value: "anything" } },
    );
    fireEvent.click(screen.getByRole("button", { name: /send report/i }));

    await waitFor(() =>
      expect(screen.getByRole("alert")).toHaveTextContent(
        /something went wrong/i,
      ),
    );
  });

  it("Cancel button in the status pill exits select mode without opening the modal", () => {
    renderHarness([CTX_A]);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with a data point/i,
      }),
    );
    expect(screen.getByRole("status")).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", { name: /cancel report selection/i }),
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("heading", { name: /report an issue/i }),
    ).not.toBeInTheDocument();
  });
});

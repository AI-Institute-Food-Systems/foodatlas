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

import { useTableReporter } from "@/components/basic/useTableReporter";
import type { ReportContext } from "@/types/Report";

// Minimal harness: three fake rows + the trigger/banner/modal from the
// hook. Mirrors what a real surface (CorrelationTable, EvidenceTable,
// etc.) sets up, so the assertions here also exercise the wiring
// contract callers depend on.
const Harness = ({ rows }: { rows: ReportContext[] }) => {
  const reporter = useTableReporter({ targetLabel: "row" });
  return (
    <div>
      {reporter.trigger}
      {reporter.banner}
      <ul>
        {rows.map((ctx, i) => (
          <li
            key={i}
            data-testid={`row-${i}`}
            {...reporter.getRowProps(ctx)}
          >
            {ctx.kind}
          </li>
        ))}
      </ul>
      {reporter.modal}
    </div>
  );
};

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

describe("useTableReporter — select-a-row flow", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("clicking the trigger enters select mode + shows the banner", () => {
    render(<Harness rows={[CTX_A, CTX_B]} />);

    expect(
      screen.getByRole("button", { name: /report an issue/i }),
    ).toBeInTheDocument();
    // No banner before entering select mode.
    expect(screen.queryByRole("status")).not.toBeInTheDocument();

    fireEvent.click(
      screen.getByRole("button", { name: /report an issue/i }),
    );

    // Banner appears + rows become role="button" (selectable).
    expect(screen.getByRole("status")).toHaveTextContent(
      /click any row to report/i,
    );
    expect(
      screen.getAllByRole("button", {
        name: /report an issue with this row/i,
      }),
    ).toHaveLength(2);
  });

  it("clicking a row in select mode opens the modal with that row's context", () => {
    render(<Harness rows={[CTX_A, CTX_B]} />);
    fireEvent.click(
      screen.getByRole("button", { name: /report an issue/i }),
    );

    fireEvent.click(screen.getByTestId("row-1"));

    // Modal title (h3) — same text as the trigger label, so we look
    // at the heading role specifically to disambiguate.
    expect(
      screen.getByRole("heading", { name: /report an issue/i }),
    ).toBeInTheDocument();

    // Reveal the context preview so we can assert the correct row's
    // fields made it in. Attestation ID is unique to CTX_B — it only
    // appears in the preview, so it's a good marker that the right
    // row's context was captured.
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

    render(<Harness rows={[CTX_A]} />);
    fireEvent.click(
      screen.getByRole("button", { name: /report an issue/i }),
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

    render(<Harness rows={[CTX_A]} />);
    fireEvent.click(
      screen.getByRole("button", { name: /report an issue/i }),
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

    render(<Harness rows={[CTX_A]} />);
    fireEvent.click(
      screen.getByRole("button", { name: /report an issue/i }),
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

  it("clicking the trigger a second time cancels select mode without opening the modal", () => {
    render(<Harness rows={[CTX_A]} />);
    const trigger = screen.getByRole("button", { name: /report an issue/i });
    fireEvent.click(trigger);
    expect(screen.getByRole("status")).toBeInTheDocument();

    // The trigger relabels to Cancel selection in select mode.
    fireEvent.click(
      screen.getByRole("button", { name: /cancel selection/i }),
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    // Modal should not have opened — no heading with that text.
    expect(
      screen.queryByRole("heading", { name: /report an issue/i }),
    ).not.toBeInTheDocument();
  });
});

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

import ReportIssueButton from "@/components/basic/ReportIssueButton";
import type { ReportContext } from "@/types/Report";

const context: ReportContext = {
  kind: "food-composition-evidence",
  entityType: "food",
  entitySlug: "onion",
  attestationId: "ATT-123",
  extractedChemical: "quercetin",
  extractedFood: "onion",
  concentration: "42",
  referenceUrl: "https://example.com/paper",
};

describe("ReportIssueButton → ReportIssueModal", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("opens the modal on click and shows the auto-captured context", () => {
    render(<ReportIssueButton context={context} />);

    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with this data point/i,
      }),
    );

    // Modal title is rendered as a Heading; matching by exact text.
    expect(screen.getByText("Report an issue")).toBeInTheDocument();

    // Context preview is behind a <details>; open it to inspect the row.
    const preview = screen.getByText(/what gets sent with this report/i);
    fireEvent.click(preview);
    expect(screen.getByText("attestationId")).toBeInTheDocument();
    expect(screen.getByText("ATT-123")).toBeInTheDocument();
  });

  it("submits the form and shows the success banner (no email needed)", async () => {
    const fetchMock = vi
      .spyOn(global, "fetch")
      .mockResolvedValue(
        new Response(JSON.stringify({ success: true }), { status: 200 }),
      );

    render(<ReportIssueButton context={context} />);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with this data point/i,
      }),
    );

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
    // No email → key should not carry a value.
    expect(body.email).toBeUndefined();
    expect(body.context).toEqual(context);
  });

  it("passes the user's email through when provided", async () => {
    const fetchMock = vi
      .spyOn(global, "fetch")
      .mockResolvedValue(
        new Response(JSON.stringify({ success: true }), { status: 200 }),
      );

    render(<ReportIssueButton context={context} />);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with this data point/i,
      }),
    );

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

    render(<ReportIssueButton context={context} />);
    fireEvent.click(
      screen.getByRole("button", {
        name: /report an issue with this data point/i,
      }),
    );
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
});

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/contact",
  useSearchParams: () => new URLSearchParams(),
}));

import ContactForm from "@/components/contact/ContactForm";

// The API-access questions exist only for "API Access Request", block
// submission until answered, and reach /contact/send as `apiAccess`.

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// Listbox buttons are named by their Field label, not the current value.
const pick = async (label: string | RegExp, option: string) => {
  fireEvent.click(screen.getByRole("button", { name: label }));
  fireEvent.click(await screen.findByRole("option", { name: option }));
};

const submit = () =>
  fireEvent.submit(
    screen.getByRole("button", { name: /send/i }).closest("form")!,
  );

const postedBody = (fetchMock: ReturnType<typeof vi.fn>) =>
  JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string);

describe("ContactForm API access fields", () => {
  it("are shown only for the API topic", async () => {
    render(<ContactForm isApiAccessRequest={false} />);
    expect(screen.queryByText(/which data do you need\?/i)).toBeNull();
    expect(screen.getByRole("textbox", { name: /affiliation/i })).not.toBeRequired();

    await pick(/what can we help with/i, "API Access Request");
    expect(screen.getByText(/which data do you need\?/i)).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: /affiliation/i })).toBeRequired();
    expect(screen.getByRole("textbox", { name: /describe your project/i })).toBeInTheDocument();

    await pick(/what can we help with/i, "Data Issue");
    expect(screen.queryByText(/which data do you need\?/i)).toBeNull();
  });

  it("blocks submission until the questions are answered", () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    render(<ContactForm isApiAccessRequest />);
    fireEvent.click(screen.getByRole("radio", { name: "No" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Foods" }));
    submit();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent(/api access questions/i);
  });

  const answerAll = async () => {
    await pick(/how will you use the api/i, "Nonprofit");
    await pick(/expected volume/i, ">10k requests/day");
    fireEvent.click(screen.getByRole("radio", { name: "Not sure" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Bioactivity" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Foods" }));
    fireEvent.change(screen.getByRole("textbox", { name: /project or lab url/i }), {
      target: { value: "https://lab.example.org" },
    });
  };

  it("asks for the terms before sending, then posts the answers", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    render(<ContactForm isApiAccessRequest />);
    await answerAll();
    submit();

    const dialog = await screen.findByRole("dialog");
    expect(dialog).toHaveTextContent(/CC BY-NC 4\.0/);
    expect(dialog).toHaveTextContent(/non-commercial use only/i);
    expect(
      screen.getByRole("button", { name: /copy citation/i }),
    ).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();

    // The send button does nothing until the terms box is ticked.
    fireEvent.click(screen.getByRole("button", { name: "Send request" }));
    expect(fetchMock).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("checkbox", { name: /agree to them/i }));
    fireEvent.click(screen.getByRole("button", { name: "Send request" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(postedBody(fetchMock).apiAccess).toEqual({
      useCategory: "Nonprofit",
      commercial: "Not sure",
      dataNeeded: ["Bioactivity", "Foods"],
      volume: ">10k requests/day",
      projectUrl: "https://lab.example.org",
      termsAccepted: true,
    });
  });

  it("sends nothing when the terms are declined", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    render(<ContactForm isApiAccessRequest />);
    await answerAll();
    submit();

    fireEvent.click(await screen.findByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("asks for the tick again each time the popup opens", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true }));
    render(<ContactForm isApiAccessRequest />);
    await answerAll();
    submit();
    fireEvent.click(await screen.findByRole("checkbox", { name: /agree to them/i }));
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());

    submit();
    expect(
      await screen.findByRole("checkbox", { name: /agree to them/i }),
    ).not.toBeChecked();
  });

  it("sends no apiAccess for other topics", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", fetchMock);
    render(<ContactForm isApiAccessRequest={false} />);
    submit();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(postedBody(fetchMock)).not.toHaveProperty("apiAccess");
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});

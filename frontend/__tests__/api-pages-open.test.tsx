import { render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

// HeadlessUI's Listbox measures its anchor.
beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
}));
vi.mock("@/utils/fetching", () => ({ getDownloadEntries: async () => [] }));
vi.mock("@/components/misc/DownloadsTable", () => ({
  default: () => <div>downloads table</div>,
}));

import ContactForm from "@/components/contact/ContactForm";
import Developers from "@/app/(everything-else)/developers/page";
import Downloads from "@/app/(everything-else)/food-composition-downloads/page";

describe("API and downloads pages are open", () => {
  it.each([
    ["developers", async () => <Developers />],
    ["downloads", async () => await Downloads({})],
  ])("renders the %s page without an overlay", async (_, page) => {
    render(await page());
    expect(screen.queryByText(/under construction/i)).toBeNull();
    expect(document.querySelector("[inert]")).toBeNull();
    expect(screen.getByRole("heading", { level: 1 })).toBeInTheDocument();
  });

  it.each([
    ["developers", async () => <Developers />],
    ["downloads", async () => await Downloads({})],
  ])("shows the key box and data license on the %s page", async (_, page) => {
    render(await page());
    expect(
      screen.getByRole("heading", { name: /get an api key/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: /request access via the contact form/i }),
    ).toHaveAttribute("href", "/contact?api-access");
    expect(
      screen.getByRole("button", { name: /copy citation/i }),
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /CC BY-NC 4\.0/ })).toHaveAttribute(
      "href",
      "https://creativecommons.org/licenses/by-nc/4.0/",
    );
  });

  it("drops the under-construction note from API requests", () => {
    render(<ContactForm isApiAccessRequest />);
    expect(screen.queryByText(/under construction/i)).toBeNull();
  });
});

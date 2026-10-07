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

const content = () => screen.getByTestId("under-construction-content");

describe("Under-construction overlay", () => {
  it("blurs the downloads page behind the label", async () => {
    render(await Downloads({}));

    expect(
      screen.getByRole("heading", { name: /under construction/i }),
    ).toBeInTheDocument();
    expect(content()).toHaveClass("blur-md", "pointer-events-none");
    expect(content()).toHaveAttribute("inert");
    // The page copy is still in the DOM (crawlable), just not reachable.
    expect(content().querySelector("h1")).not.toBeNull();
    // Structured data stays outside the inert, blurred wrapper.
    const jsonLd = document.querySelector(
      'script[type="application/ld+json"]',
    );
    expect(jsonLd).not.toBeNull();
    expect(content().contains(jsonLd)).toBe(false);
  });
});

describe("API is open again", () => {
  it("shows the developers page without the overlay", () => {
    render(<Developers />);
    expect(screen.queryByTestId("under-construction-content")).toBeNull();
    expect(screen.queryByText(/under construction/i)).toBeNull();
  });

  it("drops the under-construction note from API requests", () => {
    render(<ContactForm isApiAccessRequest />);
    expect(screen.queryByText(/under construction/i)).toBeNull();
  });
});

import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
}));

import Footer from "@/components/navigation/Footer";

describe("Footer license line", () => {
  it("states both licenses instead of all rights reserved", () => {
    render(<Footer />);
    expect(screen.queryByText(/all rights reserved/i)).toBeNull();
    expect(screen.getByRole("link", { name: /CC BY-NC 4\.0/ })).toHaveAttribute(
      "href",
      "https://creativecommons.org/licenses/by-nc/4.0/",
    );
    expect(screen.getByRole("link", { name: /^MIT/ })).toHaveAttribute(
      "href",
      "https://github.com/AI-Institute-Food-Systems/foodatlas/blob/main/LICENSE",
    );
  });
});

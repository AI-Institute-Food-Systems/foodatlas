import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import Developers from "@/app/(everything-else)/developers/page";

describe("Developers page", () => {
  it("links API-key requests to /contact?api-access", () => {
    render(<Developers />);

    const link = screen.getByRole("link", { name: /request access via the contact form/i });
    expect(link).toHaveAttribute("href", "/contact?api-access");
  });

  it("contains no mailto: links", () => {
    render(<Developers />);

    const links = screen.getAllByRole("link");
    for (const link of links) {
      expect(link.getAttribute("href")).not.toMatch(/^mailto:/);
    }
  });
});

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
}));

import CopyCitation from "@/components/basic/CopyCitation";
import { CANONICAL_PUBLICATION, citationText } from "@/utils/publications";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("CopyCitation", () => {
  it("copies the same reference it shows, with the DOI link", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { clipboard: { writeText } });
    const { container } = render(
      <CopyCitation publication={CANONICAL_PUBLICATION} />,
    );

    const doi = `https://doi.org/${CANONICAL_PUBLICATION.doi}`;
    expect(screen.getByRole("link", { name: new RegExp(doi) })).toHaveAttribute(
      "href",
      doi,
    );

    fireEvent.click(screen.getByRole("button", { name: /copy citation/i }));
    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copied = writeText.mock.calls[0][0] as string;
    expect(copied).toBe(citationText(CANONICAL_PUBLICATION));
    expect(copied.endsWith(doi)).toBe(true);
    expect(copied).not.toContain("*");

    // The rendered text, minus the external-link arrow, is what gets copied.
    const shown = container.querySelector("p")!.textContent!.replace(" ↗︎", "");
    expect(shown).toBe(copied);
    expect(
      await screen.findByRole("button", { name: /citation copied/i }),
    ).toBeInTheDocument();
  });
});

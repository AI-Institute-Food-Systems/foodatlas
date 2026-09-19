import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/food-composition-downloads",
  useSearchParams: () => new URLSearchParams(),
}));

import DownloadsTable from "@/components/misc/DownloadsTable";
import { PaginationsProvider } from "@/context/paginationsContext";

const rows = [
  {
    version: "v4.12",
    release_date: "2026-09-18",
    file_size: "1.2 GB",
    kgc_run: "20260918T100923Z",
    download_link: "https://bucket.s3.amazonaws.com/bundles/v4.12/f.zip",
    summary_link: "",
    summary: "Bioactivity release",
  },
];

const renderTable = (error?: string) =>
  render(
    <PaginationsProvider>
      <DownloadsTable data={rows} error={error} />
    </PaginationsProvider>
  );

// Desktop + mobile both render; take the first form.
const firstForm = () => document.querySelector("form")!;

describe("DownloadsTable gate", () => {
  it("never links to the raw S3 object", () => {
    renderTable();
    expect(document.body.innerHTML).not.toContain("s3.amazonaws.com");
  });

  it("posts the key to the version's download route", () => {
    renderTable();
    fireEvent.change(screen.getByLabelText("API key"), {
      target: { value: "  sk-abc  " },
    });
    const form = firstForm();
    expect(form.getAttribute("method")).toBe("post");
    expect(form.getAttribute("action")).toBe("/food-composition-downloads/v4.12");
    expect(form.querySelector('input[name="key"]')).toHaveValue("sk-abc");
    const submit = vi.fn();
    form.requestSubmit = submit;
    fireEvent.click(screen.getAllByRole("button", { name: /download v4\.12/i })[0]);
    expect(submit).toHaveBeenCalled();
  });

  it("keeps Download disabled until a key is entered", () => {
    renderTable();
    const buttons = screen.getAllByRole("button", { name: /enter your api key/i });
    expect(buttons[0]).toBeDisabled();
    fireEvent.change(screen.getByLabelText("API key"), { target: { value: "k" } });
    expect(screen.getAllByRole("button", { name: /download v4\.12/i })[0]).toBeEnabled();
  });

  it("shows the bounce reason and a request-access link", () => {
    renderTable("invalid_key");
    expect(screen.getByRole("alert")).toHaveTextContent(/not accepted/i);
    expect(screen.getByRole("link", { name: /request access/i })).toHaveAttribute(
      "href",
      "/contact?api-access"
    );
  });
});

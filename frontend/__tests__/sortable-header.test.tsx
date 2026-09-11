// THE header cell, and the one click rule behind every sortable column.
//
// The disease page's Chemicals tab had no sorting at all: its two tables
// hand-rolled their headers and neither copy grew the affordance, while
// the four copies that did exist had drifted — the food composition
// table's arrows pointed the opposite way from everyone else's, and its
// first click on a numeric column gave the smallest values where every
// other table gave the largest.

import { fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

// HeadlessUI's Listbox (inside SortListbox) observes its button.
beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

import {
  MobileSort,
  nextSort,
  Th,
} from "@/components/entities/shared/EvidenceTable";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

const inTable = (th: React.ReactElement) =>
  render(
    <table>
      <thead>
        <tr>{th}</tr>
      </thead>
    </table>
  );

describe("Th", () => {
  it("is a plain label without `sort`", () => {
    inTable(<Th>Signal</Th>);
    expect(screen.queryByRole("button")).toBeNull();
    expect(screen.getByText("Signal")).toBeInTheDocument();
  });

  it("becomes a button with the sort affordance when `sort` is given", () => {
    const onClick = vi.fn();
    inTable(<Th sort={{ active: false, dir: "desc", onClick }}>Assays</Th>);
    fireEvent.click(screen.getByRole("button", { name: /assays/i }));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it("announces the active direction on the cell", () => {
    inTable(<Th sort={{ active: true, dir: "asc", onClick: () => {} }}>A</Th>);
    expect(screen.getByRole("columnheader")).toHaveAttribute("aria-sort", "ascending");
    inTable(<Th sort={{ active: true, dir: "desc", onClick: () => {} }}>B</Th>);
    expect(screen.getAllByRole("columnheader")[1]).toHaveAttribute("aria-sort", "descending");
  });

  it("does not claim a direction when another column is driving the order", () => {
    inTable(<Th sort={{ active: false, dir: "desc", onClick: () => {} }}>A</Th>);
    expect(screen.getByRole("columnheader")).not.toHaveAttribute("aria-sort");
  });

  it("carries the explanation beside a sortable label", () => {
    inTable(
      <Th sort={{ active: false, dir: "desc", onClick: () => {} }} help="what it means">
        Efficacy
      </Th>
    );
    expect(screen.getByRole("img", { name: "About the Efficacy column" })).toBeInTheDocument();
  });
});

describe("nextSort", () => {
  it("starts a new column in its natural direction", () => {
    expect(nextSort({ by: "name", dir: "asc" }, "n_assays", "desc")).toEqual({
      by: "n_assays",
      dir: "desc",
    });
    expect(nextSort({ by: "n_assays", dir: "desc" }, "name", "asc")).toEqual({
      by: "name",
      dir: "asc",
    });
  });

  it("flips the same column", () => {
    expect(nextSort({ by: "n_assays", dir: "desc" }, "n_assays")).toEqual({
      by: "n_assays",
      dir: "asc",
    });
    expect(nextSort({ by: "n_assays", dir: "asc" }, "n_assays")).toEqual({
      by: "n_assays",
      dir: "desc",
    });
  });
});

describe("MobileSort", () => {
  it("offers every column in both directions, in words, and reports the pick", () => {
    const onChange = vi.fn();
    render(
      <MobileSort
        sort={{ by: "n_assays", dir: "desc" }}
        columns={[
          { key: "n_assays", labels: { desc: "Most assays", asc: "Fewest assays" } },
          { key: "name", labels: { asc: "Chemical A–Z", desc: "Chemical Z–A" } },
        ]}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: /sort/i }));
    expect(screen.getByText("Fewest assays")).toBeInTheDocument();
    expect(screen.getByText("Chemical A–Z")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Chemical Z–A"));
    expect(onChange).toHaveBeenCalledWith({ by: "name", dir: "desc" });
  });
});

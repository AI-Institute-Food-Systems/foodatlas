// THE row-expand affordance and the panel it opens.
//
// The two accordion tables — composition data points → premise, assay
// measurements → Hill curve — each carried their own chevron chip and
// their own tinted panel, and the copies had drifted: one chip was `sm`
// and said "Premise", the other `md` and said "Show Hill Curve"; the
// mobile panels had different padding. One chip, one panel, one grammar.

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import {
  RowExpandChip,
  RowExpandPanel,
} from "@/components/entities/shared/EvidenceTable";

describe("RowExpandChip", () => {
  it("says the noun when closed and Hide when open, and names itself either way", () => {
    const { rerender } = render(
      <RowExpandChip what="Hill curve" expanded={false} onToggle={() => {}} />
    );
    const closed = screen.getByRole("button", { name: "Show Hill curve" });
    expect(closed).toHaveTextContent("Hill curve");
    expect(closed).toHaveAttribute("aria-pressed", "false");

    rerender(
      <RowExpandChip what="Hill curve" expanded onToggle={() => {}} />
    );
    const open = screen.getByRole("button", { name: "Hide Hill curve" });
    expect(open).toHaveTextContent("Hide");
    expect(open).toHaveAttribute("aria-pressed", "true");
  });

  it("toggles once per click and keeps the click from the row behind it", () => {
    const onToggle = vi.fn();
    const onRow = vi.fn();
    render(
      <div onClick={onRow}>
        <RowExpandChip what="Premise" expanded={false} onToggle={onToggle} />
      </div>
    );
    fireEvent.click(screen.getByRole("button", { name: "Show Premise" }));
    expect(onToggle).toHaveBeenCalledTimes(1);
    expect(onRow).not.toHaveBeenCalled();
  });

  it("is a table-cell action (md), whatever it opens", () => {
    render(
      <>
        <RowExpandChip what="Premise" expanded={false} onToggle={() => {}} />
        <RowExpandChip what="Hill curve" expanded={false} onToggle={() => {}} />
      </>
    );
    const [a, b] = screen.getAllByRole("button");
    expect(a.className).toBe(b.className);
    expect(a.className).toContain("text-xs");
  });
});

describe("RowExpandPanel", () => {
  it("spans the table as a row when given a colSpan", () => {
    const { container } = render(
      <table>
        <tbody>
          <RowExpandPanel colSpan={6}>detail</RowExpandPanel>
        </tbody>
      </table>
    );
    const td = container.querySelector("td")!;
    expect(td).toHaveAttribute("colspan", "6");
    expect(td).toHaveTextContent("detail");
  });

  it("is a block at the foot of a card without one", () => {
    const { container } = render(<RowExpandPanel>detail</RowExpandPanel>);
    expect(container.querySelector("td")).toBeNull();
    expect(container.firstElementChild).toHaveTextContent("detail");
  });

  it("carries the same accent rule and tint in both forms", () => {
    const row = render(
      <table>
        <tbody>
          <RowExpandPanel colSpan={2}>x</RowExpandPanel>
        </tbody>
      </table>
    ).container.querySelector("td")!;
    const block = render(<RowExpandPanel>x</RowExpandPanel>).container
      .firstElementChild!;
    for (const el of [row, block]) {
      expect(el.className).toContain("border-l-accent-600");
      expect(el.className).toContain("bg-light-900/30");
    }
  });
});

import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import Skeleton from "@/components/basic/Skeleton";
import { SKELETON_SHAPE, SKELETON_TONE } from "@/components/basic/skeletonTokens";

const renderSkeleton = (ui: React.ReactElement) => {
  const { container } = render(ui);
  const el = container.firstElementChild;
  if (!(el instanceof HTMLElement)) throw new Error("no skeleton rendered");
  return el;
};

describe("Skeleton", () => {
  it("is hidden from assistive tech", () => {
    // The bars are decorative; the surrounding container owns the
    // role="status" announcement so a 20-row table doesn't read out
    // "loading" 80 times.
    expect(renderSkeleton(<Skeleton />)).toHaveAttribute("aria-hidden");
  });

  it("rests at light-800 and brightens rather than fading", () => {
    // Regression guard for the original defect: the fill was bg-light-950,
    // identical to Card, so skeletons inside a Card were invisible. An
    // opacity-based animate-pulse would reintroduce that mid-cycle, hence
    // the dedicated keyframe.
    const el = renderSkeleton(<Skeleton />);
    expect(el).toHaveClass("bg-light-800");
    expect(el).toHaveClass("motion-safe:animate-skeleton-pulse");
    expect(el.className).not.toMatch(/(^|\s|:)animate-pulse\b/);
  });

  it("rests at a lighter fill when motion is reduced", () => {
    // With no pulse to carry the signal, the fill has to.
    expect(renderSkeleton(<Skeleton />)).toHaveClass(
      "motion-reduce:bg-light-700"
    );
  });

  it("supports the cream tone for genuinely cream elements", () => {
    const el = renderSkeleton(<Skeleton tone="cream" />);
    expect(el).toHaveClass("bg-light-200/60");
    expect(el).not.toHaveClass("bg-light-800");
  });

  it.each([
    ["text", SKELETON_SHAPE.text],
    ["block", SKELETON_SHAPE.block],
    ["pill", SKELETON_SHAPE.pill],
  ] as const)("maps the %s shape to its radius", (shape, expected) => {
    expect(renderSkeleton(<Skeleton shape={shape} />)).toHaveClass(expected);
  });

  it("lets a caller override the default height", () => {
    // twMerge, not concatenation — every table passes its own height and
    // would otherwise fight the h-4 default.
    const el = renderSkeleton(<Skeleton className="h-9" />);
    expect(el).toHaveClass("h-9");
    expect(el).not.toHaveClass("h-4");
  });

  it("exposes exactly the tones the type allows", () => {
    expect(Object.keys(SKELETON_TONE).sort()).toEqual(["cream", "default"]);
  });
});

// The Hill curve's labels are the same size on every screen.
//
// Fluid mode used to keep the supplied 720×320 as the viewBox and let the
// browser scale the whole drawing to the container. Label sizes were in
// viewBox units, so on a phone (container ~300px) they came out at a few
// pixels, and after being bumped to read on phones, at ~20px on a desktop
// (container ~750px) — the readout dwarfed the 11px text beside it.
// Fluid mode now draws at the container's real pixel size, 1:1.

import { render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import HillCurveSparkline from "@/components/entities/bioactivity/HillCurveSparkline";

const fit = { zero: 0, infinite: 100, logAC50: -6, slope: 1 };

const rendered = (w: number, h: number) =>
  vi
    .spyOn(SVGElement.prototype, "getBoundingClientRect")
    .mockReturnValue({ width: w, height: h, top: 0, left: 0, right: w, bottom: h, x: 0, y: 0, toJSON: () => ({}) } as DOMRect);

const fontSizes = (container: HTMLElement) =>
  Array.from(container.querySelectorAll("text")).map((t) =>
    Number(t.getAttribute("fontSize") ?? t.getAttribute("font-size"))
  );

afterEach(() => vi.restoreAllMocks());

describe("HillCurveSparkline in fluid mode", () => {
  it.each([
    ["a desktop modal", 750, 333],
    ["a phone", 300, 133],
  ])("on %s, draws at the container's pixel size with 11/10px labels", (_, w, h) => {
    rendered(w, h);
    const { container } = render(
      <HillCurveSparkline {...fit} width={720} height={320} fluid />
    );
    const svg = container.querySelector("svg")!;
    expect(svg.getAttribute("viewBox")).toBe(`0 0 ${w} ${h}`);
    expect(svg.getAttribute("width")).toBe("100%");
    const sizes = fontSizes(container);
    expect(sizes.length).toBeGreaterThan(0);
    expect(new Set(sizes)).toEqual(new Set([11, 10]));
  });

  it("puts the readout at the right edge of the drawn size, not the nominal one", () => {
    rendered(300, 133);
    const { container } = render(
      <HillCurveSparkline {...fit} width={720} height={320} fluid />
    );
    const readout = container.querySelector("text")!;
    expect(Number(readout.getAttribute("x"))).toBe(300);
  });
});

describe("HillCurveSparkline at a fixed size", () => {
  it("keeps the compact sparkline's small ticks", () => {
    const { container } = render(<HillCurveSparkline {...fit} />);
    expect(container.querySelector("svg")!.getAttribute("viewBox")).toBe("0 0 160 64");
    expect(new Set(fontSizes(container))).toEqual(new Set([8, 7]));
  });
});

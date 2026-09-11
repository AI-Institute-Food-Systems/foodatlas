// THE hover info box. Positioned from the trigger's own rect, fixed to
// the viewport, in a portal — so where it lands cannot depend on which
// ancestor happens to be positioned or scrolling.
//
// The reported bug: the "i" on the food page's Efficacy column header,
// near the bottom of the viewport, opened its bubble at the bottom of
// the SCREEN. The old bubble was absolutely positioned inside the
// trigger and, on flipping, set `bottom: 0` — which resolves against the
// nearest positioned ancestor, not the trigger.

import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { InfoTip, Tooltip } from "@/components/basic/Tooltip";

const rect = (top: number, left: number, width: number, height: number) =>
  ({
    top,
    left,
    width,
    height,
    bottom: top + height,
    right: left + width,
    x: left,
    y: top,
    toJSON: () => ({}),
  }) as DOMRect;

// jsdom has no layout, so give the trigger and the bubble real rects.
// The bubble is the element carrying role="tooltip"; everything else on
// the way up to the trigger span reports the trigger's rect.
const layout = (trigger: DOMRect, bubble: DOMRect) =>
  vi
    .spyOn(HTMLElement.prototype, "getBoundingClientRect")
    .mockImplementation(function (this: HTMLElement) {
      return this.getAttribute("role") === "tooltip" ||
        this.closest('[role="tooltip"]')
        ? bubble
        : trigger;
    });

const open = async () => {
  fireEvent.mouseEnter(screen.getByText("trigger"));
  await act(async () => {
    vi.advanceTimersByTime(300);
  });
  return screen.getByRole("tooltip") as HTMLElement;
};

beforeEach(() => {
  vi.useFakeTimers();
  Object.defineProperty(window, "innerHeight", { value: 800, configurable: true });
  Object.defineProperty(window, "innerWidth", { value: 1200, configurable: true });
});
afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Tooltip", () => {
  it("renders into <body>, not inside the trigger", async () => {
    layout(rect(400, 600, 16, 16), rect(0, 0, 120, 40));
    const { container } = render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    const tip = await open();
    expect(container.contains(tip)).toBe(false);
    expect(document.body.contains(tip)).toBe(true);
    expect(tip.textContent).toContain("hello");
  });

  it("sits just above a trigger near the bottom of the viewport — the reported case", async () => {
    // Trigger 16px tall at y=740 in an 800px viewport. Plenty of room
    // above; the bubble must hang off the trigger, not off the screen edge.
    layout(rect(740, 600, 16, 16), rect(0, 0, 120, 40));
    render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    const tip = await open();
    expect(tip.dataset.side).toBe("top");
    expect(tip.style.position || getComputedStyle(tip).position).not.toBe("absolute");
    expect(tip.style.top).toBe(`${740 - 8 - 40}px`);
    // Centred on the trigger.
    expect(tip.style.left).toBe(`${600 + 8 - 60}px`);
  });

  it("flips below a trigger that has no room above", async () => {
    layout(rect(10, 600, 16, 16), rect(0, 0, 120, 40));
    render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    const tip = await open();
    expect(tip.dataset.side).toBe("bottom");
    expect(tip.style.top).toBe(`${10 + 16 + 8}px`);
  });

  it("honours placement=bottom, and flips up when the bottom has no room", async () => {
    layout(rect(790, 600, 16, 16), rect(0, 0, 120, 40));
    render(
      <Tooltip content="hello" placement="bottom">
        <span>trigger</span>
      </Tooltip>
    );
    const tip = await open();
    expect(tip.dataset.side).toBe("top");
  });

  it("stays inside the viewport horizontally", async () => {
    // Trigger at the far right; a centred bubble would overflow.
    layout(rect(400, 1190, 8, 16), rect(0, 0, 200, 40));
    render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    const tip = await open();
    expect(parseFloat(tip.style.left) + 200).toBeLessThanOrEqual(1200 - 8);
  });

  it("closes on mouse leave and on Escape", async () => {
    layout(rect(400, 600, 16, 16), rect(0, 0, 120, 40));
    render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    await open();
    fireEvent.mouseLeave(screen.getByText("trigger"));
    expect(screen.queryByRole("tooltip")).toBeNull();

    await open();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("tooltip")).toBeNull();
  });

  it("does not open on a hover shorter than the delay", () => {
    layout(rect(400, 600, 16, 16), rect(0, 0, 120, 40));
    render(
      <Tooltip content="hello">
        <span>trigger</span>
      </Tooltip>
    );
    fireEvent.mouseEnter(screen.getByText("trigger"));
    vi.advanceTimersByTime(100);
    fireEvent.mouseLeave(screen.getByText("trigger"));
    vi.advanceTimersByTime(500);
    expect(screen.queryByRole("tooltip")).toBeNull();
  });
});

describe("InfoTip", () => {
  it("is the one 'i' glyph, named for what it explains", () => {
    render(<InfoTip content="what it means" label="About the Efficacy column" />);
    const glyph = screen.getByRole("img", { name: "About the Efficacy column" });
    expect(glyph.tagName.toLowerCase()).toBe("svg");
  });
});

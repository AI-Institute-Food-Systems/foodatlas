import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { useDeferredLoading } from "@/hooks/useDeferredLoading";

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

const advance = (ms: number) => act(() => void vi.advanceTimersByTime(ms));

describe("useDeferredLoading", () => {
  it("shows nothing for a response that beats the delay", () => {
    // The case this exists for: locally the API answers in 15-50ms, so
    // the skeleton was one frozen frame of a 2s pulse — a flicker, not an
    // affordance.
    const { result, rerender } = renderHook(
      ({ loading }) => useDeferredLoading(loading),
      { initialProps: { loading: true } }
    );
    advance(50);
    rerender({ loading: false });
    advance(1000);
    expect(result.current).toBe(false);
  });

  it("shows the skeleton once the fetch outlasts the delay", () => {
    const { result } = renderHook(() => useDeferredLoading(true));
    expect(result.current).toBe(false);
    advance(199);
    expect(result.current).toBe(false);
    advance(2);
    expect(result.current).toBe(true);
  });

  it("holds the skeleton for a floor once shown", () => {
    // Without this, data landing just after the delay produces a few-ms
    // flash — the very artefact the delay removes.
    const { result, rerender } = renderHook(
      ({ loading }) => useDeferredLoading(loading),
      { initialProps: { loading: true } }
    );
    advance(210);
    expect(result.current).toBe(true);

    rerender({ loading: false });
    advance(100);
    expect(result.current).toBe(true); // still inside the floor
    advance(350);
    expect(result.current).toBe(false);
  });

  it("measures the floor from first paint, not from the request start", () => {
    const { result, rerender } = renderHook(
      ({ loading }) => useDeferredLoading(loading),
      { initialProps: { loading: true } }
    );
    advance(200); // skeleton appears
    advance(500); // ...and has now been up longer than the floor
    rerender({ loading: false });
    advance(1);
    expect(result.current).toBe(false); // no extra hold
  });

  it("accepts custom thresholds", () => {
    const { result } = renderHook(() =>
      useDeferredLoading(true, { delayMs: 50, floorMs: 100 })
    );
    advance(51);
    expect(result.current).toBe(true);
  });
});

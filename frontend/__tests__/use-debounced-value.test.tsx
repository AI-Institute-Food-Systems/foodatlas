import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useDebouncedValue } from "@/hooks/useDebouncedValue";

afterEach(() => {
  vi.useRealTimers();
});

describe("useDebouncedValue", () => {
  it("returns the initial value without waiting", () => {
    // First paint must not be held back by the debounce — the tables key
    // their first fetch off this too.
    vi.useFakeTimers();
    const { result } = renderHook(() => useDebouncedValue("tomato", 250));

    expect(result.current).toBe("tomato");
  });

  it("holds a change until the delay elapses", () => {
    vi.useFakeTimers();
    const { result, rerender } = renderHook(
      ({ value }) => useDebouncedValue(value, 250),
      { initialProps: { value: "" } }
    );

    rerender({ value: "tom" });
    expect(result.current).toBe("");

    act(() => {
      vi.advanceTimersByTime(249);
    });
    expect(result.current).toBe("");

    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(result.current).toBe("tom");
  });

  it("emits once for a burst of keystrokes", () => {
    // The actual defect: typing "tomato" fired six requests, each of
    // which blanked the table.
    vi.useFakeTimers();
    const { result, rerender } = renderHook(
      ({ value }) => useDebouncedValue(value, 250),
      { initialProps: { value: "" } }
    );

    for (const value of ["t", "to", "tom", "toma", "tomat", "tomato"]) {
      rerender({ value });
      act(() => {
        vi.advanceTimersByTime(50);
      });
    }

    // 300ms of typing, but no gap ever reached 250ms.
    expect(result.current).toBe("");

    act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(result.current).toBe("tomato");
  });
});

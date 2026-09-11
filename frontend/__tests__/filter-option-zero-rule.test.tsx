// FilterOption owns the zero-count rule. It was written by hand at eleven
// call sites, and the sites that forgot it hid the option instead — so
// the same facet disabled its empties on one page and dropped them on the
// next. These pin what the primitive guarantees so no site has to.

import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { FilterOption } from "@/components/entities/shared/filters/FilterControls";

// Fresh container per call: several tests mount more than one option,
// and a radio renders role="radio", so query the element not the role.
const option = (props: Partial<Parameters<typeof FilterOption>[0]>) => {
  const { container } = render(
    <FilterOption
      label="opt"
      selected={false}
      onClick={() => {}}
      {...props}
    />
  );
  return container.querySelector("button") as HTMLButtonElement;
};

describe("FilterOption's zero-count rule", () => {
  it("disables a zero that is not selected", () => {
    expect(option({ count: 0 })).toBeDisabled();
  });

  it("keeps a zero that IS selected clickable, so it can be dropped", () => {
    // The composition Source facet starts with everything on; on pepper
    // (raw) FDC is 0 and was greyed out AND checked — stuck in the query.
    expect(option({ count: 0, selected: true })).toBeEnabled();
  });

  it("never disables the group's reset option", () => {
    // "All" carries the current total, which a search can drive to 0.
    // Disabling it there would leave no way back.
    expect(option({ count: 0, resetOption: true, mode: "radio" })).toBeEnabled();
  });

  it("leaves a positive count enabled", () => {
    expect(option({ count: 3 })).toBeEnabled();
  });

  it("does not disable before the counts have loaded", () => {
    // A row with no count yet is "unknown", not "zero".
    expect(option({ countsLoaded: false })).toBeEnabled();
    expect(option({ count: undefined })).toBeEnabled();
  });

  it("ORs an explicit disabled reason with the zero rule", () => {
    // A loading skeleton or a dimmed source is an extra reason to
    // disable, never a way to re-enable a zero.
    expect(option({ count: 5, disabled: true })).toBeDisabled();
    expect(option({ count: 0, disabled: false })).toBeDisabled();
  });

  it("mirrors the state into aria-disabled for the facet crawler", () => {
    expect(option({ count: 0 })).toHaveAttribute("aria-disabled", "true");
    expect(option({ count: 1 })).not.toHaveAttribute("aria-disabled");
  });
});

describe("FilterOption's label", () => {
  it("is available in full on hover, since the sidebar truncates it", () => {
    // "molecular-level" renders as "Molecular…" in the w-48 sidebar. The
    // tooltip is on the row, so hovering the tick or the count shows it
    // too — not only the clipped text itself.
    expect(option({ label: "molecular-level", count: 3 })).toHaveAttribute(
      "title",
      "molecular-level"
    );
  });
});

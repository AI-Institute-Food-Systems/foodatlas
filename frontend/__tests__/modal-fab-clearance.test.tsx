// On phones the modal sheet reaches the viewport bottom, and the
// "Report an issue" FAB — fixed bottom-right, deliberately above the
// dialog so a report can start from inside a table modal — sat exactly
// on the pager's Next/Last buttons. Pages 2+ of any evidence list were
// unreachable on a phone. The panel now reserves the FAB's footprint.

import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/context/reportModeContext", () => ({
  useReportModeState: () => ({
    isSelectMode: false,
    toggleSelectMode: () => {},
    exitSelectMode: () => {},
    activeContext: null,
    closeModal: () => {},
  }),
}));
vi.mock("@/components/basic/ReportIssueModal", () => ({
  default: () => null,
}));

import Modal, { FAB_CLEARANCE_PB } from "@/components/basic/Modal";
import ReportFab from "@/components/basic/ReportFab";

describe("modal clears the report FAB on small screens", () => {
  it("pads the panel bottom by the FAB's footprint below md", () => {
    render(
      <Modal isOpen onClose={() => {}} title="Data points">
        <button type="button">Next page</button>
      </Modal>
    );
    const panel = screen.getByText("Next page").closest("[data-headlessui-state]");
    expect(panel).not.toBeNull();
    for (const cls of FAB_CLEARANCE_PB.split(" ")) {
      expect(panel!.className.split(/\s+/)).toContain(cls);
    }
  });

  it("keeps the FAB where the clearance was measured for", () => {
    // 3.75rem = bottom-4 (1rem) + a ~2.25rem-tall pill + air. If the
    // FAB moves or grows, revisit FAB_CLEARANCE_PB together with it.
    const { container } = render(<ReportFab />);
    const wrapper = container.querySelector(".fixed");
    expect(wrapper?.className).toMatch(/\bbottom-4\b/);
    expect(wrapper?.className).toMatch(/\bright-4\b/);
    expect(wrapper?.className).toMatch(/\bz-\[60\]/);
    expect(screen.getByRole("button").className).toMatch(/\bpy-2\b/);
  });
});

// THE maintenance bar is an env switch, nothing more: absent var → nothing
// in the DOM at all; present var → its text, announced as a status region.

import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import MaintenanceNotice from "@/components/misc/MaintenanceNotice";

describe("MaintenanceNotice", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("renders nothing when NEXT_PUBLIC_MAINTENANCE_NOTICE is unset", () => {
    vi.stubEnv("NEXT_PUBLIC_MAINTENANCE_NOTICE", "");
    const { container } = render(<MaintenanceNotice />);
    expect(container).toBeEmptyDOMElement();
  });

  it("shows the message as a status region when the var is set", () => {
    vi.stubEnv(
      "NEXT_PUBLIC_MAINTENANCE_NOTICE",
      "Scheduled maintenance until 15:00 UTC."
    );
    render(<MaintenanceNotice />);
    expect(screen.getByRole("status")).toHaveTextContent(
      "Scheduled maintenance until 15:00 UTC."
    );
  });
});

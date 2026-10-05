import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { middleware } from "@/middleware";

// /food/e2908 → /food/cow--milk. The id always names the same entity, so the
// redirect is permanent (A8): a 307 told crawlers to keep the id URL.
describe("entity id redirect", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "https://api.test");
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        Response.json({ entity_type: "food", common_name: "cow milk" })
      )
    );
  });
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("is a 308 to the name URL", async () => {
    const res = await middleware(
      new NextRequest("https://www.foodatlas.ai/food/e2908")
    );
    expect(res.status).toBe(308);
    expect(res.headers.get("location")).toBe(
      "https://www.foodatlas.ai/food/cow--milk"
    );
  });

  it("leaves a name slug alone", async () => {
    const res = await middleware(
      new NextRequest("https://www.foodatlas.ai/food/papaya")
    );
    expect(res.headers.get("location")).toBeNull();
    expect(fetch).not.toHaveBeenCalled();
  });
});

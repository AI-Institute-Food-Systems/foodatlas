import { beforeEach, describe, expect, it, vi } from "vitest";

import { GET, HEAD } from "@/app/%5Fproxy-api/[...path]/route";

// The proxy attaches our API key to whatever it forwards, so "which paths may
// it forward" is an authorization boundary, not a routing detail.
//
// It previously forwarded anything: /_proxy-api/v1/stats returned 200 to an
// anonymous caller while api.foodatlas.ai/v1/stats returned 401, which made the
// /v1 key ledger and its per-key rate limit bypassable from any browser. These
// pin the boundary rather than the current router list — adding a router to
// ALLOWED_ROUTERS is a deliberate act that should require touching this file.

const KEY = "test-key-value";

const call = (path: string[], headers: Record<string, string> = {}) =>
  GET(
    new Request(`https://www.foodatlas.ai/_proxy-api/${path.join("/")}`, {
      headers,
    }),
    {
      params: { path },
    },
  );

describe("/_proxy-api authorization", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "https://api.test");
    vi.stubEnv("API_KEY", KEY);
    vi.restoreAllMocks();
  });

  const fetchSpy = () =>
    vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(
        new Response("{}", { headers: { "content-type": "application/json" } }),
      );

  it("forwards the routers the UI actually calls", async () => {
    const spy = fetchSpy();
    for (const path of [
      ["food", "composition"],
      ["chemical", "correlation"],
      ["disease", "correlation"],
      ["bioactivity", "endpoints"],
      ["download", "x"],
      ["metadata", "statistics"],
      ["metadata", "search"],
    ]) {
      const res = await call(path);
      expect(res.status, path.join("/")).toBe(200);
    }
    expect(spy).toHaveBeenCalledTimes(7);
  });

  it("refuses metadata/entities — 780 KB index the browser never needs", async () => {
    // Read only by app/sitemap.ts, which runs server-side and bypasses this
    // proxy entirely.
    const spy = fetchSpy();
    expect((await call(["metadata", "entities"])).status).toBe(404);
    expect((await call(["metadata"])).status).toBe(404);
    expect(spy).not.toHaveBeenCalled();
  });

  it("refuses /v1 — the metered surface the ledger exists to protect", async () => {
    const spy = fetchSpy();
    for (const path of [
      ["v1", "stats"],
      ["v1", "bundles"],
      ["v1", "foods", "e1"],
    ]) {
      const res = await call(path);
      expect(res.status, path.join("/")).toBe(404);
    }
    expect(spy).not.toHaveBeenCalled();
  });

  it("refuses an unknown router without ever attaching the key", async () => {
    const spy = fetchSpy();
    expect((await call(["openapi.json"])).status).toBe(404);
    expect((await call(["docs"])).status).toBe(404);
    expect((await call([])).status).toBe(404);
    expect(spy).not.toHaveBeenCalled();
  });

  it("refuses path traversal segments", async () => {
    const spy = fetchSpy();
    expect((await call(["food", "..", "v1", "stats"])).status).toBe(400);
    expect((await call(["food", "."])).status).toBe(400);
    expect(spy).not.toHaveBeenCalled();
  });

  it("refuses cross-site callers", async () => {
    const spy = fetchSpy();
    const res = await call(["food", "composition"], {
      "sec-fetch-site": "cross-site",
    });
    expect(res.status).toBe(403);
    expect(spy).not.toHaveBeenCalled();
  });

  it("allows same-origin and header-less callers", async () => {
    // Googlebot's renderer and older browsers omit Sec-Fetch-Site; blocking
    // them would stop entity pages rendering for crawlers.
    const spy = fetchSpy();
    expect(
      (await call(["food", "composition"], { "sec-fetch-site": "same-origin" }))
        .status,
    ).toBe(200);
    expect((await call(["food", "composition"])).status).toBe(200);
    expect(spy).toHaveBeenCalledTimes(2);
  });

  it("sends the key upstream, and never the caller's own Authorization", async () => {
    const spy = fetchSpy();
    await call(["food", "composition"], {
      authorization: "Bearer attacker-supplied",
    });
    const init = spy.mock.calls[0][1] as RequestInit;
    expect(init.headers).toEqual({ Authorization: `Bearer ${KEY}` });
  });

  it("applies the same rules to HEAD", async () => {
    const spy = fetchSpy();
    const blocked = await HEAD(
      new Request("https://www.foodatlas.ai/_proxy-api/v1/stats"),
      { params: { path: ["v1", "stats"] } },
    );
    expect(blocked.status).toBe(404);
    expect(spy).not.toHaveBeenCalled();
  });
});

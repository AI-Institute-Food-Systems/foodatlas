import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// UMAMI_ENABLED is computed at import time from VERCEL_ENV, so each block
// sets the env and re-imports the module fresh.
const load = async (vercelEnv: string | undefined) => {
  vi.resetModules();
  if (vercelEnv === undefined) delete process.env.VERCEL_ENV;
  else process.env.VERCEL_ENV = vercelEnv;
  return await import("@/utils/umami");
};

const request = (headers: Record<string, string> = {}) =>
  new Request("https://foodatlas.ai/food-composition-downloads/v4.12", {
    headers,
  });

afterEach(() => {
  delete process.env.VERCEL_ENV;
  delete window.umami;
  vi.unstubAllGlobals();
});

describe("track", () => {
  it("is a no-op without window.umami", async () => {
    const { track } = await load("production");
    expect(() => track("x", { a: 1 })).not.toThrow();
  });

  it("forwards name and data when the tracker is present", async () => {
    const spy = vi.fn();
    window.umami = { track: spy };
    const { track } = await load("production");
    track("search_select", { query: "tomato" });
    expect(spy).toHaveBeenCalledWith("search_select", { query: "tomato" });
  });
});

describe("sendUmamiEvent", () => {
  let fetchMock: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    fetchMock = vi.fn().mockResolvedValue(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);
  });

  it("does nothing outside production", async () => {
    const { sendUmamiEvent } = await load("preview");
    await sendUmamiEvent(request(), "bundle_download", { version: "v1" });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("builds the payload from the request headers", async () => {
    const { sendUmamiEvent, UMAMI_WEBSITE_ID, UMAMI_HOST } =
      await load("production");
    await sendUmamiEvent(
      request({
        "x-forwarded-for": "203.0.113.9, 10.0.0.1",
        "x-forwarded-host": "foodatlas.ai",
        "user-agent": "Mozilla/5.0 test",
        referer: "https://foodatlas.ai/food-composition-downloads",
      }),
      "bundle_download",
      { version: "v4.12", kgc_run: "20260918T100923Z", file_size: "1.2 GB" }
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`${UMAMI_HOST}/api/send`);
    expect(init.method).toBe("POST");
    expect(init.headers["User-Agent"]).toBe("Mozilla/5.0 test");
    const body = JSON.parse(init.body);
    expect(body.type).toBe("event");
    expect(body.payload).toMatchObject({
      website: UMAMI_WEBSITE_ID,
      hostname: "foodatlas.ai",
      url: "/food-composition-downloads/v4.12",
      name: "bundle_download",
      data: { version: "v4.12", kgc_run: "20260918T100923Z" },
      ip: "203.0.113.9",
      userAgent: "Mozilla/5.0 test",
      referrer: "https://foodatlas.ai/food-composition-downloads",
    });
  });

  it("falls back to x-real-ip and omits unknown fields", async () => {
    const { sendUmamiEvent } = await load("production");
    await sendUmamiEvent(request({ "x-real-ip": "198.51.100.4" }), "e", {});
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(body.payload.ip).toBe("198.51.100.4");
    expect(body.payload).not.toHaveProperty("userAgent");
    expect(body.payload).not.toHaveProperty("referrer");
  });

  it("swallows fetch rejections", async () => {
    fetchMock.mockRejectedValue(new Error("ECONNREFUSED"));
    const { sendUmamiEvent } = await load("production");
    await expect(
      sendUmamiEvent(request(), "e", {})
    ).resolves.toBeUndefined();
  });

  it("aborts after the timeout", async () => {
    vi.useFakeTimers();
    fetchMock.mockImplementation(
      (_url: string, init: { signal: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          init.signal.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError"))
          );
        })
    );
    const { sendUmamiEvent } = await load("production");
    const pending = sendUmamiEvent(request(), "e", {}, { timeoutMs: 50 });
    await vi.advanceTimersByTimeAsync(60);
    await expect(pending).resolves.toBeUndefined();
    expect(fetchMock.mock.calls[0][1].signal.aborted).toBe(true);
    vi.useRealTimers();
  });
});

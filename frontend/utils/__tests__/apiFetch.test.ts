import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { apiFetch, clearApiCache } from "@/utils/apiFetch";

const DAY = 86400;

const okResponse = (body: unknown) => ({
  ok: true,
  status: 200,
  json: async () => body,
});

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  clearApiCache();
  fetchMock = vi.fn(async () => okResponse({ data: ["row"] }));
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("apiFetch client cache", () => {
  it("serves a repeat request from cache", async () => {
    const first = await apiFetch("/api/food?x=1", { revalidate: DAY });
    const second = await apiFetch("/api/food?x=1", { revalidate: DAY });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(await first.json()).toEqual({ data: ["row"] });
    expect(await second.json()).toEqual({ data: ["row"] });
  });

  it("collapses concurrent requests for the same url into one fetch", async () => {
    const [a, b, c] = await Promise.all([
      apiFetch("/api/food?x=1", { revalidate: DAY }),
      apiFetch("/api/food?x=1", { revalidate: DAY }),
      apiFetch("/api/food?x=1", { revalidate: DAY }),
    ]);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(await a.json()).toEqual({ data: ["row"] });
    expect(await b.json()).toEqual({ data: ["row"] });
    expect(await c.json()).toEqual({ data: ["row"] });
  });

  it("keys on the full url, so different params are different entries", async () => {
    await apiFetch("/api/food?page=1", { revalidate: DAY });
    await apiFetch("/api/food?page=2", { revalidate: DAY });

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("sends the api key", async () => {
    await apiFetch("/api/food?x=1", { revalidate: DAY });

    const [, init] = fetchMock.mock.calls[0];
    expect(init.headers.Authorization).toMatch(/^Bearer /);
  });

  it("does not cache a failed response", async () => {
    fetchMock.mockResolvedValueOnce({
      ok: false,
      status: 502,
      json: async () => ({ message: "bad gateway" }),
    });

    const bad = await apiFetch("/api/food?x=1", { revalidate: DAY });
    expect(bad.ok).toBe(false);
    expect(bad.status).toBe(502);

    // A cached failure would pin an empty state for the whole session.
    const retry = await apiFetch("/api/food?x=1", { revalidate: DAY });
    expect(retry.ok).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("refetches once the ttl has elapsed", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-13T00:00:00Z"));

    await apiFetch("/api/download", { revalidate: 300 });
    vi.setSystemTime(new Date("2026-08-13T00:04:00Z")); // 240s — still fresh
    await apiFetch("/api/download", { revalidate: 300 });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    vi.setSystemTime(new Date("2026-08-13T00:05:01Z")); // past 300s
    await apiFetch("/api/download", { revalidate: 300 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not cache when no revalidate window is given", async () => {
    await apiFetch("/api/food?x=1");
    await apiFetch("/api/food?x=1");

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("bounds the cache, evicting the oldest entries", async () => {
    // 200 is the cap; the 201st write must push the first url out.
    for (let i = 0; i < 201; i++) {
      await apiFetch(`/api/food?page=${i}`, { revalidate: DAY });
    }
    expect(fetchMock).toHaveBeenCalledTimes(201);

    await apiFetch("/api/food?page=0", { revalidate: DAY });
    expect(fetchMock).toHaveBeenCalledTimes(202);

    // A recent entry is still cached, so this isn't just "nothing caches".
    await apiFetch("/api/food?page=200", { revalidate: DAY });
    expect(fetchMock).toHaveBeenCalledTimes(202);
  });

  it("clearApiCache drops cached entries", async () => {
    await apiFetch("/api/food?x=1", { revalidate: DAY });
    clearApiCache();
    await apiFetch("/api/food?x=1", { revalidate: DAY });

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

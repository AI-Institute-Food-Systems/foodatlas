// Single entry point for every FoodAtlas API request.
//
// On the server, Next's data cache already does the work — `next.revalidate`
// is honoured and this is a plain pass-through.
//
// In the browser `next.revalidate` is silently ignored, and the /_proxy-api
// handler answers `Cache-Control: no-store`, so the HTTP cache doesn't help
// either. (The browser sends no Authorization header — the handler attaches
// the key server-side.) So we keep our own:
// an in-flight map that collapses duplicate concurrent requests, plus a
// bounded TTL map of parsed responses. That is what makes remounting a
// component — reopening an entity tab, navigating back to a page you just
// left — instant instead of a fresh round-trip.
//
// The cache lives in module memory only, so a reload always gets fresh data.

import { track } from "@/utils/umami";

// Matches the shape of the platform `Response` members that callers use, so
// call sites read the same whether they're served from cache or the network.
// `json()` mirrors Response.json()'s own `Promise<any>` signature.
export type ApiResponse = {
  ok: boolean;
  status: number;
  json: () => Promise<any>;
};

type CacheEntry = { at: number; body: unknown };

// Entity payloads are paginated and small; 200 entries covers a deep session
// of tab and page switching without letting the map grow without bound.
const MAX_ENTRIES = 200;

const cache = new Map<string, CacheEntry>();
const inFlight = new Map<string, Promise<ApiResponse>>();

// Exported for tests; also a hook for a future "refresh" affordance.
export const clearApiCache = (): void => {
  cache.clear();
  inFlight.clear();
};

// Path with ids collapsed, so the api_fetch_error property has one value per
// endpoint rather than one per entity. Query string dropped for the same
// reason.
const stripIds = (url: string): string =>
  url
    .split("?")[0]
    .replace(/^https?:\/\/[^/]+/, "")
    .replace(/\/e\d+(?=\/|$)/g, "/{id}")
    .replace(/\/\d+(?=\/|$)/g, "/{id}");

// Server-only. API_KEY has no NEXT_PUBLIC_ prefix on purpose: the browser
// must never see it. Client-side requests go to /_proxy-api, whose route
// handler attaches this same key on the server.
const authHeaders = () => ({
  Authorization: `Bearer ${process.env.API_KEY}`,
});

const cached = (body: unknown): ApiResponse => ({
  ok: true,
  status: 200,
  json: async () => body,
});

const remember = (url: string, body: unknown) => {
  // Map iterates in insertion order, so the first key is the oldest write.
  if (cache.size >= MAX_ENTRIES) {
    const oldest = cache.keys().next();
    if (!oldest.done) cache.delete(oldest.value);
  }
  cache.set(url, { at: Date.now(), body });
};

export async function apiFetch(
  url: string,
  opts: { revalidate?: number } = {}
): Promise<ApiResponse> {
  const { revalidate } = opts;

  if (typeof window === "undefined") {
    const res = await fetch(url, {
      headers: authHeaders(),
      ...(revalidate === undefined ? {} : { next: { revalidate } }),
    });
    return { ok: res.ok, status: res.status, json: () => res.json() };
  }

  // The client TTL deliberately tracks the caller's own revalidate window, so
  // a given endpoint goes stale at the same rate on both sides of the render
  // boundary and there's no second number to keep in sync.
  const ttlMs = (revalidate ?? 0) * 1000;

  const hit = cache.get(url);
  if (hit && Date.now() - hit.at < ttlMs) return cached(hit.body);
  if (hit) cache.delete(url);

  const pending = inFlight.get(url);
  if (pending) return pending;

  const request = (async (): Promise<ApiResponse> => {
    let res: Response;
    try {
      res = await fetch(url);
    } catch (err) {
      track("api_fetch_error", { path: stripIds(url), status: "network" });
      throw err;
    }
    if (!res.ok) {
      track("api_fetch_error", { path: stripIds(url), status: res.status });
      // Never cache a failure: the getX wrappers degrade to null/throw on
      // error, and a cached miss would pin that empty state for the session.
      return { ok: false, status: res.status, json: () => res.json() };
    }
    const body = await res.json();
    if (ttlMs > 0) remember(url, body);
    return { ok: true, status: res.status, json: async () => body };
  })();

  inFlight.set(url, request);
  try {
    return await request;
  } finally {
    inFlight.delete(url);
  }
}

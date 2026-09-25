import { NextResponse } from "next/server";

// Same-origin proxy to the API. This used to be a plain rewrite in
// next.config.mjs, which forwards headers untouched and injects nothing —
// so the browser had to hold the bearer token itself, which is why the key
// shipped in the client bundle as NEXT_PUBLIC_API_KEY. A route handler runs
// on the server, so the key can stay server-only and the browser sends no
// Authorization header at all.
//
// The directory is `%5Fproxy-api`, not `_proxy-api`: App Router treats a
// leading underscore as a private folder and drops it from routing. `%5F`
// is the documented escape that yields a literal `/_proxy-api` URL, which
// keeps robots.txt, the umami event paths and the existing client code
// working unchanged.
//
// Read-only by design: the internal routers this fronts are all GETs, so
// anything else is refused rather than forwarded.

export const dynamic = "force-dynamic";

// The routers the UI actually calls, derived from every `apiBase()` call site
// in utils/fetching.ts, the hooks and the components. Anything else is refused.
//
// Without this the proxy was a general-purpose gateway: it joined the caller's
// path onto the API base and attached our key, so `/_proxy-api/v1/stats`
// returned 200 to anyone while `api.foodatlas.ai/v1/stats` returned 401. That
// made the /v1 key ledger, its per-key attribution and its rate limit
// bypassable from any browser — the internal key skips the limiter outright
// (backend/api/src/rate_limit.py:47). Moving the key server-side fixed secrecy;
// this fixes authorization.
//
// `/v1` is deliberately absent: the UI never calls it, and it is the surface
// the ledger exists to meter.
const ALLOWED_ROUTERS = new Set([
  "food",
  "chemical",
  "disease",
  "bioactivity",
  "download",
]);

// `metadata` is allowlisted per-subpath rather than wholesale: the browser only
// needs the landing counters and the search autocomplete. `metadata/entities`
// is the ~780 KB unpaginated entity index, and it is read exclusively by
// app/sitemap.ts — which runs on the server, where apiBase() returns the API
// directly and never this proxy. Leaving it open would hand anyone the whole
// index for free.
const ALLOWED_METADATA = new Set(["statistics", "search"]);

const isAllowed = (path: string[]): boolean => {
  if (path.length === 0) return false;
  if (path[0] === "metadata") return ALLOWED_METADATA.has(path[1] ?? "");
  return ALLOWED_ROUTERS.has(path[0]);
};

// Browsers send Sec-Fetch-Site on every fetch; `cross-site` means another
// origin's page is driving this. Absent means a non-browser client (curl,
// a script) — allowed, because Googlebot's renderer and older browsers omit
// it and blocking those would break page rendering. The allowlist above is
// the real control; this only strips the cheapest cross-origin abuse.
const isCrossSite = (req: Request): boolean =>
  req.headers.get("sec-fetch-site") === "cross-site";

const upstream = async (
  req: Request,
  path: string[],
  method: "GET" | "HEAD",
) => {
  const base = process.env.NEXT_PUBLIC_API_URL;
  if (!base) {
    return NextResponse.json({ error: "api_not_configured" }, { status: 503 });
  }

  // Reject before the key is ever attached.
  if (!isAllowed(path)) {
    return NextResponse.json({ error: "not_proxied" }, { status: 404 });
  }
  if (isCrossSite(req)) {
    return NextResponse.json({ error: "cross_site" }, { status: 403 });
  }

  // `..` would otherwise survive: encodeURIComponent leaves `.` alone and
  // fetch() normalises the result, so a segment could climb above the base
  // path. Harmless while the base has no path, a prefix escape the moment it
  // gains one.
  if (path.some((s) => s === "." || s === "..")) {
    return NextResponse.json({ error: "bad_path" }, { status: 400 });
  }

  const suffix = path.map(encodeURIComponent).join("/");
  const { search } = new URL(req.url);
  const key = process.env.API_KEY;

  let res: Response;
  try {
    res = await fetch(`${base}/${suffix}${search}`, {
      method,
      // Only the key goes upstream. The client's own headers are dropped:
      // this endpoint is unauthenticated, and forwarding an attacker-supplied
      // Authorization would let them present a different key as if it were ours.
      headers: key ? { Authorization: `Bearer ${key}` } : {},
      cache: "no-store",
    });
  } catch {
    return NextResponse.json(
      { error: "upstream_unavailable" },
      { status: 502 },
    );
  }

  // Stream the body through untouched. Content-Type is the only header worth
  // copying — Content-Encoding would be a lie (fetch has already decoded) and
  // Content-Length is recomputed by the platform.
  const headers = new Headers();
  const contentType = res.headers.get("content-type");
  if (contentType) headers.set("content-type", contentType);
  headers.set("cache-control", "no-store");

  return new NextResponse(method === "HEAD" ? null : res.body, {
    status: res.status,
    headers,
  });
};

export const GET = (req: Request, { params }: { params: { path: string[] } }) =>
  upstream(req, params.path, "GET");

export const HEAD = (
  req: Request,
  { params }: { params: { path: string[] } },
) => upstream(req, params.path, "HEAD");

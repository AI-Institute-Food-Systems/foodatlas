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

const upstream = async (req: Request, path: string[], method: "GET" | "HEAD") => {
  const base = process.env.NEXT_PUBLIC_API_URL;
  if (!base) {
    return NextResponse.json({ error: "api_not_configured" }, { status: 503 });
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
    return NextResponse.json({ error: "upstream_unavailable" }, { status: 502 });
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

export const HEAD = (req: Request, { params }: { params: { path: string[] } }) =>
  upstream(req, params.path, "HEAD");

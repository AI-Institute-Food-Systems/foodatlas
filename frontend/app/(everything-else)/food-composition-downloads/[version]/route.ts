import { NextResponse } from "next/server";

import { DOWNLOADS_PATH } from "@/utils/site";
import { sendUmamiEvent } from "@/utils/umami";

// Downloads are gated like the API. The downloads page POSTs the user's
// API key here; we present it to `/v1/bundles/{version}/download`, which
// checks it (same ledger, rate limit and access log as every other /v1
// call) and answers with a 302 to a short-lived pre-signed S3 URL. That
// Location is what the browser is sent to, so the key rides in a POST
// body, never a URL, and the zip never sits behind a public link.
//
// The frontend holds no key of its own here — an unauthenticated visitor
// gets bounced back to the page with a reason, not a download.
export const dynamic = "force-dynamic";

const back = (req: Request, version: string, error: string) =>
  NextResponse.redirect(
    new URL(
      `${DOWNLOADS_PATH}?error=${error}&version=${encodeURIComponent(version)}`,
      req.url
    ),
    303
  );

const keyFrom = async (req: Request): Promise<string> => {
  const auth = req.headers.get("authorization") ?? "";
  if (auth.startsWith("Bearer ")) return auth.slice(7).trim();
  const form = await req.formData().catch(() => null);
  const key = form?.get("key");
  return typeof key === "string" ? key.trim() : "";
};

export async function POST(
  req: Request,
  { params }: { params: { version: string } }
) {
  const { version } = params;
  const key = await keyFrom(req);
  if (!key) return back(req, version, "missing_key");

  let upstream: Response;
  try {
    // Server-only handler, so talk to the API directly rather than through
    // apiBase(), whose browser branch would return the /_proxy-api path.
    upstream = await fetch(
      `${process.env.NEXT_PUBLIC_API_URL ?? ""}/v1/bundles/${encodeURIComponent(version)}/download`,
      {
        headers: { Authorization: `Bearer ${key}` },
        redirect: "manual",
        cache: "no-store",
      }
    );
  } catch {
    return back(req, version, "unavailable");
  }

  if (upstream.status === 401 || upstream.status === 403) {
    return back(req, version, "invalid_key");
  }
  if (upstream.status === 429) return back(req, version, "rate_limited");
  if (upstream.status === 404) {
    return NextResponse.json({ error: "unknown version" }, { status: 404 });
  }
  const location = upstream.headers.get("location");
  if (!location || upstream.status < 300 || upstream.status > 399) {
    return back(req, version, "unavailable");
  }

  // Awaited on purpose: Next 14.2 has no `after()`, and on Vercel anything
  // still in flight when the response is sent gets killed with the
  // function. sendUmamiEvent swallows errors and caps itself at 1.5 s.
  // Same identity rule as the API's own events: key prefix, never the key.
  await sendUmamiEvent(req, "bundle_download", {
    version,
    key_prefix: key.slice(0, 8),
  });

  return NextResponse.redirect(location, 303);
}

// A bare GET (old bookmarks, crawlers) has no key to offer — send it to
// the page, which explains how to get one.
export function GET(req: Request, { params }: { params: { version: string } }) {
  return back(req, params.version, "missing_key");
}

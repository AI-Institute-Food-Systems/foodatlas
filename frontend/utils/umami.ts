// Umami custom-event helpers. One module, two halves:
//
// - `track` is the browser side. The umami script (loaded in app/layout.tsx,
//   prod only) exposes `window.umami`; when it isn't there — dev, preview,
//   adblock — this is a no-op, so call sites never have to guard.
// - `sendUmamiEvent` is the server side, for things the browser never sees
//   (the bundle-download redirect). It POSTs straight to umami with the
//   caller's ip/UA so the event lands in the visitor's own session.
//
// Server-safe on purpose: no "use client", no React, so route handlers and
// server components can import it alongside client components.

export const UMAMI_WEBSITE_ID = "a63b88b0-aa17-4ca1-a3c6-62a568fe0757";
export const UMAMI_HOST = "https://umami.aifs.ucdavis.edu";
// Prod only — a preview deploy would otherwise pollute the one dashboard.
export const UMAMI_ENABLED = process.env.VERCEL_ENV === "production";

export type UmamiEventData = Record<string, string | number | boolean>;

// Client helper. In development the event is echoed to the console so a
// local click-through can confirm the wiring without a real umami.
export const track = (name: string, data?: UmamiEventData): void => {
  if (typeof window === "undefined") return;
  if (process.env.NODE_ENV === "development") {
    console.debug("[umami]", name, data);
  }
  window.umami?.track(name, data);
};

const firstHop = (value: string | null): string | undefined => {
  const hop = value?.split(",")[0]?.trim();
  return hop || undefined;
};

// Server helper. Never throws and never blocks past `timeoutMs`; a dead
// umami must not make a download slower than the timeout, and never fail it.
export const sendUmamiEvent = async (
  req: Request,
  name: string,
  data: UmamiEventData,
  { timeoutMs = 1500 }: { timeoutMs?: number } = {}
): Promise<void> => {
  if (!UMAMI_ENABLED) return;
  const url = new URL(req.url);
  const headers = req.headers;
  const payload = {
    website: UMAMI_WEBSITE_ID,
    hostname: headers.get("x-forwarded-host") ?? headers.get("host") ?? url.host,
    url: url.pathname,
    name,
    data,
    ip: firstHop(headers.get("x-forwarded-for")) ?? headers.get("x-real-ip") ?? undefined,
    userAgent: headers.get("user-agent") ?? undefined,
    referrer: headers.get("referer") ?? undefined,
  };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    await fetch(`${UMAMI_HOST}/api/send`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // umami rejects requests without a UA; forward the caller's so the
        // event joins their session rather than a synthetic one.
        "User-Agent": payload.userAgent ?? "foodatlas-frontend",
      },
      body: JSON.stringify({ type: "event", payload }),
      signal: controller.signal,
      cache: "no-store",
    });
  } catch {
    // Analytics never gets to break the request it rides on.
  } finally {
    clearTimeout(timer);
  }
};

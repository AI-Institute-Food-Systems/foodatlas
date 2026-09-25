import { NextResponse } from "next/server";

// Shared guards for the two unauthenticated POST routes that reach SES
// (/contact/send and /report/issue/send).
//
// Both were presence-validated only: `if (!name || !email || !message)`. The
// length caps lived exclusively in the React components, so a direct POST
// could put megabytes of arbitrary text into the team inbox, and `email` went
// into ReplyToAddresses unvalidated. Neither route is an open relay — the
// recipients come from env and SESv2 `Simple` content puts subject and body in
// API fields rather than raw headers, so there is no header injection — but
// "not an open relay" is not the same as "safe to leave unbounded".
//
// Rate limiting is deliberately NOT here: a per-instance counter on serverless
// is close to useless, and the honest place for it is the same Vercel WAF rule
// that already covers /_proxy-api/*. Tracked separately.

// Mirrors the maxLength attributes on the client inputs, so a normal
// submission can never trip these — they only bound a direct POST.
export const LIMITS = {
  name: 40,
  email: 80,
  affiliation: 80,
  message: 2000,
  description: 4000,
  pageUrl: 500,
} as const;

// Deliberately loose. This is a sanity bound to keep malformed values out of
// ReplyToAddresses (where SES rejects them and the route 500s), not an
// attempt to decide what a valid address is — that argument has no winner.
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export const isEmail = (v: string): boolean =>
  v.length <= LIMITS.email && EMAIL_RE.test(v);

/** A string field that is present, a string, and within its cap. */
export const str = (
  value: unknown,
  max: number,
  { required = true }: { required?: boolean } = {},
): string | null => {
  if (value === undefined || value === null || value === "") {
    return required ? null : "";
  }
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (required && trimmed === "") return null;
  if (trimmed.length > max) return null;
  return trimmed;
};

/**
 * Reject a submission driven from another origin.
 *
 * Browsers send Sec-Fetch-Site on every fetch; `cross-site` means another
 * site's page is posting here. A missing header is allowed — non-browser
 * clients omit it, and these forms have legitimate non-browser callers during
 * testing. This raises the cost of drive-by abuse; it is not a CSRF boundary,
 * and it does not pretend to be one.
 */
export const isCrossSite = (req: Request): boolean =>
  req.headers.get("sec-fetch-site") === "cross-site";

/**
 * Parse the CONTACT_EMAIL fan-out list.
 *
 * Was `JSON.parse(process.env.CONTACT_EMAIL!)` — a non-null assertion on an
 * env var, so an unset or malformed value threw and surfaced as a 500 with a
 * stack, rather than a clear server-side error.
 */
export const recipients = (): string[] | null => {
  const raw = process.env.CONTACT_EMAIL;
  if (!raw) return null;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length === 0) return null;
    if (!parsed.every((r) => typeof r === "string" && r.includes("@"))) {
      return null;
    }
    return parsed as string[];
  } catch {
    return null;
  }
};

export const badRequest = (error: string): NextResponse =>
  NextResponse.json({ error }, { status: 400 });

export const misconfigured = (): NextResponse =>
  NextResponse.json({ error: "contact_not_configured" }, { status: 503 });

export const crossSite = (): NextResponse =>
  NextResponse.json({ error: "cross_site" }, { status: 403 });

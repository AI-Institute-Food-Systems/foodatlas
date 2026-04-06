import { NextRequest, NextResponse } from "next/server";

/**
 * Detect entity ID slugs (e.g. /food/e2908) and redirect to the
 * name-based URL (e.g. /food/cow--milk) by resolving via the API.
 *
 * Entity IDs match the pattern: "e" followed by one or more digits.
 */

const ENTITY_ID_RE = /^e\d+$/;
const ENTITY_ROUTES = new Set(["food", "chemical", "disease"]);

function encodeSpace(phrase: string) {
  return phrase.replace(/ /g, "--");
}

export async function middleware(request: NextRequest) {
  const segments = request.nextUrl.pathname.split("/").filter(Boolean);

  // Only intercept /{entityType}/{slug} routes
  if (segments.length !== 2) return NextResponse.next();

  const [entityType, slug] = segments;
  if (!ENTITY_ROUTES.has(entityType)) return NextResponse.next();
  if (!ENTITY_ID_RE.test(slug)) return NextResponse.next();

  // Resolve entity ID → common_name via API
  const apiUrl = process.env.NEXT_PUBLIC_API_URL;
  const apiKey = process.env.NEXT_PUBLIC_API_KEY;
  if (!apiUrl) return NextResponse.next();

  try {
    const res = await fetch(
      `${apiUrl}/resolve?entity_id=${encodeURIComponent(slug)}`,
      {
        headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : {},
      }
    );

    if (!res.ok) return NextResponse.next();

    const { entity_type, common_name } = await res.json();
    const encodedName = encodeURIComponent(encodeSpace(common_name));
    const redirectUrl = new URL(
      `/${entity_type}/${encodedName}`,
      request.url
    );

    return NextResponse.redirect(redirectUrl);
  } catch {
    return NextResponse.next();
  }
}

export const config = {
  matcher: ["/(food|chemical|disease)/:slug*"],
};

// @ts-check

/** @type {import('next').NextConfig} */
// Everything the browser loads is same-origin except Google Analytics, which
// @next/third-parties pulls from googletagmanager and beacons to
// google-analytics. umami and Vercel Insights are proxied through first-party
// paths (/_a/*, /_vercel/*), so they need no CSP entry of their own.
//
// script-src keeps 'unsafe-inline' because App Router emits ~31 inline
// hydration scripts per page. The alternative is a per-request nonce from
// middleware, which Next can only do by opting every page into dynamic
// rendering — too high a price here, and middleware.ts currently matches only
// the four entity routes. Tracked as a follow-up; the policy below still
// constrains *where* scripts come from, and frame-ancestors / base-uri /
// object-src / form-action are unaffected by it.
//
// form-action allows S3 because the gated bundle download is a same-origin
// form POST (components/misc/DownloadsTable.tsx:57) that 3xx-redirects to a
// presigned URL — Chrome checks form-action against redirect targets, so
// 'self' alone would break it.
const csp = [
  "default-src 'self'",
  "base-uri 'self'",
  "object-src 'none'",
  "frame-ancestors 'none'",
  "form-action 'self' https://*.amazonaws.com",
  "script-src 'self' 'unsafe-inline' https://www.googletagmanager.com",
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data: blob: https://www.googletagmanager.com https://*.google-analytics.com",
  "font-src 'self' data:",
  "connect-src 'self' https://*.google-analytics.com https://*.analytics.google.com https://*.googletagmanager.com",
  "manifest-src 'self'",
  "upgrade-insecure-requests",
].join("; ");

// No `preload` on HSTS: that token is a request to be baked into browser
// preload lists, which is slow to undo. includeSubDomains is safe today —
// api. and dev. are both HTTPS-only.
const securityHeaders = [
  { key: "Content-Security-Policy", value: csp },
  {
    key: "Strict-Transport-Security",
    value: "max-age=63072000; includeSubDomains",
  },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  {
    key: "Permissions-Policy",
    value: "camera=(), microphone=(), geolocation=(), payment=()",
  },
];

const nextConfig = {
  reactStrictMode: false,
  headers: async () => [{ source: "/:path*", headers: securityHeaders }],
  rewrites: async () => {
    const rewrites = [
      // Proxy umami through a first-party path so adblock pattern rules
      // (||umami.*, /script.js, /api/send) don't match.
      {
        source: "/_a/script.js",
        destination: "https://umami.aifs.ucdavis.edu/script.js",
      },
      {
        source: "/_a/api/send",
        destination: "https://umami.aifs.ucdavis.edu/api/send",
      },
    ];
    // /_proxy-api is no longer a rewrite: a rewrite forwards headers as-is
    // and cannot attach the API key, which forced the key into the client
    // bundle. It is now a route handler that injects the key server-side —
    // see app/%5Fproxy-api/[...path]/route.ts.
    return rewrites;
  },
  redirects: async () => [
    // old urls
    {
      source: "/background",
      destination: "/technical-background",
      permanent: true,
    },
    {
      source: "/summary",
      destination: "/technical-background",
      permanent: true,
    },
    {
      source: "/data",
      destination: "/food-composition-table",
      permanent: true,
    },
    {
      source: "/api_documentation",
      destination: "/",
      permanent: false,
    },
    {
      source: "/downlodas",
      destination: "/food-composition-downloads",
      permanent: true,
    },
    // temp for api under construction
    {
      source: "/api",
      destination: "/",
      permanent: false,
    },
    {
      source: "/food-composition-api",
      destination: "/",
      permanent: false,
    },
  ],
};

export default nextConfig;

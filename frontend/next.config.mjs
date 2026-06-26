// @ts-check

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false,
  rewrites: async () => [
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
    // Same-origin proxy to the API. Lets the browser call a HTTPS path on
    // dev.foodatlas.ai even when the upstream ALB only serves HTTP (staging
    // has no TLS cert yet). Resolved at build time — Vercel must have
    // NEXT_PUBLIC_API_URL set when `next build` runs.
    {
      source: "/_proxy-api/:path*",
      destination: `${process.env.NEXT_PUBLIC_API_URL}/:path*`,
    },
  ],
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

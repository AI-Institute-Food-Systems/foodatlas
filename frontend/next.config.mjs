// @ts-check

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false,
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

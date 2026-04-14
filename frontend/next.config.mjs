// @ts-check

/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: false,
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

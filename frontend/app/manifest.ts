import type { MetadataRoute } from "next";

import { SITE_NAME } from "@/utils/site";

// Served at /manifest.webmanifest, and linked from every page by Next. The
// icons are app/icon.svg rendered onto its own black ground, so they are
// opaque at every size; public/icon-512.png is also the Organization logo.
const manifest = (): MetadataRoute.Manifest => ({
  name: `${SITE_NAME}: Evidence-Based Food Composition Database`,
  short_name: SITE_NAME,
  description:
    "Evidence-based knowledge graph of foods, chemicals, diseases and bioactivities, every association traced to its source.",
  start_url: "/",
  display: "browser",
  background_color: "#0D0C0C",
  theme_color: "#0D0C0C",
  icons: [
    { src: "/icon-192.png", sizes: "192x192", type: "image/png" },
    { src: "/icon-512.png", sizes: "512x512", type: "image/png" },
    { src: "/icon.svg", sizes: "any", type: "image/svg+xml" },
  ],
});

export default manifest;

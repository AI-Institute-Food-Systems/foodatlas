// Google Analytics, gated like umami (utils/umami.ts): production only, so a
// preview or local build never reports into the one property. Without an id
// it rendered anyway and loaded gtag/js?id= with an empty id.
export const GA_MEASUREMENT_ID = process.env.NEXT_PUBLIC_GA_MEASUREMENT_ID ?? "";
export const GA_ENABLED =
  process.env.VERCEL_ENV === "production" && GA_MEASUREMENT_ID !== "";

import { Metadata } from "next";

import HeroSection from "@/components/landing/HeroSection";
import JsonLd from "@/components/misc/JsonLd";
import { organizationJsonLd, webSiteJsonLd } from "@/utils/structuredData";
import { HOME_TITLE, buildMetadata } from "@/utils/site";

export const metadata: Metadata = buildMetadata({
  title: HOME_TITLE,
  absoluteTitle: true,
  description:
    "Access extensive food composition data sourced by AI from peer-reviewed research. Apply reliable data to your research using the API or downloadable data sets.",
  path: "/",
});

const Landing = () => (
  <>
    <JsonLd data={webSiteJsonLd()} />
    <JsonLd data={organizationJsonLd()} />
    <HeroSection />
  </>
);

export default Landing;

Landing.displayName = "Landing";

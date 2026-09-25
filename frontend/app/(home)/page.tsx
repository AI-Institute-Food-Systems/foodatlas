import { Metadata } from "next";

import HeroSection from "@/components/landing/HeroSection";
import JsonLd from "@/components/misc/JsonLd";
import { webSiteJsonLd } from "@/utils/structuredData";

export const metadata: Metadata = {
  alternates: { canonical: "/" },
  title: "FoodAtlas | Evidence-Based Food Composition Database",
  description:
    "Access extensive food composition data sourced by AI from peer-reviewed research. Apply reliable data to your research using the API or downloadable data sets.",
};

const Landing = () => (
  <>
    <JsonLd data={webSiteJsonLd()} />
    <HeroSection />
  </>
);

export default Landing;

Landing.displayName = "Landing";

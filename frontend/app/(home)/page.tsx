import { Metadata } from "next";

import HeroSection from "@/components/landing/HeroSection";

export const metadata: Metadata = {
  title: "FoodAtlas | Evidence-Based Food Composition Database",
  description:
    "Access extensive food composition data sourced by AI from peer-reviewed research. Apply reliable data to your research using the API or downloadable data sets.",
};

const Landing = () => <HeroSection />;

export default Landing;

Landing.displayName = "Landing";

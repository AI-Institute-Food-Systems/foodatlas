import { Metadata } from "next";

import HeroSection from "@/components/landing/HeroSection";
import NumbersSection from "@/components/landing/NumbersSection";
import DemoSection from "@/components/landing/DemoSection";

export const metadata: Metadata = {
  title: "FoodAtlas | Evidence-Based Food Composition Database",
  description:
    "Access extensive food composition data sourced by AI from peer-reviewed research. Apply reliable data to your research using the API or downloadable data sets.",
};

const Landing = () => {
  return (
    <>
      <HeroSection />
      <NumbersSection />
      {/* <DemoSection /> */}
    </>
  );
};

export default Landing;

Landing.displayName = "Landing";

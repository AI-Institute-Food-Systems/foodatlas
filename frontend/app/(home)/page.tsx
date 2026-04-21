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
      <div className="px-3 md:px-12 pt-4">
        <div className="max-w-6xl mx-auto">
          <div className="border-l-4 border-accent-500 bg-light-900/60 rounded-r-md px-5 py-4">
            <div className="text-accent-500 text-xs font-semibold uppercase tracking-wider">
              News
            </div>
            <div className="mt-1 text-light-200 text-base md:text-lg">
              <span className="text-light-400">4/20/2026 — </span>
              FoodAtlas Knowledge Graph <strong>v4.0</strong> has been
              released with improved data quality and expanded coverage!
            </div>
          </div>
        </div>
      </div>
      <HeroSection />
      <NumbersSection />
      {/* <DemoSection /> */}
    </>
  );
};

export default Landing;

Landing.displayName = "Landing";

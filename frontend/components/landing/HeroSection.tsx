import Image from "next/image";
import { FaMicroscope } from "react-icons/fa6";

import Badge from "@/components/basic/Badge";
import AIIcon from "@/components/icons/AIIcon";
import SearchWrapper from "@/components/landing/SearchWrapper";
import Heading from "@/components/basic/Heading";

const HeroSection = () => {
  return (
    <div className="h-[47rem] relative">
      {/* background image */}
      <Image
        className="object-cover h-full -z-10 max-w-[130rem] mx-auto blur-sm"
        fill
        alt="Background wallpaper of a graph resembling a neural network"
        src="/images/hero_wallpaper_color.webp"
        priority
        quality={100}
      />
      {/* container */}
      <div className="flex flex-col relative">
        {/* heading container */}
        <div className="px-3 md:px-12">
          <div className="max-w-6xl mt-[3.3rem] md:mt-20 lg:mt-16 flex flex-col items-center mx-auto gap-6">
            {/* badge container */}
            <div className="flex gap-2 md:gap-3 lg:gap-4">
              <Badge
                leftIcon={
                  <AIIcon height={"1em"} width={"1em"} color="#FF5722" />
                }
              >
                AI-Powered
              </Badge>
              <Badge
                leftIcon={
                  <FaMicroscope height={"1em"} width={"1em"} color="#FF5722" />
                }
              >
                Research-Based
              </Badge>
            </div>
            <Heading
              type="h1"
              className="text-[2.3rem] leading-[2.4rem] md:leading-[3rem] md:text-[2.9rem] lg:text-6xl text-center text-shadow-lg text-light-50 max-w-sm md:max-w-none font-semibold"
            >
              Explore the links between <br /> foods, chemicals & diseases
            </Heading>
            {/* separator */}
            <div className="w-32 md:w-48 h-[0.1rem] md:h-1 rounded-full  bg-gradient-to-r from-accent-400/50 via-accent-600/80 to-accent-400/50 border border-accent-500" />
            {/* secondary heading */}
            <h2 className="max-w-xl lg:max-w-none text-lg md:text-2xl text-light-300 text-center">
              Introducing <i>FoodAtlas</i>, the world&apos;s first
              evidence-based food knowledge base.
            </h2>
          </div>
        </div>
        <SearchWrapper />
      </div>
    </div>
  );
};

HeroSection.displayName = "HeroSection";

export default HeroSection;

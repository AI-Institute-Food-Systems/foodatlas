import { FaMicroscope } from "react-icons/fa6";

import Badge from "@/components/basic/Badge";
import AIIcon from "@/components/icons/AIIcon";
import HeroAnimation from "@/components/landing/HeroAnimation";

const HeroSectionAnimated = () => {
  return (
    <>
      {/* background image */}
      {/* <img src="/icons/graph.svg" /> */}
      <HeroAnimation />
      {/* container */}
      <div className="h-[47rem] max-w-7xl mx-auto px-3 md:px-12 flex flex-col relative">
        {/* heading container */}
        <div className="mt-16 flex flex-col items-center mx-auto gap-6">
          {/* badge container */}
          <div className="flex gap-2 md:gap-3 lg:gap-4">
            <Badge
              leftIcon={<AIIcon height={"1em"} width={"1em"} color="#F4511E" />}
            >
              AI-Powered
            </Badge>
            <Badge leftIcon={<FaMicroscope />}>Science-Based</Badge>
          </div>
          {/* main heading */}
          <h1 className="text-4xl md:text-6xl lg:text-6xl text-center font-bold text-shadow-lg text-light-50">
            Explore the links between <br /> foods, chemicals & diseases
          </h1>
          {/* separator */}
          <hr className="w-32 border-accent-600 border-[1px]" />
          {/* secondary heading */}
          <h2 className="text-md md:text-xl lg:text-2xl text-light-300 text-shadow-lg text-center">
            Introducing FoodAtlas, the world&apos;s first evidence-based food
            knowledge base.
          </h2>
        </div>
        {/* search */}
        <div className="mt-10 mx-auto w-full">
          {/* <HeroSearchWrapper /> */}
        </div>
      </div>
    </>
  );
};

HeroSectionAnimated.displayName = "HeroSectionAnimated";

export default HeroSectionAnimated;

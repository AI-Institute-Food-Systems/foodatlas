import Image from "next/image";
import { FaMicroscope } from "react-icons/fa6";

import Badge from "@/components/basic/Badge";
import AIIcon from "@/components/icons/AIIcon";
import Heading from "@/components/basic/Heading";
import HeroStatsLine from "@/components/landing/HeroStatsLine";
import SearchWrapper from "@/components/landing/SearchWrapper";

// Hero is a single centred flex column. Each child sits in normal
// flow with one consistent gap so the entire stack scales together
// when content changes. The portaled <SearchBar/> overlays the
// SearchWrapper spacer via a measured offset, so there are no magic
// pixel positions anywhere in this file.
const HeroSection = () => {
  return (
    <section className="relative">
      <Image
        className="object-cover -z-10 max-w-[130rem] mx-auto blur-sm"
        fill
        alt="Background wallpaper of a graph resembling a neural network"
        src="/images/hero_wallpaper_color.webp"
        priority
        quality={100}
      />
      <div className="relative min-h-[44rem] md:min-h-[48rem] flex flex-col justify-center px-3 md:px-12 py-14 md:py-20">
        <div className="max-w-4xl w-full mx-auto flex flex-col items-center gap-6 md:gap-8">
          {/* News line — typeset (no chrome), accent eyebrow + serif
           * italic body. Sits closest to the top edge. */}
          <div className="flex items-center gap-2 md:gap-3 leading-none">
            <span className="font-mono italic text-[10px] md:text-xs uppercase tracking-[0.22em] text-accent-500">
              News
            </span>
            <span className="block h-3 w-px bg-accent-500/40" aria-hidden />
            <span className="font-serif italic text-xs md:text-sm text-light-200">
              FoodAtlas Knowledge Graph{" "}
              <strong className="not-italic font-semibold text-white">
                v4.1
              </strong>{" "}
              released
            </span>
          </div>

          {/* Credentials */}
          <div className="flex gap-2 md:gap-3">
            <Badge
              size="sm"
              leftIcon={<AIIcon height="1em" width="1em" color="#FF5722" />}
            >
              AI-Powered
            </Badge>
            <Badge
              size="sm"
              leftIcon={
                <FaMicroscope height={"1em"} width={"1em"} color="#FF5722" />
              }
            >
              Peer-Reviewed
            </Badge>
          </div>

          {/* Headline */}
          <Heading
            type="h1"
            className="text-[2rem] leading-[2.2rem] md:leading-[2.8rem] md:text-[2.5rem] lg:text-5xl text-center text-shadow-lg text-light-50 font-semibold"
          >
            The evidence-based <br className="hidden md:block" /> food knowledge
            graph.
          </Heading>

          <div className="w-32 md:w-48 h-[0.1rem] md:h-1 rounded-full bg-gradient-to-r from-accent-400/50 via-accent-600/80 to-accent-400/50 border border-accent-500" />

          {/* Subhead */}
          <p className="max-w-xl lg:max-w-3xl text-lg md:text-2xl text-light-300 text-center">
            Search bioactivities, foods, chemicals &amp; diseases &mdash; every
            link traced to peer-reviewed sources.
          </p>

          {/* Search spacer — reserves vertical space matching the
           * portaled SearchBar + TryChips and reports its position
           * back through SearchContext so the overlay aligns. */}
          <SearchWrapper />

          {/* Stats — hidden on phones; the row would either scroll
           * horizontally or wrap into 3+ lines, neither of which feels
           * good on a small viewport. */}
          <div className="hidden md:block">
            <HeroStatsLine />
          </div>
        </div>
      </div>
    </section>
  );
};

HeroSection.displayName = "HeroSection";

export default HeroSection;

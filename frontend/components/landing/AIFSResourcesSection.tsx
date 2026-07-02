import { CSSProperties } from "react";

interface Resource {
  name: string;
  href: string;
  label: string;
  description: string;
  accent: string;
  isCurrent?: boolean;
}

// Sister AIFS databases. URLs are placeholders until the real ones land —
// search this file for "TODO: url" to wire them up.
const RESOURCES: Resource[] = [
  {
    name: "FoodAtlas",
    href: "/",
    label: "Food · Chemicals · Disease",
    description:
      "The world's first evidence-based food knowledge base. Explore connections between foods, their chemical components, and associated diseases using AI-powered analysis of peer-reviewed research.",
    accent: "#F4511E",
    isCurrent: true,
  },
  {
    name: "Byproduct Database",
    // TODO: url
    href: "#",
    label: "Crops · Waste · Reuse",
    description:
      "Maps and quantifies agricultural and food processing byproducts from California crops. Identifies waste streams and their potential for reuse, supporting circular economy principles.",
    accent: "#10b981",
  },
  {
    name: "Preclinical Database",
    // TODO: url
    href: "#",
    label: "Drugs · Diseases · Animals",
    description:
      "AI-powered repository of in vivo preclinical animal studies extracted from PubMed Central. Standardizes disease, drug, and animal entities to accelerate the translation of preclinical research into clinical trials.",
    accent: "#a855f7",
  },
];

const AIFSResourcesSection = () => {
  return (
    <section className="bg-light-1000 w-full">
      <div className="px-3 md:px-12">
        <div className="max-w-5xl mx-auto py-20 md:py-24">
          {/* masthead */}
          <div className="flex flex-col gap-3 mb-12 md:mb-16 max-w-2xl">
            <span className="font-mono italic uppercase text-light-500 text-[11px] tracking-[0.22em]">
              Sister Collections
            </span>
            <h2 className="font-serif text-3xl md:text-5xl text-light-100 leading-tight">
              More AIFS Resources
            </h2>
            <p className="font-serif italic text-light-400 text-base md:text-lg">
              Explore other databases and tools built by AIFS.
            </p>
          </div>

          {/* three drawers */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-x-6 gap-y-10 md:gap-y-6">
            {RESOURCES.map((r) => {
              const Wrapper = r.isCurrent ? "div" : "a";
              const wrapperProps = r.isCurrent
                ? {}
                : { href: r.href, rel: "noopener" as const };
              return (
                <Wrapper
                  key={r.name}
                  {...wrapperProps}
                  className={
                    r.isCurrent
                      ? "group block"
                      : "group block focus:outline-none focus-visible:outline-light-200"
                  }
                >
                  <article
                    style={{ "--accent": r.accent } as CSSProperties}
                    className="relative h-full flex flex-col bg-light-950 border-[1.5px] border-light-50/[0.08] rounded-xl overflow-visible shadow-[inset_0_5px_8px_rgba(255,249,242,0.02)] transition-all duration-300 group-hover:-translate-y-1 group-hover:border-light-50/[0.18] group-hover:shadow-[0_12px_32px_-12px_rgba(0,0,0,0.6),inset_0_5px_8px_rgba(255,249,242,0.04)]"
                  >
                    {/* top accent stripe (the jar's color band) */}
                    <span
                      aria-hidden
                      className="absolute inset-x-0 top-0 h-[3px] rounded-t-xl"
                      style={{
                        backgroundColor: "var(--accent)",
                        opacity: 0.85,
                      }}
                    />

                    {/* card-catalog chip protruding from left edge */}
                    <span className="self-start mt-7 -ml-2 px-2.5 py-0.5 bg-light-200 shadow-inner shadow-light-50 rounded-r-md font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase">
                      {r.label}
                    </span>

                    <div className="px-6 pb-7 pt-4 flex flex-col flex-1">
                      <h3 className="font-serif text-2xl text-light-100">
                        {r.name}
                      </h3>

                      {/* double-rule separator */}
                      <div className="mt-3 mb-4 border-t-2 border-double border-light-700/50" />

                      <p className="text-sm text-light-300 leading-relaxed flex-1">
                        {r.description}
                      </p>

                      <span
                        className="mt-6 inline-flex items-center gap-1.5 font-mono italic text-sm"
                        style={{ color: r.isCurrent ? "#dad3cb" : "var(--accent)" }}
                      >
                        {r.isCurrent ? (
                          <>
                            <span
                              aria-hidden
                              className="inline-block w-1.5 h-1.5 rounded-full"
                              style={{ backgroundColor: "var(--accent)" }}
                            />
                            You are reading
                          </>
                        ) : (
                          <>
                            Visit {r.name}
                            <span className="transition-transform duration-200 group-hover:translate-x-0.5">
                              →
                            </span>
                          </>
                        )}
                      </span>
                    </div>
                  </article>
                </Wrapper>
              );
            })}
          </div>
        </div>
      </div>
    </section>
  );
};

AIFSResourcesSection.displayName = "AIFSResourcesSection";

export default AIFSResourcesSection;

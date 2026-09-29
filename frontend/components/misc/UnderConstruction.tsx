import { ReactNode } from "react";

import Heading from "@/components/basic/Heading";

interface UnderConstructionProps {
  children: ReactNode;
  message?: string;
}

// Blurs a whole page behind a big "Under construction" label. The page
// stays in the DOM — crawlers still index the copy — but `inert` takes it
// out of the tab order and the accessibility tree, so nothing behind the
// blur can be clicked, focused or read out. React 18.3 doesn't know
// `inert` and drops a boolean `true`, so it has to go through as the
// empty string (and past the experimental `inert?: boolean` typing).
const INERT: Record<string, string> = { inert: "" };

const UnderConstruction = ({ children, message }: UnderConstructionProps) => {
  return (
    <div className="relative">
      <div
        data-testid="under-construction-content"
        className="blur-md pointer-events-none select-none"
        {...INERT}
      >
        {children}
      </div>
      {/* Sticky label so it stays in view on these long pages. */}
      <div className="absolute inset-0 z-10">
        <div className="sticky top-1/3 flex justify-center px-2">
          <div
            role="status"
            className="max-w-xl rounded-2xl border-[1.5px] border-light-50/[0.08] bg-light-1000/70 px-8 py-10 text-center shadow-2xl shadow-black/40 backdrop-blur-sm"
          >
            <Heading type="h2" variant="display">
              Under construction
            </Heading>
            {message && (
              <p className="mt-4 text-base leading-relaxed text-light-200">
                {message}
              </p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default UnderConstruction;

import { ReactNode } from "react";

interface SubHeadingProps {
  children: string | ReactNode;
}

const SubHeading = ({ children }: SubHeadingProps) => {
  // Editorial subtitle — serif italic mirrors the apothecary-card mood the
  // entity pages established. text-base (not text-lg) so it sits as a
  // restrained kicker under the H1 without pulling visual weight away
  // from the page body.
  return (
    <p className="mt-1 max-w-3xl text-base italic font-serif leading-relaxed text-light-400">
      {children}
    </p>
  );
};

SubHeading.displayName = "SubHeading";

export default SubHeading;

import { ReactNode } from "react";

interface SubHeadingProps {
  children: string | ReactNode;
}

const SubHeading = ({ children }: SubHeadingProps) => {
  return (
    <p className="mt-0.5 max-w-3xl text-lg leading-relaxed text-light-400">
      {children}
    </p>
  );
};

SubHeading.displayName = "SubHeading";

export default SubHeading;

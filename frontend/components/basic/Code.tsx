import { ReactNode } from "react";

interface CodeProps {
  className?: string;
  children: ReactNode;
  size?: string;
}

const Code = ({ children, className, size = "text-[0.9rem]" }: CodeProps) => {
  return (
    <>
      {" "}
      <code
        className={`h-fit w-fit bg-light-700 text-light-50 px-2 py-1 rounded ${size} ${className}`}
      >
        {children}
      </code>{" "}
    </>
  );
};

Code.displayName = "Code";

export default Code;

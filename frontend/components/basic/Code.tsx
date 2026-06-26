import { ReactNode } from "react";

interface CodeProps {
  className?: string;
  children: ReactNode;
  size?: string;
}

const Code = ({ children, className, size = "text-[0.85rem]" }: CodeProps) => {
  // Inline code — tighter padding + darker fill so it reads as part of
  // the editorial body text, not a label chip. border-light-700/40 keeps
  // the edge visible against the dark card backgrounds without shouting.
  return (
    <>
      {" "}
      <code
        className={`h-fit w-fit bg-light-900 text-light-100 px-1.5 py-0.5 rounded border border-light-700/40 font-mono ${size} ${className}`}
      >
        {children}
      </code>{" "}
    </>
  );
};

Code.displayName = "Code";

export default Code;

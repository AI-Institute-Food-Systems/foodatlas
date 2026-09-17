import { ReactNode } from "react";

interface CodeProps {
  className?: string;
  children: ReactNode;
  size?: string;
}

const Code = ({ children, className, size = "text-[0.85rem]" }: CodeProps) => {
  // Inline code — vivid FA orange (accent-500) text on a barely-there
  // warm halo. The orange whispers "brand" without swallowing the
  // paragraph; multiple tokens in one paragraph read as consistent
  // editorial callouts rather than repeated shouts. font-medium bumps
  // the mono letterforms so they hold their weight against the tint.
  return (
    <>
      {" "}
      <code
        className={`h-fit w-fit inline-block align-baseline
          font-mono font-medium ${size}
          text-accent-300
          bg-accent-500/[0.04]
          border border-accent-500/15
          px-1.5 py-0.5 rounded ${className ?? ""}`}
      >
        {children}
      </code>{" "}
    </>
  );
};

Code.displayName = "Code";

export default Code;

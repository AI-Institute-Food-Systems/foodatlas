import { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

const styles = {
  normal: "text-4xl font-medium font-serif",
  boxed:
    "w-fit h-fit bg-light-200 shadow-inner shadow-light-50 rounded-md px-2.5 py-0.5 text text-light-900 font-mono italic font-medium",
};

interface HeadingProps {
  className?: string;
  children: string | ReactNode;
  variant?: "normal" | "boxed";
  type: "h1" | "h2" | "h3" | "h4";
}

const Heading = ({
  children,
  variant = "normal",
  type,
  className,
}: HeadingProps) => {
  return {
    h1: <h1 className={twMerge(styles[variant], className)}>{children}</h1>,
    h2: <h2 className={twMerge(styles[variant], className)}>{children}</h2>,
    h3: <h3 className={twMerge(styles[variant], className)}>{children}</h3>,
    h4: <h4 className={twMerge(styles[variant], className)}>{children}</h4>,
  }[type];
};

Heading.displayName = "Heading";

export default Heading;

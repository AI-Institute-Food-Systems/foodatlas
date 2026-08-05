import { ReactNode } from "react";
import { twMerge } from "tailwind-merge";

const styles = {
  normal: "text-3xl font-medium font-serif",
  boxed:
    "w-fit h-fit bg-light-200 shadow-inner shadow-light-50 rounded-md px-2.5 py-0.5 text text-light-900 font-mono italic font-medium",
  // Apothecary "cream chip on the edge" label — matches Taxonomy and
  // OverviewCardCatalog section labels. The -ml-3 makes the chip
  // hang past the container's left edge like a filing-cabinet tab.
  chip: "-ml-3 inline-block bg-light-200 shadow-inner shadow-light-50 rounded-r-md px-2.5 py-0.5 font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase",
  // Bold display heading — matches the hero H1's font/weight so page
  // titles across the site read as one family.
  display:
    "font-serif font-semibold text-light-50 text-[2rem] leading-tight md:text-[2.5rem] lg:text-5xl",
};

interface HeadingProps {
  className?: string;
  children: string | ReactNode;
  variant?: "normal" | "boxed" | "chip" | "display";
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

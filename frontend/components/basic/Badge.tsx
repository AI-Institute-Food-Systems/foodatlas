import { twMerge } from "tailwind-merge";

interface BadgeProps {
  color?: string;
  leftIcon?: React.ReactNode;
  children: React.ReactNode;
  size?: keyof typeof sizes;
  square?: boolean;
}

const sizes = {
  xs: "px-1.5 py-[0.1rem] text-[0.5rem] md:text-[0.6rem] border-[1px] gap-1 shadow-[inset_0_0.5px_2px_rgba(0,0,0,0.6)]",
  sm: "px-2.5 py-[0.2rem] text-[0.65rem] md:text-[0.7rem] border-[1px] gap-1 shadow-[inset_0_0.5px_2px_rgba(0,0,0,0.6)]",
  md: "px-2.5 py-1 md:px-3.5 md:py-1 text-[0.72rem] md:text-[0.85rem] border-[1.5px] gap-1.5 shadow-[inset_0_1px_6px_rgba(0,0,0,0.5)]",
  lg: "px-3 py-0.5 md:py-1 text-[0.95rem] border-[1px] md:border-[2px] gap-1.5 md:gap-2 shadow-[inset_0_2px_8px_rgba(0,0,0,0.4)] md:shadow-inset_0_2px_8px_rgba(0,0,0,0.6)]",
};

const Badge = ({ color, leftIcon, children, size = "md" }: BadgeProps) => {
  const badgeColor =
    color ??
    "text-accent-500 border-accent-600 bg-accent-600/10 shadow-accent-800";
  const badgeSize = sizes[size];

  return (
    <div
      className={twMerge(
        `rounded-full w-fit h-fit font-mono font-medium md:font-semibold flex whitespace-nowrap items-center capitalize`,
        badgeColor,
        badgeSize
      )}
    >
      {leftIcon && leftIcon}
      {children}
    </div>
  );
};

Badge.displayName = "Badge";

export default Badge;

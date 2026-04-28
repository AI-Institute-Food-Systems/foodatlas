"use client";

import {
  ButtonHTMLAttributes,
  forwardRef,
  useState,
  useEffect,
  useRef,
} from "react";
import { useRouter } from "next/navigation";
import { twMerge } from "tailwind-merge";

enum Variants {
  filled = "filled",
  outlined = "outlined",
  ghost = "ghost",
  base = "base",
}

const variants = {
  filled:
    "bg-accent-900 text-light-50 shadow-inner shadow-accent-500/20 hover:bg-accent-800 hover:shadow-accent-700/30",
  outlined:
    "border text-light-300 border-light-300 hover:border-light-200 hover:text-light-200",
  ghost: "px-0 !shadow-none",
  base: "px-0",
};

enum ButtonSize {
  xs = "xs",
  sm = "sm",
  md = "md",
  lg = "lg",
  xl = "xl",
  xxl = "xxl",
}

const sizes = {
  [ButtonSize.xs]:
    "h-fit w-fit px-[0.40rem] py-[0.05rem] gap-1 text-[0.7rem] rounded",
  [ButtonSize.sm]: "h-fit w-fit px-1.5 py-0.5 gap-1.5 text-sm rounded",
  [ButtonSize.md]: "h-fit w-fit px-3 py-1.5 gap-2 rounded text-[0.9rem]",
  [ButtonSize.lg]:
    "h-fit w-fit px-3 py-1.5 gap-2 text-[1.05rem] md:text-[1.2rem] rounded",
  [ButtonSize.xl]:
    "h-fit w-fit px-3 py-2 gap-2 text-[1.2rem] md:px-4 md:py-3 md:gap-2.5 md:text-[1.4rem] rounded-lg",
  [ButtonSize.xxl]:
    "h-fit w-fit px-3 py-2 gap-2 text-[3rem] md:px-4 md:py-3 md:gap-2.5 md:text-[2.4rem] rounded-lg",
};

interface ButtonProps {
  id?: string;
  tabIndex?: number;
  children: React.ReactNode;
  className?: string;
  type?: ButtonHTMLAttributes<HTMLButtonElement>["type"];
  href?: string;
  variant?: keyof typeof Variants;
  size?: keyof typeof ButtonSize;
  isDisabled?: boolean;
  isIconOnly?: boolean;
  isSquared?: boolean; // New prop added here
  onClick?: (event: React.MouseEvent<HTMLButtonElement>) => void;
  "aria-label"?: string;
}

const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      id,
      tabIndex,
      children,
      className = "",
      href,
      variant = "base",
      size = "md",
      type,
      isDisabled = false,
      isIconOnly = false,
      isSquared = false, // Default value for the new prop
      onClick: propagateOnClick = () => {},
      "aria-label": ariaLabel,
    },
    ref
  ) => {
    const [isPressed, setIsPressed] = useState(false);
    const buttonRef = useRef<HTMLButtonElement>(null);
    const router = useRouter();

    const onClick = (event: React.MouseEvent<HTMLButtonElement>) => {
      setIsPressed(!isPressed);
      propagateOnClick(event);
    };

    useEffect(() => {
      if (isSquared && buttonRef.current) {
        const height = buttonRef.current.clientHeight;
        buttonRef.current.style.width = `${height}px`;
      }
    }, [isSquared, size]);

    // event handler for click
    const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
      if (isDisabled) {
        return;
      } else if (href) {
        router.push(href);
      } else {
        onClick(event);
      }
    };

    // event handler for keys
    const handleKeyUp = (event: React.KeyboardEvent<HTMLButtonElement>) => {
      if (event.key === "Enter") {
        // @ts-ignore
        handleClick(event);
      }
    };

    return (
      <button
        id={id}
        tabIndex={isDisabled ? -1 : tabIndex || 0}
        ref={(el) => {
          if (typeof ref === "function") ref(el);
          else if (ref) ref.current = el;
          // @ts-ignore
          buttonRef.current = el;
        }}
        className={twMerge(
          "flex items-center justify-center select-none whitespace-nowrap transition-all duration-150",
          sizes[size],
          variants[variant],
          isIconOnly ? "p-3" : "", // Add padding for icon only buttons
          isSquared ? "aspect-square" : "", // Make button square if isSquared is true
          isDisabled
            ? isIconOnly
              ? "text-light-700 cursor-not-allowed pointer-events-none"
              : "text-light-500 bg-light-800 cursor-not-allowed pointer-events-none shadow-light-700"
            : "",
          className
        )}
        type={type}
        onClick={handleClick}
        onKeyUp={handleKeyUp}
        aria-label={ariaLabel}
      >
        {children}
      </button>
    );
  }
);

Button.displayName = "Button";

export default Button;

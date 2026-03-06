import { ReactNode } from "react";

interface ExternalLinkProps {
  className?: string;
  children: string | number | ReactNode;
  href: string;
  isExternal?: boolean;
}

const Link = ({
  children,
  href,
  className,
  isExternal = true,
}: ExternalLinkProps) => {
  return (
    <a
      className={`underline decoration-1 underline-offset-4 break-all hover:opacity-60 transition duration-300 ease-in-out ${className}`}
      href={href}
      target={isExternal ? "_blank" : ""}
      rel={isExternal ? "noopener noreferrer" : ""}
      tabIndex={0}
    >
      {children}
      {isExternal && <span aria-hidden="true"> ↗︎</span>}
    </a>
  );
};

Link.displayName = "Link";

export default Link;

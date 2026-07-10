interface NavbarLinkProps {
  label: string;
  href: string;
  isActive: boolean;
  isNavMenu?: boolean;
}

const NavbarLink = ({
  label,
  href,
  isActive,
  isNavMenu = false,
}: NavbarLinkProps) => {
  return (
    <a
      className={`inline-flex items-center hover:underline underline-offset-8 text-light-50 font-medium hover:decoration-light-500 font-serif ${
        isActive ? "underline decoration-light-300" : ""
      } ${
        isNavMenu
          ? // Mobile menu row — matches the navbar row height (h-12,
            // 48px) with px-0 so text aligns with the logo at the
            // parent's px-4 edge.
            "min-h-12 px-0 text-base"
          : "min-h-11 px-3 text-xs lg:text-sm"
      }`}
      href={href}
    >
      {label}
    </a>
  );
};

NavbarLink.displayName = "NavbarLink";

export default NavbarLink;

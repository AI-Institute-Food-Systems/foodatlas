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
      className={`inline-flex items-center min-h-11 px-3 hover:underline underline-offset-8 text-light-50 font-medium hover:decoration-light-500 font-serif ${
        isActive ? "underline decoration-light-300" : ""
      } ${isNavMenu ? "text-2xl" : "text-base lg:text-lg"}`}
      href={href}
    >
      {label}
    </a>
  );
};

NavbarLink.displayName = "NavbarLink";

export default NavbarLink;

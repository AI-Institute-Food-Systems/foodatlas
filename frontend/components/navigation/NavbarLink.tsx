interface NavbarLinkProps {
  index: number;
  label: string;
  href: string;
  isActive: boolean;
  isNavMenu?: boolean;
}

const NavbarLink = ({
  index,
  label,
  href,
  isActive,
  isNavMenu = false,
}: NavbarLinkProps) => {
  return (
    <a
      className={`hover:underline underline-offset-8 text-light-50 font-medium hover:decoration-light-500 font-serif ${
        isActive ? "underline decoration-light-300" : ""
      } ${isNavMenu ? "text-2xl" : "text-base lg:text-lg"}`}
      href={href}
      tabIndex={index}
    >
      {label}
    </a>
  );
};

NavbarLink.displayName = "NavbarLink";

export default NavbarLink;

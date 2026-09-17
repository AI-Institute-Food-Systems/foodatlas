"use client";

import { useState, useEffect, useContext } from "react";
import { usePathname, useRouter } from "next/navigation";
import { MdMenu, MdSearch } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import NavbarLink from "@/components/navigation/NavbarLink";
import Button from "@/components/basic/Button";
import FoodAtlasIcon from "@/components/icons/FoodAltasIcon";
import { SearchContext } from "@/context/searchContext";

const NAV_ITEMS = [
  // "Explore" hidden 2026-07-02 (the "/" landing is still reachable
  // via the logo click; nav slot removed while the landing is being
  // reworked).
  { text: "Background", href: "/technical-background" },
  { text: "API", href: "/developers" },
  { text: "Downloads", href: "/food-composition-downloads" },
  { text: "About", href: "/about" },
  { text: "Contact", href: "/contact" },
];

interface NavbarProps {
  className?: string;
}

const Navbar = ({ className }: NavbarProps) => {
  const [isNavMenuOpen, setIsNavMenuOpen] = useState(false);
  const { setIsFocused, inputRef, setIsVisible, setOffsetTop } =
    useContext(SearchContext);
  const [isScrolled, setIsScrolled] = useState(false);
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 0);
    };
    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  const handleNavButtonClick = () => setIsNavMenuOpen(!isNavMenuOpen);

  // Opens the global search overlay no matter what route we're on:
  // - On a hosting route (`/` or `/results`) the bar is already
  //   mounted at a layout-anchored position — we just focus it.
  // - On an entity page the bar is mounted but invisible; we
  //   re-anchor it just below the navbar and fade it back in.
  const handleSearchButtonClick = () => {
    const hostsSearch = pathname === "/" || pathname.startsWith("/results");
    if (!hostsSearch) {
      // Anchor with a half-navbar gap under the navbar bottom —
      // 48 + 24 = 72 mobile, 56 + 28 = 84 md+. SearchBar's fly-up
      // class carries the same responsive top so docked + focused
      // states line up.
      setOffsetTop(
        typeof window !== "undefined" &&
          window.matchMedia("(min-width: 768px)").matches
          ? 84
          : 72,
      );
    }
    setIsVisible(true);
    setIsFocused(true);
    setIsNavMenuOpen(false);
    // Wait one paint so the fade-in / re-anchor has taken effect,
    // then focus without letting Safari scroll the page to bring the
    // input into view — we've already anchored it below the navbar,
    // any Safari-driven scroll would just fight our position.
    requestAnimationFrame(() =>
      inputRef.current?.focus({ preventScroll: true }),
    );
  };

  return (
    <div
      className={twMerge(
        // Base sits at z-40. When the mobile menu is open we bump to
        // z-[60] so the SearchBar portal (z-50) and its backdrop don't
        // punch through the menu.
        // min-w-[320px] mirrors the html rule in globals.css so the
        // navbar stays as wide as the page when the viewport is
        // narrower than 320 (fixed elements are sized against the
        // viewport, not the html element).
        "fixed top-0 w-[100vw] min-w-[320px] bg-[#0a0a09]/30 backdrop-blur-2xl saturate-200 px-4 md:px-24",
        isNavMenuOpen ? "z-[60]" : "z-40",
        isScrolled ? "border-b border-light-800" : "",
        className,
      )}
    >
      <div className="max-w-5xl mx-auto">
        <div className="py-1.5 w-full h-12 md:h-14 mx-auto flex justify-between items-center gap-3">
          <Button
            className="relative flex-shrink-0 cursor-pointer min-h-9 min-w-9 p-1 m-0"
            isIconOnly
            onClick={() => router.push("/")}
            aria-label="FoodAtlas home"
          >
            <FoodAtlasIcon height={30} width={120} color={"#FFFBF7"} />
          </Button>
          <div className="hidden sm:flex sm:gap-5 lg:gap-8">
            {NAV_ITEMS.map((navItem) => (
              <NavbarLink
                key={navItem.href}
                label={navItem.text}
                href={navItem.href}
                isActive={pathname === navItem.href}
              />
            ))}
          </div>
          <div className="flex items-center gap-1">
            {/* Search button — always visible, opens the global
             * overlay (re-anchored under the navbar on entity pages).
             */}
            <Button
              className="min-h-9 min-w-9"
              isIconOnly
              onClick={handleSearchButtonClick}
              aria-label="Open search"
            >
              <MdSearch className="w-5 h-5" />
            </Button>
            {/* Mobile menu */}
            <div className="sm:hidden">
              <Button
                className="min-h-9 min-w-9"
                onClick={handleNavButtonClick}
                isIconOnly
                aria-label="Open navigation menu"
              >
                <MdMenu className="w-6 h-6" />
              </Button>
            </div>
          </div>
        </div>
        {isNavMenuOpen && (
          <div className="h-[calc(100dvh-3rem)] overflow-y-auto pt-8 pb-1.5 pl-2 flex flex-col gap-4">
            {NAV_ITEMS.map((navItem) => (
              <NavbarLink
                key={navItem.href}
                label={navItem.text}
                href={navItem.href}
                isActive={pathname === navItem.href}
                isNavMenu
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

Navbar.displayName = "Navbar";

export default Navbar;

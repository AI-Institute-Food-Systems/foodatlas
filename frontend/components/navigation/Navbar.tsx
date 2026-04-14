"use client";

import { useState, useEffect, useContext } from "react";
import { usePathname, useRouter } from "next/navigation";
import { MdMenu } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import NavbarLink from "@/components/navigation/NavbarLink";
import Button from "@/components/basic/Button";
import FoodAtlasIcon from "@/components/icons/FoodAltasIcon";
import { SearchContext } from "@/context/searchContext";

const NAV_ITEMS = [
  { text: "Background", href: "/technical-background" },
  // { text: "API", href: "/food-composition-api" },
  { text: "Downloads", href: "/food-composition-downloads" },
  { text: "About", href: "/about" },
  { text: "Contact", href: "/contact" },
];

interface NavbarProps {
  className?: string;
}

const Navbar = ({ className }: NavbarProps) => {
  const [isNavMenuOpen, setIsNavMenuOpen] = useState(false);
  const { isFocused, setIsFocused, inputRef, isVisible, setIsVisible } =
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

  const handleSearchButtonClick = (
    event: React.MouseEvent<HTMLButtonElement>
  ) => {
    if (pathname === "/") {
      event.stopPropagation();
      event.preventDefault();

      if (isFocused) {
        // @ts-ignore
        inputRef.current.blur();
        // setIsFocused((prev) => {
        //   inputRef.current.blur();
        //   return !prev;
        // });
      } else {
        // @ts-ignore
        inputRef.current.focus();
      }
      // inputRef.current.focus();
      // setIsVisible(!isVisible);
      setIsFocused(!isFocused);
    } else {
      // @ts-ignore
      setIsVisible((prev) => !prev);
      setIsFocused(true);
    }

    // if (isFocused) {
    //   setIsFocused(false);
    //   setIsVisible(false);
    // } else {
    //   setIsVisible(true);
    //   setIsFocused(true);
    // }

    // setIsFocused((prev) => {
    //   // setIsVisible(!isVisible);
    //   setIsVisible(true);
    //   return !prev;
    // });
  };

  useEffect(() => {
    if (isFocused && inputRef.current) {
    }
  }, [isFocused, inputRef]);

  return (
    <div
      className={twMerge(
        "fixed top-0 w-[100vw] bg-[#0a0a09]/30 backdrop-blur-2xl saturate-200 z-40 px-3 md:px-12",
        isScrolled ? "border-b border-light-800" : "",
        className
      )}
    >
      <div className="max-w-6xl mx-auto">
        <div className="py-2 w-full h-14 sm:h-16 md:h-20 mx-auto flex justify-between items-center">
          <Button
            className="relative flex-shrink-0 cursor-pointer p-0 m-0"
            isIconOnly
            tabIndex={10}
            onClick={() => router.push("/")}
          >
            <FoodAtlasIcon height={45} width={""} color={"#FFFBF7"} />
          </Button>
          <div className="hidden md:flex md:gap-8 lg:gap-20">
            {NAV_ITEMS.map((navItem, index) => (
              <NavbarLink
                index={index + 11}
                key={navItem.href}
                label={navItem.text}
                href={navItem.href}
                isActive={pathname === navItem.href}
              />
            ))}
          </div>
          {/* search & menu button container */}
          <div className="md:hidden flex items-center">
            {/* search button */}
            {/* <Button
              tabIndex={0}
              isDisabled
              isIconOnly
              onClick={handleSearchButtonClick}
            >
              <MdSearch className="w-7 h-7" />
            </Button> */}
            {/* menu button */}
            <div className="">
              <Button onClick={handleNavButtonClick} isIconOnly>
                <MdMenu className="w-7 h-7" />
              </Button>
            </div>
          </div>
        </div>
        {isNavMenuOpen && (
          <div className="h-screen mt-16 mx-3 flex flex-col gap-12">
            {NAV_ITEMS.map((navItem, index) => (
              <NavbarLink
                index={index}
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

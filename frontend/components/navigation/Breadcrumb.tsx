"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { MdChevronRight } from "react-icons/md";

// Slug → display title for the top-level content pages. Entity pages
// and search results are dynamic and handled separately (or not shown).
const TITLES: Record<string, string> = {
  "/about": "About",
  "/contact": "Contact",
  "/developers": "Developer API",
  "/food-composition-api": "API Documentation",
  "/food-composition-downloads": "Downloads",
  "/technical-background": "Technical Background",
};

const Breadcrumb = () => {
  const pathname = usePathname();
  const title = TITLES[pathname];
  // Only render breadcrumbs for known content pages — dynamic routes
  // (entities, results) don't fit the "Home / X" shape.
  if (!title) return null;

  return (
    <nav
      aria-label="Breadcrumb"
      className="mb-6 flex items-center gap-1.5 text-[11px] font-mono italic uppercase tracking-[0.15em]"
    >
      <Link
        href="/"
        className="text-light-500 hover:text-light-200 transition-colors"
      >
        Home
      </Link>
      <MdChevronRight
        aria-hidden
        className="text-light-600 w-3.5 h-3.5 shrink-0"
      />
      <span className="text-light-300">{title}</span>
    </nav>
  );
};

Breadcrumb.displayName = "Breadcrumb";
export default Breadcrumb;

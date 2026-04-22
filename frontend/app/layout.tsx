import { Portal } from "@headlessui/react";
import { GoogleAnalytics } from "@next/third-parties/google";

import Providers from "@/app/providers";
import SearchBar from "@/components/search/SearchBar";
import "@/styles/globals.css";
import { fontMono, fontSans, fontSerif } from "@/styles/fonts";

interface ClientLayoutProps {
  children: React.ReactNode;
}

const Layout = ({ children }: ClientLayoutProps) => {
  return (
    <html
      className={`antialiased ${fontMono.variable} ${fontSerif.variable} ${fontSans.variable}`}
      lang="en"
      suppressHydrationWarning
    >
      <head />
      <body>
        <nav aria-label="Skip">
          <a
            href="#main-content"
            className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-[100] focus:px-4 focus:py-2 focus:bg-accent-600 focus:text-white focus:rounded focus:shadow-lg"
          >
            Skip to main content
          </a>
        </nav>
        <GoogleAnalytics
          gaId={process.env.NEXT_PUBLIC_GA_MEASUREMENT_ID ?? ""}
        />
        <Providers>
          <main id="main-content">
            {children}
            <Portal>
              <SearchBar />
            </Portal>
          </main>
        </Providers>
      </body>
    </html>
  );
};

export default Layout;

Layout.displayName = "Layout";

import { Portal } from "@headlessui/react";
import { GoogleAnalytics } from "@next/third-parties/google";
import type { Viewport } from "next";
import Script from "next/script";

import Providers from "@/app/providers";
import NavigationProgress from "@/components/navigation/NavigationProgress";
import SearchBar from "@/components/search/SearchBar";
import "@/styles/globals.css";
import { fontMono, fontSans, fontSerif } from "@/styles/fonts";

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  // `interactive-widget=resizes-content` tells iOS Safari to resize the
  // layout viewport when the soft keyboard opens instead of shifting
  // the visual viewport upward. Without this, focusing the SearchBar
  // input triggers a jarring "background scrolls up including navbar"
  // animation. Only newer Safari respects the flag; older versions
  // fall back to the default `resizes-visual`.
  interactiveWidget: "resizes-content",
  // `viewport-fit=cover` lets the webview extend behind the dynamic
  // island / notch and the home-indicator gutter, so fullscreen
  // sheets (mobile modals) hit the screen edges instead of stopping
  // at Safari's chrome. `env(safe-area-inset-*)` inside components
  // keeps content clear of those regions.
  viewportFit: "cover",
};

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
        <a
          href="#main-content"
          className="sr-only focus:not-sr-only focus:fixed focus:top-2 focus:left-2 focus:z-[100] focus:px-4 focus:py-2 focus:bg-accent-600 focus:text-white focus:rounded focus:shadow-lg"
        >
          Skip to main content
        </a>
        <GoogleAnalytics
          gaId={process.env.NEXT_PUBLIC_GA_MEASUREMENT_ID ?? ""}
        />
        {process.env.VERCEL_ENV === "production" && (
          <Script
            defer
            src="/_a/script.js"
            data-website-id="a63b88b0-aa17-4ca1-a3c6-62a568fe0757"
            data-host-url="/_a"
          />
        )}
        <Providers>
          <NavigationProgress />
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

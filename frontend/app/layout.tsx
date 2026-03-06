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
        <GoogleAnalytics
          gaId={process.env.NEXT_PUBLIC_GA_MEASUREMENT_ID ?? ""}
        />
        <Providers>
          <main>
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

import { IBM_Plex_Mono, Aleo, Roboto } from "next/font/google";

export const fontMono = IBM_Plex_Mono({
  weight: ["100", "200", "300", "400", "500", "600", "700"],
  subsets: ["latin"],
  style: ["normal", "italic"],
  variable: "--font-mono",
  display: "swap",
  // 7 weights x 2 styles = 14 files. Preloaded, they all went out at High
  // priority on every page and competed with the LCP text and image on a
  // slow mobile link. Mono sets only labels and ids, never the LCP, so it
  // loads on use instead; display: swap keeps that text visible meanwhile.
  preload: false,
});

export const fontSerif = Aleo({
  weight: ["400", "700"],
  subsets: ["latin"],
  style: ["normal"],
  variable: "--font-serif",
  display: "swap",
});

export const fontSans = Roboto({
  weight: ["400", "500", "700"],
  subsets: ["latin"],
  style: ["normal"],
  variable: "--font-sans",
  display: "swap",
});

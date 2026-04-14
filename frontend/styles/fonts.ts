import { IBM_Plex_Mono, Aleo, Roboto } from "next/font/google";

export const fontMono = IBM_Plex_Mono({
  weight: ["100", "200", "300", "400", "500", "600", "700"],
  subsets: ["latin"],
  style: ["normal", "italic"],
  variable: "--font-mono",
  display: "swap",
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

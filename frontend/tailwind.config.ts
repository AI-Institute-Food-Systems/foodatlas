const plugin = require("tailwindcss/plugin");
import type { Config } from "tailwindcss";
import defaultColors from "tailwindcss/colors";
import resolveConfig from "tailwindcss/resolveConfig";

export type DefaultColors = typeof defaultColors;

const colors = {
  accent: {
    "100": "#FFCCBC",
    "200": "#FFAB91",
    "300": "#FF8A65",
    "400": "#FF7043",
    "500": "#FF5722",
    "600": "#F4511E",
    "700": "#E64A19",
    "800": "#D84315",
    "900": "#BF360C",
  },
  light: {
    "50": "#fff9f2",
    "100": "#f9f0e7",
    "200": "#efe6de",
    "300": "#dad3cb",
    "400": "#b2aca5",
    "500": "#837e7a",
    "600": "#575451",
    "700": "#383634",
    "800": "#21201f",
    "900": "#1a1918",
    "950": "#151414",
    "1000": "#0D0C0C",
  },
};

export type Colors = typeof colors;

const config: Config = {
  darkMode: "class",
  important: true,
  content: [
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors,
      fontFamily: {
        mono: ["var(--font-mono)"],
        serif: ["var(--font-serif)"],
        sans: ["var(--font-sans)"],
      },
      backgroundImage: {
        "gradient-radial": "radial-gradient(var(--tw-gradient-stops))",
        "gradient-conic":
          "conic-gradient(from 180deg at 50% 50%, var(--tw-gradient-stops))",
      },
      textShadow: {
        sm: "0 0.063rem 0.5rem rgba(0,0,0,0.4)",
        DEFAULT: "0 0 1rem rgba(0,0,0,0.8)",
        lg: "0 0 1.5rem rgba(0,0,0,0.8)",
      },
      textHighlight: {
        sm: "0 0 0.063rem 0.5rem rgba(255,255,255,0.5)",
        DEFAULT: "0 0 1rem rgba(255,255,255,0.5)",
        lg: "0 0 1.5rem rgba(255,255,255,0.55)",
      },
    },
  },
  plugins: [
    // @ts-ignore
    plugin(function ({ matchUtilities, theme }) {
      matchUtilities(
        {
          // @ts-ignore
          "text-shadow": (value) => ({
            textShadow: value,
          }),
        },
        { values: theme("textShadow") }
      );
    }),
  ],
};

export default config;

const tw = resolveConfig(config);

export const { theme } = tw as unknown as {
  theme: (typeof tw)["theme"] & { colors: DefaultColors & Colors };
};

import { twMerge } from "tailwind-merge";

// Shared class stack for text inputs — keeps every Field visually
// aligned without repeating the ring/focus/border rules five times.
export const FIELD_CLASS = twMerge(
  "mt-2 block w-full rounded-lg bg-light-800 border-light-700/50 border py-2 px-3 text-sm/6 text-light-50 placeholder-light-500",
  "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25",
);
export const LABEL_CLASS = "text-sm/6 font-medium text-white";
export const HINT_CLASS = "font-normal text-light-400";

"use client";

import { useEffect } from "react";

import { track } from "@/utils/umami";

interface Props {
  kind: "not_found" | "error";
}

// Fires one `error_page` event on mount. Split out so `not-found.tsx` can
// stay a server component and only this leaf ships to the client.
const ErrorPageBeacon = ({ kind }: Props) => {
  useEffect(() => {
    track("error_page", { kind, path: window.location.pathname });
  }, [kind]);
  return null;
};

ErrorPageBeacon.displayName = "ErrorPageBeacon";

export default ErrorPageBeacon;

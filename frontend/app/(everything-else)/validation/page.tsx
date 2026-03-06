"use client";

import { SessionProvider } from "next-auth/react";

import ValidationPageContent from "@/components/validation/ValidationContent";

export default function ValidationPage() {
  return (
    <SessionProvider>
      <ValidationPageContent />
    </SessionProvider>
  );
}

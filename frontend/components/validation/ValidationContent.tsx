/**
 * @author Lukas Masopust
 * @email lmasopust@ucdavis.edu
 * @create date 2025-05-22 14:42:27
 * @modify date 2025-05-22 14:42:27
 * @desc Wrapper for password protected validation page
 */

"use client";

import { useSession } from "next-auth/react";

import Password from "@/components/misc/Password";
import SignedInContent from "@/components/validation/SignedInContent";

export default function ValidationPageContent() {
  const { status } = useSession();
  const signedIn = status === "authenticated";

  // show nothing while loading
  if (status === "loading") {
    return null;
  }

  return (
    <div className="">
      {signedIn ? (
        <SignedInContent />
      ) : (
        <div className="flex translate-y-1/2 justify-center">
          <Password />
        </div>
      )}
    </div>
  );
}

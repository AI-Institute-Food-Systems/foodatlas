/**
 * @author Lukas Masopust
 * @email lmasopust@ucdavis.edu
 * @create date 2025-05-22 14:42:27
 * @modify date 2025-05-22 14:42:27
 * @desc Wrapper for password protected validation page
 */

"use client";

import { useSession } from "next-auth/react";

import Skeleton from "@/components/basic/Skeleton";
import Password from "@/components/misc/Password";
import SignedInContent from "@/components/validation/SignedInContent";

export default function ValidationPageContent() {
  const { status } = useSession();
  const signedIn = status === "authenticated";

  // Resolving the session used to render nothing at all, so the page was
  // blank until it settled. Stand in with the password card's footprint —
  // it's the branch an unauthenticated visitor almost always lands on.
  if (status === "loading") {
    return (
      <div className="flex translate-y-1/2 justify-center">
        <Skeleton shape="block" className="h-40 w-80 max-w-[90vw]" />
      </div>
    );
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

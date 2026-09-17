import { twMerge } from "tailwind-merge";

import {
  SKELETON_SHAPE,
  SKELETON_TONE,
  type SkeletonShape,
  type SkeletonTone,
} from "@/components/basic/skeletonTokens";

interface SkeletonProps {
  className?: string;
  tone?: SkeletonTone;
  shape?: SkeletonShape;
}

// The one loading placeholder in the app. Everything that used to
// hand-roll an `animate-pulse` div goes through here, so fill, radius and
// motion behaviour can only be changed in one place.
//
// No "use client" and no hooks — this renders in Server Components (route
// loading shells, Suspense fallbacks) and in client tables alike. That is
// load-bearing: the same element tree paints on both sides of the
// route -> SSR -> hydration handoff, so the handoff cannot visibly shift.
//
// aria-hidden because the placeholder is decorative; the surrounding
// container owns the `role="status"` / `aria-busy` announcement so screen
// readers hear "loading" once rather than once per bar.
const Skeleton = ({
  className,
  tone = "default",
  shape = "text",
}: SkeletonProps) => (
  <div
    aria-hidden
    className={twMerge(
      // The hairline border gives the placeholder an edge even at the
      // low fill contrast the default tone deliberately uses.
      "w-full h-4 border border-light-50/10",
      SKELETON_TONE[tone],
      SKELETON_SHAPE[shape],
      className
    )}
  />
);

Skeleton.displayName = "Skeleton";

export default Skeleton;

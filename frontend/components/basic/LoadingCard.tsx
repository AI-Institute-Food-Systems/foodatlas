import { twMerge } from "tailwind-merge";

import Skeleton from "@/components/basic/Skeleton";

interface LoadingCardProps {
  className?: string;
}

// Deprecated — use <Skeleton /> directly.
//
// Kept as a thin wrapper so the fill/motion fix reaches all existing call
// sites in a single change: the old implementation filled with
// bg-light-950, the exact same colour as Card, which made every skeleton
// inside a Card a 1.00:1 invisible rectangle. Removed once the remaining
// call sites have been migrated.
const LoadingCard = ({ className }: LoadingCardProps) => (
  <Skeleton shape="block" className={twMerge("h-24", className)} />
);

LoadingCard.displayName = "LoadingCard";

export default LoadingCard;

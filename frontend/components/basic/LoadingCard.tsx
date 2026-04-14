import { twMerge } from "tailwind-merge";

interface LoadingCardProps {
  className?: string;
}

const LoadingCard = ({ className }: LoadingCardProps) => {
  return (
    <div
      className={twMerge(
        "w-full h-24 bg-light-950 shadow-inner shadow-light-700/20 border border-light-50/10 rounded-lg animate-pulse",
        className
      )}
    />
  );
};

LoadingCard.displayName = "LoadingCard";

export default LoadingCard;

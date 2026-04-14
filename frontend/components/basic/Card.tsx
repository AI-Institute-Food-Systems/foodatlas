import { twMerge } from "tailwind-merge";

interface CardProps {
  className?: string;
  children: React.ReactNode;
  onClick?: () => void;
}

const Card = ({ className, children, onClick }: CardProps) => {
  return (
    <div
      className={twMerge(
        "w-full p-5 md:p-6 flex flex-col bg-light-950 shadow-[inset_0_5px_8px_rgba(255,249,242,0.02)] border-[1.5px] border-light-50/[0.08] rounded-xl relative",
        className
      )}
      onClick={onClick}
    >
      {children}
    </div>
  );
};

Card.displayName = "Card";

export default Card;

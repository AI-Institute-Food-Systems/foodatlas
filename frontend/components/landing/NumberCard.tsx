import Card from "@/components/basic/Card";
import LoadingCard from "@/components/basic/LoadingCard";

interface NumberCardProps {
  isLoading: boolean;
  icon: React.ReactNode;
  number: number;
  label: string;
}

const NumberCard = ({ isLoading, icon, number, label }: NumberCardProps) => {
  if (isLoading) {
    return <LoadingCard className="h-[15.625rem]" />;
  } else {
    return (
      <Card className="!bg-light-900">
        <div className="h-48 flex flex-col justify-between">
          <div className="relative w-12 h-12 md:w-14 md:h-14">{icon}</div>
          <div className="whitespace-nowrap">
            <p className="text-xl sm:text-2xl md:text-3xl lg:text-4xl">
              {number?.toLocaleString() ?? "0"}
            </p>
            <p className="text-xs md:text-sm lg:text-base font-mono italic text-light-200">
              {label}
            </p>
          </div>
        </div>
      </Card>
    );
  }
};

NumberCard.displayName = "NumberCard";

export default NumberCard;

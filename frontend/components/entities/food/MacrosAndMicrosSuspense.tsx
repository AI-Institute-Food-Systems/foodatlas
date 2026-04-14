import LoadingCard from "@/components/basic/LoadingCard";

const MacrosAndMicrosSuspense = () => {
  return (
    <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-3">
      <LoadingCard className="h-[16.1875rem]" />
      <LoadingCard className="h-[16.1875rem]" />
      <LoadingCard className="h-[16.1875rem]" />
      <LoadingCard className="h-[16.1875rem]" />
      <LoadingCard className="h-[16.1875rem]" />
      <LoadingCard className="h-[16.1875rem]" />
    </div>
  );
};

export default MacrosAndMicrosSuspense;

MacrosAndMicrosSuspense.displayName = "MacrosAndMicrosSuspense";

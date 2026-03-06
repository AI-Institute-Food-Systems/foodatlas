import { IoInformationCircleOutline } from "react-icons/io5";

const InfoBanner = ({ description }: { description: React.ReactNode }) => {
  return (
    <div className="px-2.5 py-2.5 w-full border border-amber-600 bg-amber-600/10 rounded-md text-xs flex items-center gap-2 text-amber-500 shadow-amber-800 shadow-[inset_0_1px_6px_rgba(0,0,0,0.5)]">
      <IoInformationCircleOutline className="w-4 h-4 flex-shrink-0" />
      <div>{description}</div>
    </div>
  );
};

export default InfoBanner;

InfoBanner.displayName = "InfoBanner";

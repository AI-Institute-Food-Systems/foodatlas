import Citation from "@/components/basic/Citation";
import CopyCitationButton from "@/components/basic/CopyCitationButton";
import { Publication } from "@/types";

interface CopyCitationProps {
  publication: Publication;
  // Card already draws a box; standalone use (the terms popup) needs one.
  boxed?: boolean;
}

const CopyCitation = ({ publication, boxed = false }: CopyCitationProps) => (
  <div
    className={
      boxed
        ? "flex items-start gap-3 rounded-lg border border-light-700/50 bg-light-800 p-4"
        : "flex items-start gap-4"
    }
  >
    <p
      className={
        boxed
          ? "flex-1 text-sm leading-relaxed text-light-200"
          : "flex-1 leading-relaxed text-light-200"
      }
    >
      <Citation publication={publication} />
    </p>
    <CopyCitationButton publication={publication} />
  </div>
);

CopyCitation.displayName = "CopyCitation";
export default CopyCitation;

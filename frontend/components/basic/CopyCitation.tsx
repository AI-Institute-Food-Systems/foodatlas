"use client";

import { useState } from "react";
import { MdCheck, MdContentCopy } from "react-icons/md";

import Button from "@/components/basic/Button";
import Citation from "@/components/basic/Citation";
import { Publication } from "@/types";
import { citationText } from "@/utils/publications";

interface CopyCitationProps {
  publication: Publication;
}

const CopyCitation = ({ publication }: CopyCitationProps) => {
  const [copied, setCopied] = useState(false);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(citationText(publication));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard can be blocked; the text stays selectable.
    }
  };

  return (
    <div className="flex items-start gap-3 rounded-lg border border-light-700/50 bg-light-800 p-4">
      <p className="flex-1 text-sm leading-relaxed text-light-200 select-all">
        <Citation publication={publication} />
      </p>
      <Button
        type="button"
        variant="outlined"
        size="sm"
        className="shrink-0"
        onClick={copy}
        aria-label={copied ? "Citation copied" : "Copy citation"}
      >
        {copied ? <MdCheck /> : <MdContentCopy />}
        {copied ? "Copied" : "Copy"}
      </Button>
    </div>
  );
};

CopyCitation.displayName = "CopyCitation";
export default CopyCitation;

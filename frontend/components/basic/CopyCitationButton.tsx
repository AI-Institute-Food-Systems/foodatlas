"use client";

import { useState } from "react";
import { MdCheck, MdContentCopy } from "react-icons/md";

import Button from "@/components/basic/Button";
import { Publication } from "@/types";
import { citationText } from "@/utils/publications";

interface CopyCitationButtonProps {
  publication: Publication;
}

const CopyCitationButton = ({ publication }: CopyCitationButtonProps) => {
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
  );
};

CopyCitationButton.displayName = "CopyCitationButton";
export default CopyCitationButton;

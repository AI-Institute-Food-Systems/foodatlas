"use client";

// The assays a bioactivity↔disease link rests on, linked out to their source
// record. This is what makes the claim auditable: a reader who doubts the
// association can open the actual PubChem or ChEMBL assay behind it.
//
// The array is capped by the materializer (ASSAY_CAP = 25), so a row backed
// by more assays than that shows a representative sample. Say so rather than
// implying the list is exhaustive.

import Link from "@/components/basic/Link";
import { Tooltip } from "@/components/basic/Tooltip";
import { assayExternalUrl } from "@/utils/utils";

const DEFAULT_VISIBLE = 2;

interface Props {
  assays: string[];
  // Total assays behind the row. When it exceeds assays.length the array was
  // capped upstream, and the overflow hint should say so.
  totalCount?: number;
  visible?: number;
}

const AssayEvidenceLinks = ({
  assays,
  totalCount,
  visible = DEFAULT_VISIBLE,
}: Props) => {
  if (!assays?.length) return null;
  const shown = assays.slice(0, visible);
  const total = totalCount ?? assays.length;
  const hidden = total - shown.length;

  return (
    <span className="inline-flex flex-wrap items-baseline gap-1">
      {shown.map((assay) => (
        <AssayLink key={assay} assay={assay} />
      ))}
      {hidden > 0 && (
        <Tooltip content={overflowHint(assays, visible, total)}>
          <span className="text-[10px] font-mono text-light-500">+{hidden}</span>
        </Tooltip>
      )}
    </span>
  );
};

const overflowHint = (assays: string[], visible: number, total: number) => {
  const rest = assays.slice(visible);
  const listed = rest.join(", ");
  return total > assays.length
    ? `${listed} — and more; the evidence list is capped at ${assays.length}.`
    : listed;
};

const AssayLink = ({ assay }: { assay: string }) => {
  const ext = assayExternalUrl(assay);
  if (!ext) {
    return <span className="text-[10px] font-mono text-light-400">{assay}</span>;
  }
  return (
    <Tooltip content={`View this assay on ${ext.source}`}>
      <Link href={ext.url} className="text-[10px] font-mono no-underline">
        {assay}
      </Link>
    </Tooltip>
  );
};

AssayEvidenceLinks.displayName = "AssayEvidenceLinks";
export default AssayEvidenceLinks;

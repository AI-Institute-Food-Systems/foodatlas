"use client";

// The protein target a bioactivity↔disease link runs through.
//
// This is the answer to "through what?" — the bridge's target gene is the
// bridging assay's own target, so it names the mechanism rather than merely
// asserting one. The API pairs each id with a readable label where it has
// one (~99% of rows) and collapses the Entrez/UniProt duplicate of the same
// protein; an unlabelled id still renders, linked, rather than vanishing.

import Link from "@/components/basic/Link";
import { Tooltip } from "@/components/basic/Tooltip";
import { entrezGeneUrl, uniprotUrl } from "@/utils/utils";
import type { AssayTarget } from "@/types";

const DEFAULT_VISIBLE = 3;

// Ids arrive prefixed exactly as the source records them — "NCBIGene: 4780",
// "UniProt: Q16236" — so strip the prefix before building a URL.
export const targetUrl = (id: string): string | null => {
  const entrez = id.match(/^NCBIGene:\s*(\d+)$/i);
  if (entrez) return entrezGeneUrl(entrez[1]);
  const uniprot = id.match(/^UniProt:\s*(\S+)$/i);
  if (uniprot) return uniprotUrl(uniprot[1]);
  return null;
};

// Labels are free text from the assay record and run long ("nuclear factor
// erythroid 2-related factor 2 isoform 1 [Homo sapiens]"). Truncate for the
// chip and keep the full string in the tooltip.
const MAX_LABEL = 28;
const short = (label: string) =>
  label.length > MAX_LABEL ? `${label.slice(0, MAX_LABEL - 1)}…` : label;

interface Props {
  targets: AssayTarget[];
  visible?: number;
}

const TargetGeneChips = ({ targets, visible = DEFAULT_VISIBLE }: Props) => {
  if (!targets?.length) return null;
  const shown = targets.slice(0, visible);
  const hidden = targets.length - shown.length;

  return (
    <span className="inline-flex flex-wrap items-baseline gap-1">
      {shown.map((target) => (
        <TargetChip key={target.id} target={target} />
      ))}
      {hidden > 0 && (
        <Tooltip
          content={targets
            .slice(visible)
            .map((t) => t.label ?? t.id)
            .join(", ")}
        >
          <span className="text-[10px] font-mono text-light-500">+{hidden}</span>
        </Tooltip>
      )}
    </span>
  );
};

const TargetChip = ({ target }: { target: AssayTarget }) => {
  const url = targetUrl(target.id);
  const text = target.label ? short(target.label) : target.id;
  const body = url ? (
    <Link href={url} className="text-[10px] font-mono no-underline">
      {text}
    </Link>
  ) : (
    <span className="text-[10px] font-mono text-light-300">{text}</span>
  );

  // Always tooltip the id, so a reader can tell which of the two identifier
  // systems backs the label they're looking at.
  return (
    <Tooltip content={target.label ? `${target.label} — ${target.id}` : target.id}>
      <span className="rounded border border-light-700 px-1 py-[1px]">{body}</span>
    </Tooltip>
  );
};

TargetGeneChips.displayName = "TargetGeneChips";
export default TargetGeneChips;

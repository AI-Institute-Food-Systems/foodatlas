import Link from "@/components/basic/Link";
import { Publication } from "@/types";
import { doiUrl } from "@/utils/publications";

interface CitationProps {
  publication: Publication;
}

// One APA reference, rendered inline — no wrapper element and no typography
// of its own, so the caller keeps control of the block. /about drops these
// straight into <li>s of a list-decimal <ol>; the "How to Cite" cards wrap
// them in the <p> they already had. Emitting a <p> here would break the
// list-inside markers.
const Citation = ({ publication }: CitationProps) => {
  const { authors, year, title, venue, kind, volume, issue, articleNumber } =
    publication;

  return (
    <>
      {`${authors} (${year}). ${title}. `}
      {kind === "proceedings" && "In "}
      <i>{venue}</i>
      {volume && (
        <>
          , <i>{volume}</i>
          {issue && `(${issue})`}
        </>
      )}
      {articleNumber && `, ${articleNumber}`}.{" "}
      <Link href={doiUrl(publication.doi)}>{doiUrl(publication.doi)}</Link>
    </>
  );
};

Citation.displayName = "Citation";

export default Citation;

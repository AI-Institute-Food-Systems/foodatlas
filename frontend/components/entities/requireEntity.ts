import { notFound } from "next/navigation";

import { getChemicalBioactivities, lookupMetaData } from "@/utils/fetching";
import type { EntityType } from "@/utils/site";
import { decodeSpace } from "@/utils/utils";

// 404 an entity route whose slug names nothing. Called from each entity's
// [slug]/layout.tsx: the layout sits above loading.tsx's Suspense boundary,
// so this settles before the skeleton streams. Anywhere below it the 200 is
// already on the wire and notFound() can only swap the body — which Search
// Console reports as a soft 404.
//
// Fails open. Only a definitive "no such entity" 404s; an API error renders
// the page as before, since a 404 on a blip deindexes a real page.
export async function requireEntity(
  type: EntityType,
  slug: string
): Promise<void> {
  let commonName: string;
  try {
    commonName = decodeSpace(decodeURIComponent(slug));
  } catch {
    notFound(); // malformed percent-encoding, e.g. /food/%E0
  }
  if ((await lookupMetaData(commonName, type)) !== "missing") return;

  // mv_chemical_entities only holds chemicals reached by composition,
  // literature correlations or their ancestors (db/src/etl/materializer.py).
  // Chemicals known only from bioassays have no metadata yet a full page —
  // about a third of a bioactivity's chemical list — so for them, missing
  // metadata alone is not enough. Same call and args as the page makes, so
  // it is served from the fetch cache there.
  if (type === "chemical") {
    const bio = await getChemicalBioactivities(commonName);
    if (bio?.metadata?.total_rows !== 0) return;
  }
  notFound();
}

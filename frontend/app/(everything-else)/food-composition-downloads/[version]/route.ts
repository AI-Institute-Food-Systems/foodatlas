import { NextResponse } from "next/server";

import type { DownloadEntry } from "@/types";
import { getDownloadEntries } from "@/utils/fetching";
import { sendUmamiEvent } from "@/utils/umami";

// Download links used to point straight at S3, which made every download
// invisible: nothing of ours saw the click, and the bucket has no access
// logging. Now the table links here; this counts the download in umami and
// 302s to the same S3 object. The manifest, /v1/bundles and /download are
// untouched — API consumers who fetch S3 directly are simply not counted.
export const dynamic = "force-dynamic";

const normalise = (v: string) => v.replace(/^v/i, "");

export async function GET(
  req: Request,
  { params }: { params: { version: string } }
) {
  let entries: DownloadEntry[] = [];
  try {
    entries = await getDownloadEntries();
  } catch {
    // Manifest unreachable — same 404 as an unknown version below.
  }
  const wanted = normalise(params.version);
  const entry = entries.find((e) => normalise(e.version) === wanted);
  if (!entry) {
    return NextResponse.json({ error: "unknown version" }, { status: 404 });
  }

  // Awaited on purpose: Next 14.2 has no `after()`, and on Vercel anything
  // still in flight when the response is sent gets killed with the function.
  // sendUmamiEvent swallows errors and caps itself at 1.5 s.
  await sendUmamiEvent(req, "bundle_download", {
    version: entry.version,
    kgc_run: entry.kgc_run,
    file_size: entry.file_size,
  });

  return NextResponse.redirect(entry.download_link, 302);
}

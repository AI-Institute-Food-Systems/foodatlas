import NextLink from "next/link";

import { getLatestBundle } from "@/utils/fetching";

// Server-rendered news line for the hero. Fetches the newest bundle
// from the downloads manifest and links to /food-composition-downloads.
// Falls back to a static string if the manifest is unreachable so a
// flaky S3 read never breaks the landing page.
const FALLBACK_VERSION = "v4.1";
const DOWNLOADS_HREF = "/food-composition-downloads";

const NewsNotification = async () => {
  const latest = await getLatestBundle();
  const version = latest?.version ?? FALLBACK_VERSION;

  return (
    <NextLink
      href={DOWNLOADS_HREF}
      aria-label={`View food composition downloads — ${version} release`}
      className="group flex items-center gap-2 md:gap-3 leading-none rounded"
    >
      <span className="font-mono italic text-[10px] md:text-xs uppercase tracking-[0.22em] text-accent-500">
        News
      </span>
      <span
        className="block h-3 w-px bg-accent-500/40"
        aria-hidden
      />
      <span
        className="font-serif italic text-xs md:text-sm text-light-200 transition duration-300 ease-in-out group-hover:text-accent-500 group-hover:underline group-hover:decoration-1 group-hover:underline-offset-4"
      >
        FoodAtlas Knowledge Graph{" "}
        <strong className="not-italic font-semibold text-white group-hover:text-accent-500 transition duration-300 ease-in-out">
          {version}
        </strong>{" "}
        released
      </span>
    </NextLink>
  );
};

NewsNotification.displayName = "NewsNotification";

export default NewsNotification;

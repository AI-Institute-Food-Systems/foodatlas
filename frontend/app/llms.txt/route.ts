import { apiBase, getLatestBundle } from "@/utils/fetching";
import { apiFetch } from "@/utils/apiFetch";
import { buildLlmsTxt, type GraphStatistics } from "@/utils/llmsTxt";

// /llms.txt, generated so its counts and latest release follow the data.
// Read at request time (there is no API at build), and cached for a day at
// the edge, like the sitemap index. Either fetch failing drops its numbers,
// never the file.
export const dynamic = "force-dynamic";

const getStatistics = async (): Promise<GraphStatistics | null> => {
  try {
    const res = await apiFetch(`${apiBase()}/metadata/statistics`, {
      revalidate: 86400,
    });
    if (!res.ok) return null;
    const json = await res.json();
    return json?.data?.statistics ?? null;
  } catch {
    return null;
  }
};

export async function GET(): Promise<Response> {
  const [stats, latest] = await Promise.all([
    getStatistics(),
    getLatestBundle(),
  ]);
  return new Response(buildLlmsTxt(stats, latest), {
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "public, max-age=0, s-maxage=86400",
    },
  });
}

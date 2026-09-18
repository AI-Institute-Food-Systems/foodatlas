import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const entries = [
  {
    version: "v4.12",
    release_date: "2026-09-18",
    file_size: "1.2 GB",
    kgc_run: "20260918T100923Z",
    download_link: "https://downloads.example/bundles/v4.12.zip",
    summary_link: "https://downloads.example/bundles/v4.12.md",
  },
];

const getDownloadEntries = vi.fn();
vi.mock("@/utils/fetching", () => ({
  getDownloadEntries: () => getDownloadEntries(),
}));

let fetchMock: ReturnType<typeof vi.fn>;

const load = async () => {
  vi.resetModules();
  process.env.VERCEL_ENV = "production";
  return await import("../[version]/route");
};

const get = (version: string) =>
  new Request(`https://foodatlas.ai/food-composition-downloads/${version}`, {
    headers: {
      "x-forwarded-for": "203.0.113.9",
      "user-agent": "Mozilla/5.0 test",
    },
  });

beforeEach(() => {
  getDownloadEntries.mockResolvedValue(entries);
  fetchMock = vi.fn().mockResolvedValue(new Response("ok"));
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  delete process.env.VERCEL_ENV;
  vi.unstubAllGlobals();
});

describe("GET /food-composition-downloads/[version]", () => {
  it("302s to the S3 object for a known version", async () => {
    const { GET } = await load();
    const res = await GET(get("v4.12"), { params: { version: "v4.12" } });
    expect(res.status).toBe(302);
    expect(res.headers.get("location")).toBe(entries[0].download_link);
  });

  it("matches with or without the leading v", async () => {
    const { GET } = await load();
    const res = await GET(get("4.12"), { params: { version: "4.12" } });
    expect(res.status).toBe(302);
  });

  it("404s for an unknown version", async () => {
    const { GET } = await load();
    const res = await GET(get("v0.0"), { params: { version: "v0.0" } });
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "unknown version" });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("404s when the manifest cannot be fetched", async () => {
    getDownloadEntries.mockRejectedValue(new Error("down"));
    const { GET } = await load();
    const res = await GET(get("v4.12"), { params: { version: "v4.12" } });
    expect(res.status).toBe(404);
  });

  it("posts a bundle_download event carrying the caller's ip and UA", async () => {
    const { GET } = await load();
    await GET(get("v4.12"), { params: { version: "v4.12" } });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(body.payload).toMatchObject({
      name: "bundle_download",
      url: "/food-composition-downloads/v4.12",
      data: { version: "v4.12", kgc_run: "20260918T100923Z", file_size: "1.2 GB" },
      ip: "203.0.113.9",
      userAgent: "Mozilla/5.0 test",
    });
  });

  it("still redirects when umami is down", async () => {
    fetchMock.mockRejectedValue(new Error("ECONNREFUSED"));
    const { GET } = await load();
    const res = await GET(get("v4.12"), { params: { version: "v4.12" } });
    expect(res.status).toBe(302);
  });
});

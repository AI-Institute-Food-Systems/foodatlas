import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// The frontend never sees the manifest here any more — it only relays the
// user's key to the API's gated endpoint and forwards its redirect.
let fetchMock: ReturnType<typeof vi.fn>;

const SIGNED = "https://bucket.s3.amazonaws.com/bundles/v4.12/f.zip?X-Amz-Signature=abc";

const load = async () => {
  vi.resetModules();
  process.env.VERCEL_ENV = "production";
  process.env.NEXT_PUBLIC_API_URL = "https://api.test";
  return await import("../[version]/route");
};

// A plain <form method="post"> submits urlencoded, which is what the
// handler's req.formData() parses. jsdom's URLSearchParams isn't the one
// undici recognises as a body, so send the encoded string + content-type
// explicitly, exactly as a browser would.
const post = (version: string, key?: string, init: RequestInit = {}) => {
  const body = new URLSearchParams();
  if (key !== undefined) body.set("key", key);
  return new Request(
    `https://foodatlas.ai/food-composition-downloads/${version}`,
    {
      method: "POST",
      body: body.toString(),
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        "x-forwarded-for": "203.0.113.9",
        "user-agent": "Mozilla/5.0 test",
        ...init.headers,
      },
    }
  );
};

const params = (version: string) => ({ params: { version } });

// fetch is called for the API hop and (separately) for the umami event;
// tell them apart by URL.
const apiCalls = () =>
  fetchMock.mock.calls.filter(([url]) => String(url).includes("/v1/bundles/"));
const umamiCalls = () =>
  fetchMock.mock.calls.filter(([url]) => String(url).includes("/api/send"));

beforeEach(() => {
  fetchMock = vi.fn(async (url: string) =>
    String(url).includes("/v1/bundles/")
      ? new Response(null, { status: 302, headers: { location: SIGNED } })
      : new Response("ok")
  );
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  delete process.env.VERCEL_ENV;
  delete process.env.NEXT_PUBLIC_API_URL;
  vi.unstubAllGlobals();
});

describe("POST /food-composition-downloads/[version]", () => {
  it("relays the key as a Bearer to the API and forwards the signed redirect", async () => {
    const { POST } = await load();
    const res = await POST(post("v4.12", "sk-test-key-1234"), params("v4.12"));
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe(SIGNED);
    const [url, init] = apiCalls()[0];
    expect(url).toBe("https://api.test/v1/bundles/v4.12/download");
    expect(init.headers.Authorization).toBe("Bearer sk-test-key-1234");
    expect(init.redirect).toBe("manual");
  });

  it("records a bundle_download with the key prefix, never the key", async () => {
    const { POST } = await load();
    await POST(post("v4.12", "sk-test-key-1234"), params("v4.12"));
    expect(umamiCalls()).toHaveLength(1);
    const body = JSON.parse(umamiCalls()[0][1].body);
    expect(body.payload.name).toBe("bundle_download");
    expect(body.payload.data).toEqual({ version: "v4.12", key_prefix: "sk-test-" });
    expect(JSON.stringify(body)).not.toContain("sk-test-key-1234");
  });

  it("bounces to the page when no key is given", async () => {
    const { POST } = await load();
    const res = await POST(post("v4.12"), params("v4.12"));
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toBe(
      "https://foodatlas.ai/food-composition-downloads?error=missing_key&version=v4.12"
    );
    expect(apiCalls()).toHaveLength(0);
  });

  it.each([
    [401, "invalid_key"],
    [403, "invalid_key"],
    [429, "rate_limited"],
    [500, "unavailable"],
  ])("maps API %s to error=%s", async (status, error) => {
    fetchMock.mockImplementation(async () => new Response(null, { status }));
    const { POST } = await load();
    const res = await POST(post("v4.12", "k"), params("v4.12"));
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toContain(`error=${error}`);
    expect(umamiCalls()).toHaveLength(0);
  });

  it("404s for an unknown version", async () => {
    fetchMock.mockImplementation(async () => new Response(null, { status: 404 }));
    const { POST } = await load();
    const res = await POST(post("v0.0", "k"), params("v0.0"));
    expect(res.status).toBe(404);
    expect(await res.json()).toEqual({ error: "unknown version" });
  });

  it("bounces when the API is unreachable", async () => {
    fetchMock.mockRejectedValue(new Error("ECONNREFUSED"));
    const { POST } = await load();
    const res = await POST(post("v4.12", "k"), params("v4.12"));
    expect(res.headers.get("location")).toContain("error=unavailable");
  });

  it("accepts the key as a Bearer header too", async () => {
    const { POST } = await load();
    const res = await POST(
      post("v4.12", undefined, { headers: { Authorization: "Bearer hdr-key" } }),
      params("v4.12")
    );
    expect(res.status).toBe(303);
    expect(apiCalls()[0][1].headers.Authorization).toBe("Bearer hdr-key");
  });
});

describe("GET /food-composition-downloads/[version]", () => {
  it("sends old links to the page with the missing-key hint", async () => {
    const { GET } = await load();
    const res = GET(
      new Request("https://foodatlas.ai/food-composition-downloads/v4.12"),
      params("v4.12")
    );
    expect(res.status).toBe(303);
    expect(res.headers.get("location")).toContain("error=missing_key");
    expect(apiCalls()).toHaveLength(0);
  });
});

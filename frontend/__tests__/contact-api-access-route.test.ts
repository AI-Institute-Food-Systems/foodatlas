// @vitest-environment node
import { NextRequest } from "next/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { parseApiAccess } from "@/utils/apiAccessGuard";

// Structured API-access answers on /contact/send: validated server-side
// (the client check is a convenience only) and appended to the email in a
// fixed, parseable block.

const { send } = vi.hoisted(() => ({ send: vi.fn() }));
vi.mock("@aws-sdk/client-sesv2", () => ({
  SESv2Client: class {
    send = send;
  },
  SendEmailCommand: class {
    constructor(public input: unknown) {}
  },
}));

import { POST } from "@/app/(everything-else)/contact/send/route";

const VALID = {
  useCategory: "Academic research",
  commercial: "No",
  dataNeeded: ["Chemicals", "Foods", "Chemicals"],
  volume: "<1k requests/day",
  projectUrl: "https://lab.example.edu",
};

const BASE = {
  name: "Ada",
  email: "ada@example.edu",
  affiliation: "UC Davis",
  topic: "API Access Request",
  message: "Building a nutrient explorer.",
  apiAccess: VALID,
};

const post = (body: unknown) =>
  POST(
    new NextRequest("http://localhost/contact/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
  );

const sentBody = (): string =>
  (
    send.mock.calls[0][0] as {
      input: { Content: { Simple: { Body: { Text: { Data: string } } } } };
    }
  ).input.Content.Simple.Body.Text.Data;

beforeEach(() => {
  send.mockReset().mockResolvedValue({});
  vi.stubEnv("CONTACT_EMAIL", '["team@example.org"]');
  vi.stubEnv("CONTACT_FROM_EMAIL", "noreply@example.org");
});

afterEach(() => {
  vi.unstubAllEnvs();
});

describe("parseApiAccess", () => {
  it("canonicalises data order and drops duplicates", () => {
    expect(parseApiAccess(VALID)?.dataNeeded).toEqual(["Foods", "Chemicals"]);
  });

  it("treats a missing project URL as empty", () => {
    const raw = { ...VALID, projectUrl: undefined };
    expect(parseApiAccess(raw)?.projectUrl).toBe("");
  });

  it.each([
    ["missing object", undefined],
    ["unknown use category", { ...VALID, useCategory: "Hobby" }],
    ["missing commercial answer", { ...VALID, commercial: undefined }],
    ["unknown volume", { ...VALID, volume: "lots" }],
    ["empty data list", { ...VALID, dataNeeded: [] }],
    ["unknown data type", { ...VALID, dataNeeded: ["Foods", "Recipes"] }],
    ["non-array data", { ...VALID, dataNeeded: "Foods" }],
    ["javascript: URL", { ...VALID, projectUrl: "javascript:alert(1)" }],
    ["unparseable URL", { ...VALID, projectUrl: "lab dot edu" }],
    ["over-long URL", { ...VALID, projectUrl: `https://x.org/${"a".repeat(200)}` }],
  ])("rejects %s", (_label, raw) => {
    expect(parseApiAccess(raw)).toBeNull();
  });
});

describe("POST /contact/send — API access", () => {
  it("appends the structured block to the email", async () => {
    const res = await post(BASE);
    expect(res.status).toBe(200);
    expect(sentBody()).toContain(
      "Building a nutrient explorer.\n\n--- API access ---\n" +
        "Use category: Academic research\n" +
        "Commercial use: No\n" +
        "Data needed: Foods, Chemicals\n" +
        "Expected volume: <1k requests/day\n" +
        "Project URL: https://lab.example.edu\n",
    );
  });

  it.each([
    ["no answers", { ...BASE, apiAccess: undefined }],
    ["an invalid answer", { ...BASE, apiAccess: { ...VALID, commercial: "Maybe" } }],
    ["no affiliation", { ...BASE, affiliation: "" }],
  ])("400s with %s and sends nothing", async (_label, body) => {
    const res = await post(body);
    expect(res.status).toBe(400);
    expect(send).not.toHaveBeenCalled();
  });

  it("leaves other topics untouched", async () => {
    const res = await post({
      ...BASE,
      topic: "General Inquiry",
      affiliation: "",
      apiAccess: undefined,
    });
    expect(res.status).toBe(200);
    expect(sentBody()).not.toContain("API access");
  });

  it("ignores stray API answers on other topics", async () => {
    const res = await post({ ...BASE, topic: "Data Issue" });
    expect(res.status).toBe(200);
    expect(sentBody()).not.toContain("--- API access ---");
  });
});

import { describe, expect, it } from "vitest";

import { buildLlmsTxt } from "@/utils/llmsTxt";

const stats = {
  foods: 1756,
  chemicals: 5369,
  diseases: 2005,
  bioactivities: 21,
  connections: 336541,
};
const latest = {
  version: "v4.12",
  release_date: "2026-09-18",
  file_size: "1.2 GB",
  kgc_run: "x",
  download_link: "",
  summary_link: "",
};

// The counts used to be hand-typed and drift with each release (G1).
describe("llms.txt", () => {
  it("states the live counts and the latest release", () => {
    const txt = buildLlmsTxt(stats, latest);
    expect(txt.startsWith("# FoodAtlas\n\n> ")).toBe(true);
    expect(txt).toContain(
      "1,756 foods, 5,369 chemicals, 2,005 diseases and 21 bioactivities, 336,541 associations"
    );
    expect(txt).toContain("Latest release: v4.12 (2026-09-18).");
  });

  it("drops the numbers, not the file, when the API is down", () => {
    const txt = buildLlmsTxt(null, null);
    expect(txt).not.toMatch(/\d,\d{3}/);
    expect(txt).not.toContain("Latest release");
    expect(txt).toContain("## Bulk download");
  });

  it("cites the canonical paper with its DOI", () => {
    const txt = buildLlmsTxt(stats, latest);
    expect(txt).toContain("*npj Science of Food*, *10*(1), 33.");
    expect(txt).toContain("https://doi.org/10.1038/s41538-025-00680-9");
  });
});

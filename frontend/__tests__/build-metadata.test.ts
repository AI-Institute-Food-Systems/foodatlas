import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  ENTITY_TYPES,
  MAX_DESCRIPTION,
  MAX_TITLE,
  OG_IMAGE,
  SITE_NAME,
  TITLE_SEPARATOR,
  buildMetadata,
  fitDescription,
  fitTitle,
} from "@/utils/site";

const read = (p: string) => readFileSync(join(process.cwd(), p), "utf8");

// The audit found no og:* or twitter:* tag on any page (D1/D2). Next
// replaces a parent's openGraph block instead of merging it, so the block is
// built per page by one helper, and these pin what the helper emits.
describe("buildMetadata", () => {
  const meta = buildMetadata({
    title: "Apple: Food Composition",
    description: "What apples contain, traced to sources.",
    path: "/food/apple",
    jsonAlternate: "https://api.foodatlas.ai/v1/foods/e1",
  });

  it("emits title, description and canonical", () => {
    expect(meta.title).toBe("Apple: Food Composition");
    expect(meta.description).toBe("What apples contain, traced to sources.");
    expect(meta.alternates?.canonical).toBe("/food/apple");
    expect(meta.alternates?.types).toEqual({
      "application/json": "https://api.foodatlas.ai/v1/foods/e1",
    });
  });

  it("emits a full openGraph block with the site card", () => {
    expect(meta.openGraph).toMatchObject({
      siteName: SITE_NAME,
      url: "/food/apple",
      title: `Apple: Food Composition${TITLE_SEPARATOR}${SITE_NAME}`,
      description: "What apples contain, traced to sources.",
      images: [OG_IMAGE],
    });
  });

  it("emits a summary_large_image Twitter card", () => {
    expect(meta.twitter).toMatchObject({
      card: "summary_large_image",
      images: [OG_IMAGE.url],
    });
  });

  it("can opt out of the title template", () => {
    const home = buildMetadata({
      title: "FoodAtlas | Home",
      description: "x",
      path: "/",
      absoluteTitle: true,
    });
    expect(home.title).toEqual({ absolute: "FoodAtlas | Home" });
    expect(home.openGraph?.title).toBe("FoodAtlas | Home");
  });

  it("is used by every entity page", () => {
    for (const type of ENTITY_TYPES) {
      const src = read(`app/(everything-else)/${type}/[slug]/page.tsx`);
      expect(src, type).toContain("buildMetadata({");
      expect(src, type).toContain("fitTitle(");
    }
  });

  it("serves the card the pages name", () => {
    const src = read("app/opengraph-image.tsx");
    expect(src).toContain("OG_IMAGE.width");
    expect(OG_IMAGE).toMatchObject({ width: 1200, height: 630 });
  });
});

describe("fitTitle", () => {
  const branded = (t: string) => `${t}${TITLE_SEPARATOR}${SITE_NAME}`;

  it("leaves a short name alone", () => {
    expect(fitTitle("Apple", " in Foods")).toBe("Apple in Foods");
  });

  it("keeps a long chemical name within the limit, suffix intact", () => {
    const iupac =
      "(3r,5s)-7-[4-(4-fluorophenyl)-2-[methyl(methylsulfonyl)amino]-6-propan-2-yl-5-pyrimidinyl]-3,5-dihydroxy-6-heptenoic acid";
    const t = fitTitle(iupac, " in Foods");
    expect(branded(t).length).toBeLessThanOrEqual(MAX_TITLE);
    expect(t.endsWith("… in Foods")).toBe(true);
  });

  it("cuts at a word boundary when there is one", () => {
    const t = fitTitle(
      "Anemia, Nonspherocytic Hemolytic, Due To G6pd Deficiency",
      ": Chemical Associations"
    );
    expect(branded(t).length).toBeLessThanOrEqual(MAX_TITLE);
    expect(t).toMatch(/^Anemia, Nonspherocytic…: Chemical Associations$/);
  });
});

describe("description templates", () => {
  it("cut a long description at a word, within 160 characters", () => {
    const long = `Chemicals linked to ${"Very Long Disease Name ".repeat(8)}by evidence.`;
    const d = fitDescription(long);
    expect(d.length).toBeLessThanOrEqual(MAX_DESCRIPTION);
    expect(d.endsWith("…")).toBe(true);
    expect(fitDescription("Short and fine.")).toBe("Short and fine.");
  });

  it("do not say foods contain a disease", () => {
    const src = read("app/(everything-else)/disease/[slug]/page.tsx");
    expect(src).not.toContain("foods that contain it");
  });

  it("go through fitDescription on every entity page", () => {
    for (const type of ENTITY_TYPES) {
      const src = read(`app/(everything-else)/${type}/[slug]/page.tsx`);
      expect(src, type).toContain("fitDescription(");
    }
  });
});

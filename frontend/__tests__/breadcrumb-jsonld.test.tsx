import { readFileSync } from "node:fs";
import { join } from "node:path";

import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({ usePathname: () => "/about" }));

import Breadcrumb from "@/components/navigation/Breadcrumb";
import { ENTITY_TYPES } from "@/utils/site";
import {
  entityBreadcrumbJsonLd,
  pageBreadcrumbJsonLd,
} from "@/utils/structuredData";

const read = (p: string) => readFileSync(join(process.cwd(), p), "utf8");

// No page had BreadcrumbList JSON-LD (E3). Static pages get it from the
// layout's Breadcrumb, which renders on the server; entity pages emit it.
describe("BreadcrumbList", () => {
  it("is Home › page for a static page", () => {
    const b = pageBreadcrumbJsonLd("/about", "About");
    expect(b["@type"]).toBe("BreadcrumbList");
    expect(b.itemListElement).toEqual([
      { "@type": "ListItem", position: 1, name: "Home", item: "https://www.foodatlas.ai/" },
      { "@type": "ListItem", position: 2, name: "About", item: "https://www.foodatlas.ai/about" },
    ]);
  });

  it("ends an entity trail at the canonical URL", () => {
    const b = entityBreadcrumbJsonLd("food", "Cow Milk", "cow milk");
    expect(b.itemListElement[1]).toMatchObject({
      name: "Cow Milk",
      item: "https://www.foodatlas.ai/food/cow--milk",
    });
  });

  it("is rendered by the static-page Breadcrumb", () => {
    const { container } = render(<Breadcrumb />);
    const script = container.querySelector('script[type="application/ld+json"]');
    expect(script).not.toBeNull();
    expect(JSON.parse(script!.innerHTML)["@type"]).toBe("BreadcrumbList");
  });

  it("is emitted by every entity page", () => {
    for (const type of ENTITY_TYPES) {
      const src = read(`app/(everything-else)/${type}/[slug]/page.tsx`);
      expect(src, type).toContain("entityBreadcrumbJsonLd(");
    }
  });
});

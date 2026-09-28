import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import type { Metadata } from "@/types/Metadata";
import { ENTITY_TYPES } from "@/utils/site";
import { entityJsonLd } from "@/utils/structuredData";

// Fixtures are trimmed copies of live /{type}/metadata responses, junk included:
// the empty scientific_name, the name repeated in synonyms, the IRI synonym,
// the empty-string url.
const quercetin: Metadata = {
  id: "e60502",
  entity_type: "chemical",
  common_name: "quercetin",
  scientific_name: "",
  synonyms: ["quercetin", "sophoretin", "xanthaurine", "sophoretin"],
  external_ids: {
    chebi: {
      display_name: "ChEBI",
      ids: [
        {
          id: "16243",
          url: "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:16243",
        },
      ],
    },
    fdc_nutrient: {
      display_name: "FDC Nutrient",
      ids: [{ id: "1391", url: "" }],
    },
  },
};

const apple: Metadata = {
  id: "e897",
  entity_type: "food",
  common_name: "apple",
  scientific_name: "",
  synonyms: ["Apple", "<http://purl.obolibrary.org/obo/ncbitaxon_3750>"],
  external_ids: {
    foodon: {
      display_name: "FoodOn",
      ids: [
        {
          id: "http://purl.obolibrary.org/obo/FOODON_00002473",
          url: "http://purl.obolibrary.org/obo/FOODON_00002473",
        },
      ],
    },
  },
};

describe("entityJsonLd", () => {
  it("anchors the entity to its canonical URL", () => {
    const d = entityJsonLd("chemical", quercetin);
    expect(d["@type"]).toBe("MolecularEntity");
    expect(d.url).toBe("https://www.foodatlas.ai/chemical/quercetin");
    expect(d["@id"]).toBe("https://www.foodatlas.ai/chemical/quercetin#entity");
    expect(d.name).toBe("quercetin");
  });

  it("drops the name itself and duplicates from alternateName", () => {
    expect(entityJsonLd("chemical", quercetin).alternateName).toEqual([
      "sophoretin",
      "xanthaurine",
    ]);
  });

  it("drops IRI synonyms, and omits alternateName when nothing is left", () => {
    expect(entityJsonLd("food", apple)).not.toHaveProperty("alternateName");
  });

  it("lists every external id but only real links in sameAs", () => {
    const d = entityJsonLd("chemical", quercetin);
    expect(d.identifier).toEqual([
      { "@type": "PropertyValue", propertyID: "FoodAtlas", value: "e60502" },
      { "@type": "PropertyValue", propertyID: "ChEBI", value: "16243" },
      { "@type": "PropertyValue", propertyID: "FDC Nutrient", value: "1391" },
    ]);
    expect(d.sameAs).toEqual([
      "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:16243",
    ]);
  });

  it("makes foods terms in a FoodAtlas vocabulary", () => {
    const d = entityJsonLd("food", apple);
    expect(d["@type"]).toBe("DefinedTerm");
    expect(d.termCode).toBe("e897");
    expect(d.inDefinedTermSet).toMatchObject({ "@type": "DefinedTermSet" });
  });

  it("omits sameAs when no id has a URL (flat bioactivity shape)", () => {
    const d = entityJsonLd("bioactivity", {
      id: "e5",
      // Live /bioactivity/metadata omits entity_type; the route supplies it.
      entity_type: undefined as unknown as "bioactivity",
      common_name: "cytotoxicity",
      scientific_name: null,
      synonyms: [],
      external_ids: {
        chembl: { display_name: "Chembl", ids: [{ id: "X1", url: null }] },
      },
    });
    expect(d).not.toHaveProperty("sameAs");
    expect(d).not.toHaveProperty("description");
    expect(d.identifier).toHaveLength(2);
    expect(d.url).toBe("https://www.foodatlas.ai/bioactivity/cytotoxicity");
  });

  it("types diseases as MedicalCondition without the term-set fields", () => {
    const d = entityJsonLd("disease", apple);
    expect(d["@type"]).toBe("MedicalCondition");
    expect(d).not.toHaveProperty("termCode");
  });
});

describe("entity pages", () => {
  it.each(ENTITY_TYPES)("%s page renders the entity JSON-LD", (type) => {
    const src = readFileSync(
      join(process.cwd(), `app/(everything-else)/${type}/[slug]/page.tsx`),
      "utf8"
    );
    expect(src).toContain("<JsonLd data={entityJsonLd(entityType, metaPayload)} />");
  });
});

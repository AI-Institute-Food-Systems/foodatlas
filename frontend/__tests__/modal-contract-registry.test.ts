// Every modal is either covered by a page-filter contract test or excluded
// with a reason. A new evidence modal that ignores the page's filters is how
// the button-vs-modal count mismatches shipped; this file fails until the
// new modal is registered below.
//
// Rules a registered modal's contract test must check (filter-consistency
// rules, 2026-11-18): the modal opens on the page's active filters, offers
// every page filter dimension, stays editable, and its total equals the
// row's count button.

import fs from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

const ROOT = path.resolve(__dirname, "..");

// modal file → its contract test, and every file that opens it mapped to
// the component name that test must mount to drive that opener.
const CONTRACTS: Record<string, { test: string; openers: Record<string, string> }> = {
  "components/entities/bioactivity/BioactivityMeasurementsModal.tsx": {
    test: "__tests__/bioactivity-modal-wiring.test.tsx",
    openers: {
      "components/entities/bioactivity/BioactivityTable.tsx": "BioactivityChemicalsSection",
      "components/entities/bioactivity/FoodInferredBioactivitiesSection.tsx":
        "FoodInferredBioactivitiesSection",
    },
  },
  "components/entities/food/FoodCompositionEvidenceModal.tsx": {
    test: "__tests__/composition-modal-contract.test.tsx",
    openers: {
      "components/entities/food/FoodCompositionSection.tsx": "FoodCompositionSection",
      "components/entities/chemical/ChemicalCompositionTable.tsx": "ChemicalCompositionTable",
    },
  },
  "components/entities/CorrelationEvidenceModal.tsx": {
    test: "__tests__/health-filter-contract.test.tsx",
    openers: { "components/entities/CorrelationTable.tsx": "CorrelationTable" },
  },
  "components/entities/shared/AssayDetailModals.tsx": {
    test: "__tests__/health-filter-contract.test.tsx",
    openers: {
      "components/entities/shared/AssayFilteredModal.tsx": "AssayInferredAssociationsTable",
      // The bioactivity page's Diseases tab has no Signal/Activity filters.
      "components/entities/bioactivity/BioactivityDiseasesSection.tsx": "",
      "components/entities/shared/AssayInferredRow.tsx": "",
    },
  },
};

// modal file → why it has no page filters to carry
const EXCLUDED: Record<string, string> = {
  "components/basic/ReportIssueModal.tsx": "a form, not an evidence list",
  "components/contact/ApiTermsModal.tsx": "static terms text",
  "components/entities/EntityAmbiguityBadge.tsx":
    "lists name-sibling entities; opened from the page header, not a filtered row",
  "components/entities/TaxonomyTree.tsx": "taxonomy navigation, no table filters",
  "components/entities/food/Synonyms.tsx": "the entity's synonym list, no table filters",
};

const walk = (dir: string): string[] =>
  fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) return walk(full);
    return /\.tsx$/.test(e.name) ? [full] : [];
  });

const modalFiles = (): string[] =>
  ["components", "app"]
    .flatMap((d) => walk(path.join(ROOT, d)))
    .filter((f) => /<Modal\b/.test(fs.readFileSync(f, "utf8")))
    .map((f) => path.relative(ROOT, f));

describe("modal contract registry", () => {
  it("registers or excludes every modal", () => {
    const missing = modalFiles().filter((f) => !(f in CONTRACTS) && !(f in EXCLUDED));
    expect(
      missing,
      `register these in CONTRACTS with a test that opens them from their ` +
        `tables under page filters, or in EXCLUDED with a reason`
    ).toEqual([]);
  });

  it("lists every opener of a registered modal, and its test mounts each", () => {
    const sources = ["components", "app"].flatMap((d) => walk(path.join(ROOT, d)));
    for (const [modal, { test, openers }] of Object.entries(CONTRACTS)) {
      const name = path.basename(modal, ".tsx");
      const importers = sources
        .map((f) => path.relative(ROOT, f))
        .filter(
          (f) =>
            f !== modal &&
            new RegExp(`from "@/components/[\\w/]*${name}"`).test(
              fs.readFileSync(path.join(ROOT, f), "utf8")
            )
        );
      expect(importers.filter((f) => !(f in openers)), `unregistered openers of ${name}`).toEqual(
        []
      );
      const file = path.join(ROOT, test);
      expect(fs.existsSync(file), test).toBe(true);
      const src = fs.readFileSync(file, "utf8");
      for (const [opener, mounted] of Object.entries(openers)) {
        if (mounted) expect(src.includes(mounted), `${test} must mount ${mounted} (${opener})`).toBe(true);
      }
    }
  });

  it("has no stale entries", () => {
    const present = new Set(modalFiles());
    const stale = [...Object.keys(CONTRACTS), ...Object.keys(EXCLUDED)].filter(
      (f) => !present.has(f)
    );
    expect(stale).toEqual([]);
  });
});

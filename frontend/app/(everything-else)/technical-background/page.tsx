import Image from "next/image";
import { Metadata } from "next/types";

import Card from "@/components/basic/Card";
import Code from "@/components/basic/Code";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { CANONICAL_PUBLICATION, doiUrl } from "@/utils/publications";

export const metadata: Metadata = {
  title: "Background | How the FoodAtlas Knowledge Graph is Sourced",
  description:
    "How FoodAtlas turns peer-reviewed literature and public databases into a knowledge graph of foods, chemicals, diseases, and bioactivities.",
};

// Source families integrated by the KGC ingest stage. Kept as data so
// the "Sources" section stays visually consistent as the list grows.
const SOURCES: {
  name: string;
  domain: string;
  href: string;
}[] = [
  { name: "FoodOn", domain: "Food ontology", href: "https://foodon.org/" },
  {
    name: "ChEBI",
    domain: "Chemical ontology",
    href: "https://www.ebi.ac.uk/chebi/",
  },
  {
    name: "CDNO",
    domain: "Compositional / dietary nutrients",
    href: "https://obofoundry.org/ontology/cdno.html",
  },
  {
    name: "USDA FDC",
    domain: "Food composition data",
    href: "https://fdc.nal.usda.gov/",
  },
  {
    name: "CTD",
    domain: "Comparative Toxicogenomics",
    href: "https://ctdbase.org/",
  },
  { name: "MeSH", domain: "Disease ontology", href: "https://www.nlm.nih.gov/mesh/" },
  {
    name: "PubChem",
    domain: "Chemical + bioassay data",
    href: "https://pubchem.ncbi.nlm.nih.gov/",
  },
  {
    name: "ChEMBL",
    domain: "Bioactivity assays",
    href: "https://www.ebi.ac.uk/chembl/",
  },
  { name: "FlavorDB", domain: "Flavor descriptors", href: "https://cosylab.iiitd.edu.in/flavordb/" },
];

// One entry per typed edge in the graph. Rendered as a small table so
// each edge's meaning is scannable.
const RELATIONS: {
  code: string;
  label: string;
  from: string;
  to: string;
  description: string;
}[] = [
  {
    code: "r1",
    label: "CONTAINS",
    from: "Food",
    to: "Chemical",
    description:
      "A food contains a chemical at a measured concentration. Values normalised to mg / 100g where the source unit allows.",
  },
  {
    code: "r2",
    label: "IS_A",
    from: "Any",
    to: "Any (same type)",
    description:
      "Ontology hierarchy. Ties into FoodOn, ChEBI, MeSH, and the bioactivity hierarchy so children inherit parent context.",
  },
  {
    code: "r3",
    label: "WORSENS",
    from: "Chemical",
    to: "Disease",
    description:
      "Peer-reviewed evidence that the chemical worsens the disease's health outcomes or increases risk of onset.",
  },
  {
    code: "r4",
    label: "IMPROVES",
    from: "Chemical",
    to: "Disease",
    description:
      "Peer-reviewed evidence that the chemical improves the disease's health outcomes or reduces risk of onset.",
  },
  {
    code: "r5",
    label: "EXHIBITS",
    from: "Food",
    to: "Bioactivity",
    description:
      "A food exhibits a bioactivity — either from direct assay evidence or from a model inference over its composition.",
  },
  {
    code: "r6",
    label: "MEASURED",
    from: "Chemical",
    to: "Bioactivity",
    description:
      "A chemical was measured against a bioactivity in an assay. Carries the raw measurement + a Hill-curve fit when the assay reports dose–response.",
  },
];

// The two pipelines feed into the same graph; presented side-by-side so
// the extraction ↔ construction split reads clearly.
const IE_STAGES = [
  { title: "Corpus", text: "Refresh a local BioC-PMC corpus and rebuild the PMC-ID index." },
  { title: "Search", text: "Query PubMed for food terms; retrieve and fuzzy-match candidate sentences." },
  { title: "Filter", text: "Binary classification with a fine-tuned BioBERT model — only sentences above a 0.99 confidence threshold pass through." },
  { title: "Extract", text: "The remaining sentences are batched through OpenAI's Batch API using gpt-5.5 and structured extraction prompts." },
];

const KGC_STAGES = [
  { title: "Ingest", text: "Parse each source into standardised parquet files (nodes / edges / cross-references)." },
  { title: "Entities", text: "Three-pass entity resolution → stable `foodatlas_id`s that survive across releases. Ambiguities are recorded on attestations, not silently collapsed." },
  { title: "Triplets", text: "Build typed edges (r1–r6) from source data. Duplicates merge; ambiguous resolutions explode into candidates for later review." },
  { title: "Extraction fold-in", text: "Concentration parser normalises the information extraction pipeline's output to mg/100g. Chemical + food names resolved through the entity registry." },
  { title: "Enrichment", text: "Add derived metadata: chemical/food classifications, flavor descriptors, common names, display grouping." },
  { title: "Trust", text: "Per-attestation plausibility signals from a Gemini 3.1 Flash-Lite LLM judge. Emits a 0–1 score and a short justification per triplet." },
  { title: "Evaluation", text: "Diagnostics on the finished graph — orphan detection, unclassified entities, per-source coverage." },
];

const TechnicalBackground = () => {
  return (
    <div>
      {/* Intro — what FoodAtlas is, one paragraph, no stale specifics. */}
      <div>
        <Heading type="h1" variant="display">
          Technical Background
        </Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          <i>FoodAtlas</i> is an evidence-based knowledge graph linking foods,
          the chemicals they contain, the diseases those chemicals correlate
          with, and the bioactivities they express. Every edge in the graph
          is traceable to a public source or a peer-reviewed publication.
        </p>
        <p className="mt-3 text-base leading-relaxed text-light-200">
          The graph is built from two pipelines: an{" "}
          <b>information extraction</b> pipeline that pulls
          food–chemical relations from PubMed / PMC literature, and a{" "}
          <b>knowledge graph construction</b> pipeline that ingests
          public databases, resolves entities, and stitches everything into
          the released graph. For the full study, see the{" "}
          <Link
            className="whitespace-nowrap"
            href={doiUrl(CANONICAL_PUBLICATION.doi)}
          >
            npj Science of Food paper
          </Link>
          .
        </p>
      </div>

      {/* What's in the graph — nodes + relations table. */}
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          What&apos;s in the graph
        </Heading>
        {/* Top row: diagram (left) + expanded description (right).
         * Bottom row: compact legend of typed edges. */}
        <div className="mt-6 flex flex-col md:flex-row gap-8 items-start">
          <div className="relative w-full md:w-1/2 h-56 md:h-80 shrink-0">
            <Image
              className="object-contain"
              fill
              src="/images/kg_semantics.svg"
              alt="Diagram of the FoodAtlas graph semantics: Food, Chemical, Disease, and Bioactivity nodes connected by CONTAINS, IS_A, WORSENS, IMPROVES, EXHIBITS, and MEASURED edges."
            />
          </div>
          <div className="md:w-1/2 flex flex-col gap-4 leading-relaxed text-light-300">
            <p>
              A <b>node</b> is a <Code>Food</Code>, <Code>Chemical</Code>,{" "}
              <Code>Disease</Code>, or <Code>Bioactivity</Code>. An{" "}
              <b>edge</b> informs about the relationship between two nodes.
            </p>
            <p>
              Every edge carries an <i>attestation</i> — the supporting
              evidence, source, and any measurement metadata (concentration
              values, assay outcomes, Hill-curve fits).
            </p>
          </div>
        </div>

        <Card className="mt-6">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-light-700">
                <th className="py-2 pr-3 text-left font-mono italic text-[11px] uppercase tracking-wider text-light-500 font-medium whitespace-nowrap">
                  Code
                </th>
                <th className="py-2 pr-3 text-left font-mono italic text-[11px] uppercase tracking-wider text-light-500 font-medium whitespace-nowrap">
                  Relation
                </th>
                <th className="py-2 pr-3 text-left font-mono italic text-[11px] uppercase tracking-wider text-light-500 font-medium whitespace-nowrap">
                  Between
                </th>
                <th className="py-2 text-left font-mono italic text-[11px] uppercase tracking-wider text-light-500 font-medium">
                  Description
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-light-800">
              {RELATIONS.map((r) => (
                <tr key={r.code}>
                  <td className="align-baseline py-1.5 pr-3 font-mono text-xs text-accent-300 whitespace-nowrap">
                    {r.code}
                  </td>
                  <td className="align-baseline py-1.5 pr-3 font-mono text-light-100 whitespace-nowrap">
                    {r.label}
                  </td>
                  <td className="align-baseline py-1.5 pr-3 font-mono text-xs text-light-400 whitespace-nowrap">
                    {r.from} → {r.to}
                  </td>
                  <td className="align-baseline py-1.5 text-light-400 font-light leading-snug">
                    {r.description}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </div>

      {/* Where the data comes from — the ingested sources + PubMed. */}
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Where the data comes from
        </Heading>
        <Card className="mt-6">
          <p className="leading-relaxed text-light-300">
            The knowledge graph integrates <b>nine public sources</b> spanning
            ontologies, composition tables, and bioassay repositories. The
            information extraction pipeline layers additional food–chemical
            relations on top by reading peer-reviewed literature from{" "}
            <Link href="https://pubmed.ncbi.nlm.nih.gov/" isExternal>
              PubMed
            </Link>{" "}
            /{" "}
            <Link href="https://pmc.ncbi.nlm.nih.gov/" isExternal>
              PMC
            </Link>
            .
          </p>
          <ul className="mt-5 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-2 text-sm">
            {SOURCES.map((s) => (
              <li key={s.name} className="flex items-baseline gap-2">
                <Link href={s.href} isExternal>
                  {s.name}
                </Link>
                <span className="text-light-500 font-light">— {s.domain}</span>
              </li>
            ))}
          </ul>
        </Card>
      </div>

      {/* How it's built — the two pipelines side by side. */}
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          How it&apos;s built
        </Heading>
        <p className="mt-6 text-base leading-relaxed text-light-300 font-light">
          Our pipeline uses state-of-the-art AI models to extract and
          quantify food connections. The two major steps are (a){" "}
          <b>knowledge extraction</b>, i.e., converting literature into
          food–chemical relations, and (b) <b>knowledge graph construction</b>,
          which adds meta-information and new information to our knowledge
          base.
        </p>

        {/* Step 1: Information Extraction — full width horizontal flow. */}
        <div className="mt-8">
          <Heading type="h3" variant="chip">
            Step 1 — Information Extraction
          </Heading>
          <p className="mt-4 text-sm leading-relaxed text-light-400 font-light">
            From literature to structured triplets.
          </p>
          <div className="relative mt-10 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-y-14 gap-x-10">
            {IE_STAGES.map((s, i) => (
              <div key={s.title} className="relative">
                <div className="flex gap-3">
                  <span className="mt-0.5 flex-shrink-0 w-6 h-6 flex items-center justify-center rounded-full bg-accent-400 text-light-1000 font-mono text-xs font-semibold">
                    {i + 1}
                  </span>
                  <div>
                    <div className="font-mono italic text-light-100">
                      {s.title}
                    </div>
                    <p className="mt-3 font-extralight leading-relaxed text-light-300 text-sm">
                      {s.text}
                    </p>
                  </div>
                </div>
                {/* Arrow before each step (except the first). Vertical
                 * (rotated 90°) below md; horizontal on md+. */}
                {i > 0 && (
                  <span className="absolute -top-12 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-8 md:translate-x-0 text-3xl text-light-400 font-mono pointer-events-none">
                    &#8674;
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>

        {/* Step 2: Knowledge Graph Construction — full width, 7 stages. */}
        <div className="mt-16">
          <Heading type="h3" variant="chip">
            Step 2 — Knowledge Graph Construction
          </Heading>
          <p className="mt-4 text-sm leading-relaxed text-light-400 font-light">
            From sources + triplets to the released graph.
          </p>
          <div className="relative mt-10 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-y-14 gap-x-10">
            {KGC_STAGES.map((s, i) => (
              <div key={s.title} className="relative">
                <div className="flex gap-3">
                  <span className="mt-0.5 flex-shrink-0 w-6 h-6 flex items-center justify-center rounded-full bg-accent-400 text-light-1000 font-mono text-xs font-semibold">
                    {i + 1}
                  </span>
                  <div>
                    <div className="font-mono italic text-light-100">
                      {s.title}
                    </div>
                    <p className="mt-3 font-extralight leading-relaxed text-light-300 text-sm">
                      {s.text}
                    </p>
                  </div>
                </div>
                {i > 0 && (
                  <span className="absolute -top-12 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-8 md:translate-x-0 text-3xl text-light-400 font-mono pointer-events-none">
                    &#8674;
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Bioactivity — added late 2026-06, worth a dedicated callout. */}
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Bioactivity in detail
        </Heading>
        <Card className="mt-6">
          <p className="leading-relaxed text-light-300">
            A <i>bioactivity</i> is a biological effect a chemical or food can
            exhibit — antibacterial, antioxidant, hepatotoxic, and so on. The
            graph organises 21 bioactivity concepts in a small hierarchy so
            queries roll up naturally (e.g. an <i>antibacterial</i> hit is
            also an <i>antimicrobial</i> hit).
          </p>
          <p className="mt-4 leading-relaxed text-light-300">
            Chemical → bioactivity evidence (<Code>r6</Code>){" "}
            comes in two flavors:
          </p>
          <ul className="mt-3 flex flex-col gap-2 text-light-300">
            <li>
              <b>Experimental</b> — assay measurements pulled from{" "}
              <Link href="https://pubchem.ncbi.nlm.nih.gov/" isExternal>
                PubChem
              </Link>{" "}
              and{" "}
              <Link href="https://www.ebi.ac.uk/chembl/" isExternal>
                ChEMBL
              </Link>
              , with outcome, potency (value + unit), and a four-parameter
              Hill-curve fit when the assay reports a dose response.
            </li>
            <li>
              <b>Predicted</b> — random-forest model inferences (e.g.{" "}
              <Code>RF_antioxidant_v1</Code>) that score a chemical against a
              bioactivity based on structural features.
            </li>
          </ul>
          <p className="mt-4 leading-relaxed text-light-300">
            Food → bioactivity evidence (<Code>r5</Code>){" "}
            is either directly measured against the food or inferred from the
            bioactivities of its composed chemicals.
          </p>
        </Card>
      </div>

      {/* Trust — surfaced on every attestation in the UI as a chip. */}
      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Trust signals
        </Heading>
        <Card className="mt-6">
          <p className="leading-relaxed text-light-300">
            Every attestation in the graph carries a per-triplet{" "}
            <b>trust signal</b>. A Gemini 3.1 Flash-Lite LLM judge scores each{" "}
            <Code>(food, chemical, concentration)</Code>{" "}
            triplet on world-knowledge plausibility from 0 to 1, alongside a
            short justification. Low-trust rows aren&apos;t hidden — they&apos;re
            surfaced on the frontend so a curator can review them without
            losing the underlying evidence.
          </p>
          <p className="mt-4 leading-relaxed text-light-300">
            Trust signals are versioned and stored separately from the main
            attestations, so a rebuild of the graph doesn&apos;t lose the
            per-row judgements that were made against an earlier snapshot.
          </p>
        </Card>
      </div>
    </div>
  );
};

export default TechnicalBackground;

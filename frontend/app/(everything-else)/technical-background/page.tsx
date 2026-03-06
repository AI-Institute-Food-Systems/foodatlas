import Image from "next/image";
import { Metadata } from "next/types";

import Link from "@/components/basic/Link";
import Code from "@/components/basic/Code";
import Card from "@/components/basic/Card";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";

export const metadata: Metadata = {
  title: "Background | How the FoodAtlas Knowledge Based is Sourced",
  description:
    "Where does the FoodAtlas data come from? Learn how we control our sources and pipeline to ensure we present reliable data.",
};

const pipeline = [
  {
    title: "Filter Documents",
    text: "Relevant, peer-reviewed literature is filtered using a list of more than 1,200 keywords",
  },
  {
    title: "Predict Relevant Sentences",
    text: (
      <>
        Sentences likely to contain food information are predicted using{" "}
        <Link
          href="https://hpc.nih.gov/apps/BioBERT.html#:~:text=BioBERT%20is%20a%20biomedical%20language,extraction%2C%20question%20answering%2C%20etc."
          isExternal
        >
          BioBERT
        </Link>
      </>
    ),
  },
  {
    title: "Extract Relations",
    text: (
      <>
        Sentences are processed by{" "}
        <Link href="https://openai.com/research/gpt-4" isExternal>
          GPT-4
        </Link>{" "}
        to extract food-chemical relations
      </>
    ),
  },
  {
    title: "Data Conversion",
    text: "Output is converted into triplets, the building block of the knowledge graph data structure",
  },
  {
    title: "Entity Linking",
    text: "Triplets are linked to existing corresponding entities, or new ones are created",
  },
  {
    title: "Metadata Injection",
    text: "Metadata such as concentration values, food parts, external references, and quality scores is compiled",
  },
];

const TechnicalBackground = () => {
  return (
    <div>
      {/* heading & caption */}
      <div>
        <Heading type="h1">Technical Background</Heading>
        <SubHeading>
          <i>FoodAtlas</i> behind the Scenes
        </SubHeading>
        <p className="mt-10 text-lg leading-loose text-light-200">
          <i>FoodAtlas</i> is an AI-powered tool that maps the complex
          relationships between food, chemicals, and diseases. It not only
          identifies the types and quantities of chemicals in the foods we
          consume but also explores their potential health impacts.
        </p>
        <p className="mt-2.5 text-lg leading-loose text-light-200">
          Our system continuously monitors new research, extracting data on
          chemical concentrations and disease correlations. This data is
          cross-referenced with established databases, such as{" "}
          <Link
            className="flex-nowrap"
            href={"https://pubchem.ncbi.nlm.nih.gov"}
          >
            PubChem
          </Link>
          , and incorporated into our knowledge graph.
        </p>
        <p className="mt-2.5 text-lg leading-loose text-light-200">
          The following provides a brief overview of some of the methods and
          technologies used. For a detailed look behind the scenes, refer to our
          first{" "}
          <Link
            className="whitespace-nowrap"
            href={
              "https://drive.google.com/file/d/1e9hfCT3Og-Mvsch5Rse0oNq_V4ua_mKo/view?usp=sharing"
            }
          >
            publication
          </Link>
          .
        </p>
      </div>
      <Divider />
      {/* knowledge graph */}
      <div className="flex flex-col md:flex-row gap-2">
        {/* image */}
        <div className="relative h-52 md:h-96 m-8 md:w-2/3">
          <Image
            className="object-contain"
            fill
            src="/images/kg.webp"
            alt="An example image of a knowledge graph. The graph contains an enourmous amount of nodes and edges connecting nodes. Some nodes are highlighted, such as Soybean, Cow, or Tomato. A portion of the image is magnified to illustrate the connections between Garlic, Garlic root, Allicin, and Saponins."
          />
        </div>
        {/* info card */}
        <div className="md:w-1/3">
          <Card>
            <Heading type="h2" className="text-3xl">
              Knowledge Graph
            </Heading>
            <p className="mt-4 leading-loose text-light-300">
              <i>FoodAtlas</i> uses a{" "}
              <Link href="https://en.wikipedia.org/wiki/Knowledge_graph">
                knowledge graph
              </Link>{" "}
              to systematically store and organize a vast network of
              interconnected entities, including foods, chemicals, diseases, and
              their relationships. Each connection is represented as a{" "}
              <i>triplet</i>&ndash;a structured entry in our knowledge
              base&ndash;consisting of a head entity, a tail entity, and their
              relationship. To enhance the reliability of food-related insights,{" "}
              <i>FoodAtlas</i> also incorporates rich metadata, including
              detailed entity information and supporting evidence for each
              relationship.
            </p>
          </Card>
        </div>
      </div>
      {/* semantics */}
      <div className="mt-36 flex flex-col-reverse md:flex-row gap-2">
        <div className="md:w-5/12">
          <Card>
            <Heading type="h2" className="text-3xl">
              Graph Semantics
            </Heading>
            <p className="mt-4 leading-loose text-light-300">
              A <b>node</b> is either a <Code>Food</Code>, a
              <Code>Chemical</Code>, or a <Code>Disease</Code>.
              <br />
              <br />
              An <b>edge</b> informs on the relationship between two nodes.{" "}
              <i>FoodAtlas</i> captures <Code>contains</Code>
              relations, i.e. what chemicals are found in certain foods as well
              as <Code>is a</Code> relations for parts of foods. Chemicals may
              then either <Code>improve</Code> or <Code>worsen</Code> a disease.
            </p>
          </Card>
        </div>
        <div className="relative h-96 m-8 md:w-7/12">
          <Image
            className="object-contain"
            fill
            src="/images/kg_semantics.svg"
            alt="A graphic illustrating the semantic relationships of a knowledge graph. Three nodes, including food, chemical, and disease are shown, connected through edges. Those edges include all possible relations of two nodes, including a self-referencing 'is-a' relation which are possible on both foods and chemicals, a 'contains' relation between a food and a chemical, as well as a 'positively / negatively correlates' relation from chemical to disease and from food to disease. The latter is dashed to indicate it's work in progress."
          />
        </div>
      </div>
      <Divider />
      {/* pipeline */}
      <div className="">
        <Heading type="h2" className="text-3xl">
          Pipeline
        </Heading>
        <p className="mt-4 text-lg leading-loose text-light-300 font-light">
          Our pipeline uses state-of-the-art AI models to extract and quantify
          food connections. The two major steps are{" "}
          <i>(a) knowledge extraction</i>, i.e., converting literature into
          food-chemical relations and <i>(b) knowledge graph construction</i> ,
          which adds metainformation and new information to our knowledge base.
        </p>
        <div className="mt-20">
          <Heading type="h3" variant="boxed">
            Knowledge Extraction
          </Heading>
          <div className="relative mt-10 grid grid-cols-1 md:grid-cols-3 gap-y-16 gap-x-10">
            {pipeline.slice(0, 3).map((step: any, index: number) => (
              <div key={index + " " + step.title} className="relative">
                <div className="flex gap-3">
                  <div className="bg-accent-600/80 w-6 h-6 flex items-center justify-center rounded-full flex-shrink-0 font-mono text-xs">
                    <span>{index + 1}</span>
                  </div>
                  <div>
                    <span className="font-mono italic">{step.title}</span>
                    <p className="mt-4 font-extralight leading-relaxed text-light-300">
                      {step.text}
                    </p>
                  </div>
                </div>
                {index >= 0 && (
                  <span className="absolute -bottom-12 md:bottom-0 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-6 text-3xl text-light-400 font-mono">
                    &#8674;
                  </span>
                )}
              </div>
            ))}
            <span className="absolute -bottom-12 md:bottom-0 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-6 text-3xl text-light-400 font-mono">
              &#8674;
            </span>
          </div>
        </div>
        <div className="mt-16">
          <Heading type="h3" variant="boxed">
            Knowledge Graph Construction
          </Heading>
          <div className="relative mt-10 grid grid-cols-1 md:grid-cols-3 gap-y-16 gap-x-10">
            {pipeline.slice(3, 6).map((step: any, index: number) => (
              <div key={index + 3 + " " + step.title} className="relative">
                <div className="flex gap-3">
                  <div className="bg-accent-600/80 w-6 h-6 flex items-center justify-center rounded-full flex-shrink-0 font-mono text-xs">
                    <span>{index + 4}</span>
                  </div>
                  <div>
                    <span className="font-mono italic">{step.title}</span>
                    <p className="mt-4 font-extralight leading-relaxed text-light-300">
                      {step.text}
                    </p>
                  </div>
                </div>
                {index >= 0 && (
                  <span className="absolute -bottom-12 md:bottom-0 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-6 text-3xl text-light-400 font-mono">
                    &#8674;
                  </span>
                )}
              </div>
            ))}
            <span className="absolute -bottom-12 md:bottom-0 left-1/2 -translate-x-1/2 rotate-90 md:rotate-0 md:top-1/2 md:-translate-y-1/2 md:-left-6 text-3xl text-light-400 font-mono">
              &#8674;
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TechnicalBackground;

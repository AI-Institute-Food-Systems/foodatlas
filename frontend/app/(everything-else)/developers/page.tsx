import { Metadata } from "next";

import Card from "@/components/basic/Card";
import Code from "@/components/basic/Code";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import SubHeading from "@/components/basic/SubHeading";

export const metadata: Metadata = {
  title: "Developers | FoodAtlas Public API",
  description:
    "Programmatic access to the FoodAtlas knowledge graph. Authenticate with an API key, then call any /v1/ endpoint. Request a key via the contact form.",
};

const API_BASE = "https://api.foodatlas.ai";

const CURL_EXAMPLE = `curl -H "Authorization: Bearer YOUR_KEY" \\
  ${API_BASE}/v1/foods/FA:0001`;

const PYTHON_EXAMPLE = `import requests

resp = requests.get(
    "${API_BASE}/v1/triplets",
    params={"head_id": "FA:0001", "relationship": "contains"},
    headers={"Authorization": "Bearer YOUR_KEY"},
)
resp.raise_for_status()
print(resp.json())`;

const ENDPOINTS: Array<{ method: string; path: string; summary: string }> = [
  { method: "GET", path: "/v1/foods", summary: "List foods (paginated, filterable)" },
  { method: "GET", path: "/v1/foods/{id}", summary: "Get one food" },
  {
    method: "GET",
    path: "/v1/foods/{id}/chemicals",
    summary: "Chemicals contained in a food",
  },
  { method: "GET", path: "/v1/foods/{id}/taxonomy", summary: "IS_A ancestry" },
  { method: "GET", path: "/v1/chemicals", summary: "List chemicals" },
  { method: "GET", path: "/v1/chemicals/{id}", summary: "Get one chemical" },
  {
    method: "GET",
    path: "/v1/chemicals/{id}/foods",
    summary: "Foods containing a chemical",
  },
  {
    method: "GET",
    path: "/v1/chemicals/{id}/diseases",
    summary: "Disease correlations (reduces|worsens)",
  },
  { method: "GET", path: "/v1/diseases", summary: "List diseases" },
  { method: "GET", path: "/v1/diseases/{id}", summary: "Get one disease" },
  {
    method: "GET",
    path: "/v1/diseases/{id}/chemicals",
    summary: "Chemical correlations (reduces|worsens)",
  },
  {
    method: "GET",
    path: "/v1/triplets",
    summary: "Knowledge-graph edges (head, relationship, tail)",
  },
  { method: "GET", path: "/v1/triplets/{id}", summary: "Get one triplet" },
  {
    method: "GET",
    path: "/v1/attestations/{id}",
    summary: "Raw evidence row backing a triplet",
  },
  { method: "GET", path: "/v1/search", summary: "Trigram autocomplete" },
  { method: "GET", path: "/v1/stats", summary: "Aggregate counts" },
  { method: "GET", path: "/v1/bundles", summary: "Released bulk-download bundles" },
];

const Developers = () => {
  return (
    <div>
      <div>
        <Heading type="h1">Developer API</Heading>
        <SubHeading>
          Programmatic access to the <i>FoodAtlas</i> knowledge graph
        </SubHeading>
        <p className="mt-10 text-lg leading-loose text-light-200">
          The public API exposes the same food–chemical–disease graph that
          powers this site, with stable resource-shaped responses suited for
          research scripts. The interactive OpenAPI reference lives at{" "}
          <Link href={`${API_BASE}/docs`}>{`${API_BASE}/docs`}</Link>.
        </p>
      </div>

      <Divider />

      <div>
        <Heading type="h2" variant="boxed">
          Get an API key
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="text-lg font-light text-light-300">
            Keys are issued by hand to keep the door open without inviting
            abuse.{" "}
            <Link href="/contact?api-access" isExternal={false}>
              Request access via the contact form
            </Link>{" "}
            with your name, affiliation, and a one-line description of what
            you&apos;re building. You&apos;ll usually hear back within a few
            business days.
          </p>
          <p className="mt-4 text-lg font-light text-light-300">
            Use is intended for academic and non-commercial research. Please
            cite <i>FoodAtlas</i> in any published work.
          </p>
        </Card>
      </div>

      <div className="mt-20">
        <Heading type="h2" variant="boxed">
          Setup
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <div>
            <Heading type="h3" className="text-xl">
              Base URL
            </Heading>
            <div className="mt-2 flex flex-wrap items-center">
              <p className="mr-3 text-lg font-light text-light-300">
                All endpoints live under
              </p>
              <Code size="text-[1rem]">{API_BASE}</Code>
            </div>
          </div>
          <div className="mt-6">
            <Heading type="h3" className="text-xl">
              Authentication
            </Heading>
            <p className="mt-3 text-lg font-light text-light-300">
              Send your key in the <Code size="text-[1rem]">Authorization</Code>{" "}
              header with the <Code size="text-[1rem]">Bearer</Code> scheme:
            </p>
            <pre className="mt-4 overflow-x-auto rounded bg-light-800 p-4 text-[0.95rem] text-light-100">
              <code>{CURL_EXAMPLE}</code>
            </pre>
          </div>
        </Card>
      </div>

      <div className="mt-20">
        <Heading type="h2" variant="boxed">
          Examples
        </Heading>
      </div>
      <div className="mt-8 flex flex-col gap-6">
        <Card>
          <Heading type="h3" className="text-xl">
            Python
          </Heading>
          <pre className="mt-4 overflow-x-auto rounded bg-light-800 p-4 text-[0.95rem] text-light-100">
            <code>{PYTHON_EXAMPLE}</code>
          </pre>
        </Card>
      </div>

      <div className="mt-20">
        <Heading type="h2" variant="boxed">
          Endpoints
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="mb-4 text-lg font-light text-light-300">
            Full request/response schemas and an interactive console live at{" "}
            <Link href={`${API_BASE}/docs`}>{`${API_BASE}/docs`}</Link>. Pagination
            is offset-based:{" "}
            <Code size="text-[1rem]">?page=&page_size=</Code> (max 100).
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-[0.95rem]">
              <thead className="text-light-200">
                <tr>
                  <th className="py-2 pr-4 font-mono">Method</th>
                  <th className="py-2 pr-4 font-mono">Path</th>
                  <th className="py-2 font-mono">Summary</th>
                </tr>
              </thead>
              <tbody className="font-light text-light-300">
                {ENDPOINTS.map((e) => (
                  <tr key={`${e.method} ${e.path}`} className="border-t border-light-800">
                    <td className="py-2 pr-4 font-mono text-light-100">{e.method}</td>
                    <td className="py-2 pr-4 font-mono">
                      <Code size="text-[0.9rem]">{e.path}</Code>
                    </td>
                    <td className="py-2">{e.summary}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </div>

      <div className="mt-20">
        <Heading type="h2" variant="boxed">
          Versioning &amp; terms
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="text-lg font-light text-light-300">
            Routes under <Code size="text-[1rem]">/v1/</Code> follow a stable
            contract. Breaking changes ship under a new prefix
            (<Code size="text-[1rem]">/v2/</Code>) — we will not change response
            shapes within <Code size="text-[1rem]">/v1/</Code>.
          </p>
          <p className="mt-4 text-lg font-light text-light-300">
            Bulk downloads of released data are available under{" "}
            <Link href="/food-composition-downloads" isExternal={false}>
              Downloads
            </Link>{" "}
            and via <Code size="text-[1rem]">/v1/bundles</Code>. Use those for
            corpus-scale work rather than scraping the API.
          </p>
        </Card>
      </div>
    </div>
  );
};

export default Developers;

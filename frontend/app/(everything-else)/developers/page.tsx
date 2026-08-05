import { Metadata } from "next";

import Card from "@/components/basic/Card";
import Code from "@/components/basic/Code";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";

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
    path: "/v1/bioactivities",
    summary: "List bioactivities (paginated, filterable)",
  },
  {
    method: "GET",
    path: "/v1/bioactivities/{id}",
    summary: "Get one bioactivity + parents/children hierarchy",
  },
  {
    method: "GET",
    path: "/v1/bioactivities/{id}/chemicals",
    summary: "Chemicals measured for a bioactivity (r6)",
  },
  {
    method: "GET",
    path: "/v1/bioactivities/{id}/foods",
    summary: "Foods that exhibit a bioactivity (r5)",
  },
  {
    method: "GET",
    path: "/v1/chemicals/{id}/bioactivities",
    summary: "Bioactivities measured for a chemical",
  },
  {
    method: "GET",
    path: "/v1/foods/{id}/bioactivities",
    summary: "Bioactivities exhibited by a food",
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
        <Heading type="h1" variant="display">
          Developer API
        </Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          The public API exposes the same food–chemical–disease graph that
          powers this site, with stable resource-shaped responses suited for
          research scripts. The interactive OpenAPI reference lives at{" "}
          <Link href={`${API_BASE}/docs`}>{`${API_BASE}/docs`}</Link>.
        </p>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Get an API key
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="text-base font-light text-light-300">
            Keys are issued by hand to keep the door open without inviting
            abuse.{" "}
            <Link href="/contact?api-access" isExternal={false}>
              Request access via the contact form
            </Link>{" "}
            with your name, affiliation, and a one-line description of what
            you&apos;re building. You&apos;ll usually hear back within a few
            business days.
          </p>
          <p className="mt-4 text-base font-light text-light-300">
            Use is intended for academic and non-commercial research. Please
            cite <i>FoodAtlas</i> in any published work.
          </p>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          How to Cite
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="leading-relaxed text-light-200">
            Li, F., Youn, J., Xie, K., Chan, T., Gupta, P., Yoo, A., ... &
            Tagkopoulos, I. (2026). A unified knowledge graph linking
            foodomics to chemical-disease networks and flavor profiles.{" "}
            <i>npj Science of Food</i>.{" "}
            <Link href="https://doi.org/10.1038/s41538-025-00680-9">
              https://doi.org/10.1038/s41538-025-00680-9
            </Link>
          </p>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Setup
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <div>
            <Heading type="h3" className="font-mono italic text-light-300 text-sm font-medium">
              Base URL
            </Heading>
            <div className="mt-2 flex flex-wrap items-center">
              <p className="mr-3 text-base font-light text-light-300">
                All endpoints live under
              </p>
              <Code>{API_BASE}</Code>
            </div>
          </div>
          <div className="mt-6">
            <Heading type="h3" className="font-mono italic text-light-300 text-sm font-medium">
              Authentication
            </Heading>
            <p className="mt-3 text-base font-light text-light-300">
              Send your key in the <Code>Authorization</Code>{" "}
              header with the <Code>Bearer</Code> scheme:
            </p>
            <pre className="mt-4 overflow-x-auto rounded-md bg-light-1000 border-[1.5px] border-light-50/[0.08] p-4 text-[0.85rem] text-light-200 font-mono">
              <code>{CURL_EXAMPLE}</code>
            </pre>
          </div>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Examples
        </Heading>
      </div>
      <div className="mt-8 flex flex-col gap-6">
        <Card>
          <Heading type="h3" className="font-mono italic text-light-300 text-sm font-medium">
            Python
          </Heading>
          <pre className="mt-4 overflow-x-auto rounded-md bg-light-1000 border-[1.5px] border-light-50/[0.08] p-4 text-[0.85rem] text-light-200 font-mono">
            <code>{PYTHON_EXAMPLE}</code>
          </pre>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Endpoints
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="mb-4 text-base font-light text-light-300">
            Full request/response schemas and an interactive console live at{" "}
            <Link href={`${API_BASE}/docs`}>{`${API_BASE}/docs`}</Link>. Pagination
            is offset-based:{" "}
            <Code>?page=&page_size=</Code> (max 100).
          </p>
          <div className="hidden md:block overflow-x-auto">
            {/* Same chrome as the entity-page tables: h-9 header / py-1.5
             * rows / text-sm body, mono cells for method+path. */}
            <table className="w-full table-fixed">
              <colgroup>
                <col className="w-[12%]" />
                <col className="w-[38%]" />
                <col className="w-[50%]" />
              </colgroup>
              <thead className="text-light-400 text-left">
                <tr>
                  {["Method", "Path", "Summary"].map((h, i) => (
                    <th
                      key={h}
                      className={`h-9 border-b border-light-700 leading-none py-1.5 ${
                        i === 0 ? "pr-4" : i === 2 ? "pl-4" : "px-4"
                      } text-left`}
                    >
                      <span className="select-none uppercase text-xs font-medium">
                        {h}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="text-sm font-light">
                {ENDPOINTS.map((e) => (
                  <tr key={`${e.method} ${e.path}`}>
                    <td className="py-1.5 pr-4">
                      <div className="flex min-h-9 items-center font-mono text-light-100">
                        {e.method}
                      </div>
                    </td>
                    <td className="py-1.5 px-4">
                      <div className="flex min-h-9 items-center font-mono text-light-200 break-all">
                        {e.path}
                      </div>
                    </td>
                    <td className="py-1.5 pl-4">
                      <div className="flex min-h-9 items-center text-light-300">
                        {e.summary}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* card list — mobile. METHOD + path on top mono line,
           * summary below. */}
          <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
            {ENDPOINTS.map((e) => (
              <div
                key={`${e.method} ${e.path}`}
                className="w-full py-3 flex flex-col gap-1.5"
              >
                <div className="w-full flex items-baseline gap-2 flex-wrap font-mono">
                  <span className="text-light-100 text-sm">{e.method}</span>
                  <span className="text-light-200 text-xs break-all">
                    {e.path}
                  </span>
                </div>
                <p className="w-full text-light-300 text-sm leading-snug">
                  {e.summary}
                </p>
              </div>
            ))}
          </div>
        </Card>
      </div>

      <div className="mt-16">
        <Heading type="h2" variant="chip">
          Versioning &amp; terms
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          <p className="text-base font-light text-light-300">
            Routes under <Code>/v1/</Code> follow a stable
            contract. Breaking changes ship under a new prefix
            (<Code>/v2/</Code>) — we will not change response
            shapes within <Code>/v1/</Code>.
          </p>
          <p className="mt-4 text-base font-light text-light-300">
            Bulk downloads of released data are available under{" "}
            <Link href="/food-composition-downloads" isExternal={false}>
              Downloads
            </Link>{" "}
            and via <Code>/v1/bundles</Code>. Use those for
            corpus-scale work rather than scraping the API.
          </p>
        </Card>
      </div>
    </div>
  );
};

export default Developers;

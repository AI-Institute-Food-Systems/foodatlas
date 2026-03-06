import { Metadata } from "next";

import ApiDisclosure from "@/components/api/ApiDisclosure";
import Link from "@/components/basic/Link";
import Code from "@/components/basic/Code";
import Card from "@/components/basic/Card";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";

export const metadata: Metadata = {
  title: "FoodAtlas API | Connect to Data for Research or Industry",
  description:
    "FoodAtlas data is provided as a free resource for researchers. Connect to our API to access the extensive knowledge graph of foods, components, and concentrations.",
};

const FoodCompositionApi = () => {
  return (
    <div>
      {/* heading & caption */}
      <div>
        <Heading type="h1">API Documentation</Heading>
        <SubHeading>
          Connect to <i>FoodAtlas</i> using our publicly accessible API
        </SubHeading>
        <p className="mt-10 text-lg leading-loose text-light-200 ">
          Our extensive food composition database contains only evidence-based
          data that can be traced back to its source. FoodAtlas is a
          USDA-NSF-funded research project that provides its data as a free
          resource under the{" "}
          <Link href="https://www.apache.org/licenses/LICENSE-2.0">
            Apache-2.0
          </Link>{" "}
          license.
        </p>
      </div>
      <Divider />
      {/* setup */}
      <div>
        <Heading type="h2" variant="boxed">
          Setup
        </Heading>
      </div>
      <div className="mt-8">
        <Card>
          {/* base url */}
          <div className="">
            <Heading type="h3" className="text-xl">
              Base URL
            </Heading>
            <div className="mt-2 flex flex-wrap items-center">
              <p className="mr-3 font-light text-light-300 text-lg">
                Use the following base URL for all endpoints:
              </p>
              <Code size="text-[1rem]">https://api.foodatlas.ai</Code>
            </div>
          </div>
          {/* authentication */}
          <div className="mt-6">
            <Heading type="h3" className="text-xl">
              Authentication
            </Heading>
            <p className="mt-3 font-light text-light-300 text-lg">
              We use token-based authentication to secure <i>FoodAtlas&apos;</i>{" "}
              API. To call endpoints, you need to add your assigned{" "}
              <Code size="text-[1rem]">Bearer Token</Code>
              to the authorization header for each request.{" "}
              <Link
                className="text-accent-600"
                href="/contact?api-access"
                isExternal={false}
              >
                Request access
              </Link>
            </p>
          </div>
        </Card>
      </div>
      {/* endpoints */}
      <div className="mt-20">
        <Heading type="h2" variant="boxed">
          Endpoints
        </Heading>
      </div>
      {/* metadata */}
      <div className="mt-8">
        <h3 className="ml-3.5 text-light-200 font-mono">Metadata Endpoints</h3>
        <div className="mt-3.5">
          <ApiDisclosure
            requestType={"GET"}
            endpoint="/filters"
            description={
              <p className="font-light">
                Returns the full list of filters in the correct format needed
                for the POST body in the
                <code className="mx-1 text-[1rem]">/overview</code> POST
                endpoint
              </p>
            }
            panelContent={
              <div className="flex flex-col gap-10">
                {/* parameters */}
                <div>
                  <h4 className="font-medium">Parameters</h4>
                  <p className="mt-4">No parameters accepted</p>
                </div>
                {/* example */}
                <div>
                  <h4 className="font-medium">Request Example</h4>
                  <p className="mt-4">
                    <Code className="break-all" size="text-[1rem]">
                      https://api.foodatlas.ai/filters
                    </Code>
                  </p>
                </div>
              </div>
            }
          />
        </div>
      </div>
      <div className="mt-10">
        <h3 className="ml-3.5 text-light-200 font-mono">Data Endpoints</h3>
        <div className="mt-3.5 flex flex-col gap-5">
          <ApiDisclosure
            requestType={"GET"}
            endpoint="/{table}"
            description={
              <p className="font-light">
                Returns the unformatted raw tables from the database
              </p>
            }
            panelContent={
              <div className="flex flex-col gap-10">
                {/* placeholder */}
                <div>
                  <h4 className="font-medium">Placeholder</h4>
                  <div className="mt-4">
                    <p>
                      The{" "}
                      <code className="h-fit bg-light-800 text-light-100 px-2 py-1 rounded text-[1rem]">
                        table
                      </code>{" "}
                      placeholder can be replaced with the following tables:
                    </p>
                    <div className="mt-4 flex flex-col gap-4">
                      <Code size="text-[1rem]">knowledge_graph</Code>
                      <Code size="text-[1rem]">evidence</Code>
                      <Code size="text-[1rem]">entities</Code>
                      <Code size="text-[1rem]">organisms_group</Code>
                      <Code size="text-[1rem]">chemicals_group</Code>
                      <Code size="text-[1rem]">relations</Code>
                      <Code size="text-[1rem]">quality</Code>
                      <Code size="text-[1rem]">retired_entities</Code>
                    </div>
                  </div>
                </div>
                {/* parameters */}
                <div>
                  <h4 className="font-medium">Parameters</h4>
                  <div className="mt-5 flex flex-col gap-4">
                    <p>
                      <Code size="text-[1rem]">page</Code>
                      <span className="ml-3 font-extralight">
                        The page number from which to fetch data
                      </span>
                    </p>
                    <p>
                      <Code size="text-[1rem]">rows_per_page</Code>
                      <span className="ml-3 font-extralight">
                        The number of rows to return per request
                      </span>
                    </p>
                  </div>
                </div>
                {/* example */}
                <div>
                  <h4 className="font-medium">Example</h4>
                  <p className="mt-4">
                    <Code size="text-[1rem]" className="break-all">
                      https://api.foodatlas.ai/entities?page=3&rows_per_page=100
                    </Code>
                  </p>
                </div>
              </div>
            }
          />
          <ApiDisclosure
            requestType={"GET"}
            endpoint="/overview"
            description={
              <p className="font-light">
                Returns the formatted data from the database. Filters cannot be
                selected with endpoint. Use the
                <code className="mx-1 text-[1rem]">/overview</code> POST
                endpoint for filter support.
              </p>
            }
            panelContent={
              <div className="flex flex-col gap-10">
                {/* parameters */}
                <div>
                  <h4 className="font-medium">Parameters</h4>
                  <p className="mt-4 flex flex-col gap-4">
                    <p>
                      <Code size="text-[1rem]">page</Code>
                      <span className="ml-3 font-extralight">
                        The page number from which to fetch data
                      </span>
                    </p>
                    <p>
                      <Code size="text-[1rem]">rows_per_page</Code>
                      <span className="ml-3 font-extralight">
                        The number of rows to return per request
                      </span>
                    </p>
                    {/* order by */}
                    <p>
                      <Code>order_by</Code>
                      <span className="ml-3 font-extralight">
                        The columns (concatenated using{" "}
                        <Code size="text-[1rem]">+</Code>) by which to sort the
                        data. Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">head</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">relation</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">tail</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">evidence</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">triple_quality</Code>
                        </li>
                      </ul>
                    </p>
                    {/* order */}
                    <p>
                      <Code size="text-[1rem]">order</Code>
                      <span className="ml-3 font-extralight">
                        The sorting direction for each column (concatenated
                        using <Code size="text-[1rem]">+</Code>
                        ). Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">asc</Code>
                          <span className="ml-3 font-extralight">
                            Ascending
                          </span>
                        </li>
                        <li>
                          <Code size="text-[1rem]">desc</Code>
                          <span className="ml-3 font-extralight">
                            Descending
                          </span>
                        </li>
                      </ul>
                    </p>
                    {/* sterm */}
                    <p>
                      <Code size="text-[1rem]">sterm</Code>
                      <span className="ml-3 font-extralight">
                        The terms (concatenated using{" "}
                        <Code size="text-[1rem]">+</Code>) used to search data
                      </span>
                    </p>
                    {/* ssubstring */}
                    <p>
                      <Code size="text-[1rem]">ssubstring</Code>
                      <span className="ml-3 font-extralight">
                        Search if terms are contained within part of a name
                        (substring). Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">true</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">false</Code>
                        </li>
                      </ul>
                    </p>
                    {/* ssynonyms */}
                    <p>
                      <Code size="text-[1rem]">ssynonyms</Code>
                      <span className="ml-3 font-extralight">
                        Search for terms across all names, including synonyms,
                        or only the scientific name
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">true</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">false</Code>
                        </li>
                      </ul>
                    </p>
                    {/* scolumn */}
                    <p>
                      <Code size="text-[1rem]">scolumn</Code>
                      <span className="ml-3 font-extralight">
                        Search if terms are found in the head, tail, or both.
                        Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">both</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">head</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">tail</Code>
                        </li>
                      </ul>
                    </p>
                    {/* stype */}
                    <p>
                      <Code size="text-[1rem]">stype</Code>
                      <span className="ml-3 font-extralight">
                        The logical operator used to search multiple terms
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">and</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">or</Code>
                        </li>
                      </ul>
                    </p>
                  </p>
                </div>
                {/* example */}
                <div>
                  <h4 className="font-medium mb-4">Example</h4>
                  <Code size="text-[1rem]" className="break-all">
                    https://api.foodatlas.com/overview?page=3&rows_per_page=100&order_by=evidence+head&order=asc+desc&sterm=tomato+water&ssubstring=false&ssynonyms=true&scolumn=both&stype=or
                  </Code>
                </div>
              </div>
            }
          />
          <ApiDisclosure
            requestType={"POST"}
            endpoint="/overview"
            description={
              <p className="font-light">
                Returns the formatted data from the database. This endpoint is
                the same as the
                <code className="mx-1 text-[1rem]">/overview</code> GET endpoint
                but with filter support in the POST body.
              </p>
            }
            panelContent={
              <div className="flex flex-col gap-10">
                {/* parameters */}
                <div>
                  <h4 className="font-medium">Parameters</h4>
                  <p className="mt-4 flex flex-col gap-4">
                    <p>
                      <Code size="text-[1rem]">page</Code>
                      <span className="ml-3 font-extralight">
                        The page number from which to fetch data
                      </span>
                    </p>
                    <p>
                      <Code>rows_per_page</Code>
                      <span className="ml-3 font-extralight">
                        The number of rows to return per request
                      </span>
                    </p>
                    {/* order by */}
                    <p>
                      <Code>order_by</Code>
                      <span className="ml-3 font-extralight">
                        The columns (concatenated using{" "}
                        <Code size="text-[1rem]">+</Code>) by which to sort the
                        data. Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">head</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">relation</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">tail</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">evidence</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">triple_quality</Code>
                        </li>
                      </ul>
                    </p>
                    {/* order */}
                    <p>
                      <Code size="text-[1rem]">order</Code>
                      <span className="ml-3 font-extralight">
                        The sorting direction for each column (concatenated
                        using <Code size="text-[1rem]">+</Code>
                        ). Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">asc</Code>
                          <span className="ml-3 font-extralight">
                            Ascending
                          </span>
                        </li>
                        <li>
                          <Code size="text-[1rem]">desc</Code>
                          <span className="ml-3 font-extralight">
                            Descending
                          </span>
                        </li>
                      </ul>
                    </p>
                    {/* sterm */}
                    <p>
                      <Code size="text-[1rem]">sterm</Code>
                      <span className="ml-3 font-extralight">
                        The terms (concatenated using{" "}
                        <Code size="text-[1rem]">+</Code>) used to search data
                      </span>
                    </p>
                    {/* ssubstring */}
                    <p>
                      <Code size="text-[1rem]">ssubstring</Code>
                      <span className="ml-3 font-extralight">
                        Search if terms are contained within part of a name
                        (substring). Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">true</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">false</Code>
                        </li>
                      </ul>
                    </p>
                    {/* ssynonyms */}
                    <p>
                      <Code size="text-[1rem]">ssynonyms</Code>
                      <span className="ml-3 font-extralight">
                        Search for terms across all names, including synonyms,
                        or only the scientific name
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">true</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">false</Code>
                        </li>
                      </ul>
                    </p>
                    {/* scolumn */}
                    <p>
                      <Code size="text-[1rem]">scolumn</Code>
                      <span className="ml-3 font-extralight">
                        Search if terms are found in the head, tail, or both.
                        Takes the following arguments
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">both</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">head</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">tail</Code>
                        </li>
                      </ul>
                    </p>
                    {/* stype */}
                    <p>
                      <Code size="text-[1rem]">stype</Code>
                      <span className="ml-3 font-extralight">
                        The logical operator used to search multiple terms
                      </span>
                      <ul className="ml-6 mt-4 list-disc space-y-2">
                        <li>
                          <Code size="text-[1rem]">and</Code>
                        </li>
                        <li>
                          <Code size="text-[1rem]">or</Code>
                        </li>
                      </ul>
                    </p>
                  </p>
                </div>
                {/* body */}
                <div>
                  <h4 className="font-medium">Body</h4>
                  <p className="mt-4 flex flex-col gap-4">
                    The filters must be formatted correctly in the request body
                    or empty data will be returned. Please refer to the /filters
                    endpoint for the correct format.
                  </p>
                </div>
                {/* example */}
                <div>
                  <h4 className="font-medium">Example</h4>
                  <p className="mt-4">
                    <Code size="text-[1rem]" className="break-all">
                      https://api.foodatlas.com/overview?page=3&rows_per_page=100&order_by=evidence+head&order=asc+desc&sterm=tomato+water&ssubstring=false&ssynonyms=true&column=both&stype=or
                    </Code>
                  </p>
                </div>
                {/* body template */}
                <div>
                  <h4 className="font-medium">Body Template</h4>
                  <p className="mt-4 bg-light-800 rounded p-1.5">
                    <pre>
                      <code className="text-light-100  text-[1rem] flex-wrap">
                        {`{
  "filters": {
    "head_type": {
      "organisms": [],
      "chemicals": []
    },
    "relation_type": [], 
    "tail_type": {
      "organisms": [],
      "chemicals": [] 
    },
    "sources": [],
    "triple_quality": []
 }`}
                      </code>
                    </pre>
                  </p>
                </div>
              </div>
            }
          />
        </div>
      </div>
    </div>
  );
};

export default FoodCompositionApi;

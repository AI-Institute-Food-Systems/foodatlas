// meta information section
// displays meta information for a given entity (food, chemical, disease)

import Card from "@/components/basic/Card";
import Link from "@/components/basic/Link";
import Heading from "@/components/basic/Heading";
import Synonyms from "@/components/entities/food/Synonyms";
import { getMetaData } from "@/utils/fetching";
import { capitalizeFirstLetter } from "@/utils/utils";

interface MetainformationSectionProps {
  commonName: string;
  entityType: string;
}

const MetainformationSection = async ({
  commonName,
  entityType,
}: MetainformationSectionProps) => {
  const data = await getMetaData(commonName, entityType);

  return (
    <div className="flex flex-col gap-7">
      {/* section title */}
      <Heading type="h2" variant="boxed">
        {`${capitalizeFirstLetter(entityType)} Overview`}
      </Heading>
      {/* names & ids container */}
      <div className="flex flex-col gap-7">
        {/* names & classification container */}
        <div className="flex flex-col gap-4">
          <Heading
            type="h3"
            className="text-light-300 font-mono text-base font-medium"
          >
            Names & Classification
          </Heading>
          {/* content */}
          <div className="flex flex-col gap-3">
            {/* names & classification cards */}
            <div
              className="grid gap-3"
              style={{
                gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
              }}
            >
              {/* common name */}
              <Card>
                <Heading
                  type="h4"
                  className="font-mono italic text-light-400 text-xs"
                >
                  Common Name
                </Heading>
                <div className="mt-3 capitalize break-all">
                  <p>{commonName}</p>
                </div>
              </Card>
              {/* scientific name */}
              {data?.scientific_name && (
                <Card>
                  <Heading
                    type="h4"
                    className="font-mono italic text-light-400 text-xs"
                  >
                    Scientific Name
                  </Heading>
                  <div className="mt-3 capitalize break-all">
                    <p>{data.scientific_name}</p>
                  </div>
                </Card>
              )}
              {/* food classification */}
              {data?.food_classification && (
                <Card>
                  <Heading
                    type="h4"
                    className="font-mono italic text-light-400 text-xs"
                  >
                    Classification
                  </Heading>
                  <div className="mt-3 capitalize break-all">
                    <p>
                      {data.food_classification.length > 0
                        ? data.food_classification.join(", ")
                        : "—"}
                    </p>
                  </div>
                </Card>
              )}
              {/* chemical classification */}
              {data?.chemical_classification && (
                <Card>
                  <Heading
                    type="h4"
                    className="font-mono italic text-light-400 text-xs"
                  >
                    Classification
                  </Heading>
                  <div className="mt-3 capitalize break-all">
                    <p>
                      {data.chemical_classification.length > 0
                        ? data.chemical_classification.join(", ")
                        : "—"}
                    </p>
                  </div>
                </Card>
              )}
              {/* flavor descriptors */}
              {data?.flavor_descriptors && (
                <Card>
                  <Heading
                    type="h4"
                    className="font-mono italic text-light-400 text-xs"
                  >
                    Flavor
                  </Heading>
                  <div className="mt-3 capitalize break-all">
                    <p>
                      {data.flavor_descriptors.length > 0
                        ? data.flavor_descriptors.join(", ")
                        : "—"}
                    </p>
                  </div>
                </Card>
              )}
            </div>
            {/* synonyms */}
            {data?.synonyms && data.synonyms.length > 0 && (
              <Synonyms synonyms={data.synonyms} />
            )}
          </div>
        </div>
        {/* identifiers container */}
        <div className="flex flex-col gap-4">
          <Heading
            type="h3"
            className="text-light-300 font-mono text-base font-medium"
          >
            Identifiers
          </Heading>
          <Card>
            <table className="w-full table-fixed text-sm">
              <colgroup>
                <col className="w-[30%]" />
                <col className="w-[70%]" />
              </colgroup>
              <thead>
                <tr className="border-b border-light-50/10">
                  <th className="py-2 pr-4 text-left font-mono text-xs font-medium italic text-light-400">
                    Source
                  </th>
                  <th className="py-2 text-left font-mono text-xs font-medium italic text-light-400">
                    Identifier
                  </th>
                </tr>
              </thead>
              <tbody>
                {/* foodatlas id */}
                <tr className="border-b border-light-50/[0.05]">
                  <td className="py-2 pr-4 whitespace-nowrap">FoodAtlas</td>
                  <td className="py-2 break-all">{data?.id ?? "—"}</td>
                </tr>
                {/* external ids */}
                {data?.external_ids &&
                  Object.values(data.external_ids).map((reference) => (
                    <tr
                      key={reference.display_name}
                      className="border-b border-light-50/[0.05]"
                    >
                      <td className="py-2 pr-4 align-top whitespace-nowrap">
                        {reference.display_name}
                      </td>
                      <td className="py-2 break-all">
                        {reference.ids.map((id, idx) => (
                          <span key={id.id}>
                            {idx > 0 && ", "}
                            {id.url ? (
                              <Link href={id.url}>{id.id}</Link>
                            ) : (
                              id.id
                            )}
                          </span>
                        ))}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </Card>
        </div>
      </div>
    </div>
  );
};

MetainformationSection.displayName = "MetainformationSection";

export default MetainformationSection;

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
                    <p>{data.food_classification}</p>
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
                    Chemical Classification
                  </Heading>
                  <div className="mt-3 capitalize break-all">
                    <p>{data.chemical_classification.join(", ")}</p>
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
          <div
            className="grid gap-3"
            style={{
              gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
            }}
          >
            {/* foodatlas id */}
            <Card>
              <Heading
                type="h4"
                className="font-mono italic text-light-400 text-xs"
              >
                FoodAtlas
              </Heading>
              <div className="mt-3">{data.id}</div>
            </Card>
            {/* external ids */}
            {data?.external_ids &&
              // create a card for each source
              Object.values(data.external_ids).map((reference) => (
                <Card key={reference.display_name}>
                  <Heading
                    type="h4"
                    className="font-mono italic text-light-400 text-xs"
                  >
                    {reference.display_name}
                  </Heading>
                  <div className="mt-3 flex flex-wrap gap-3">
                    {reference.ids.map((id) => {
                      return id.url ? (
                        <Link
                          key={id.url}
                          href={id.url}
                          className="whitespace-nowrap"
                        >
                          {id.id}
                        </Link>
                      ) : (
                        <p>{id.id}</p>
                      );
                    })}
                  </div>
                </Card>
              ))}
          </div>
        </div>
      </div>
    </div>
  );
};

MetainformationSection.displayName = "MetainformationSection";

export default MetainformationSection;

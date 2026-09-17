import { Metadata } from "next";

import Person from "@/components/about/Person";
import Card from "@/components/basic/Card";
import Citation from "@/components/basic/Citation";
import Heading from "@/components/basic/Heading";
import Link from "@/components/basic/Link";
import { TeamMember } from "@/types";
import {
  CANONICAL_PUBLICATION,
  PUBLICATIONS,
  doiUrl,
} from "@/utils/publications";

export const metadata: Metadata = {
  title: "About FoodAtlas | USDA-NSF Funded Food Composition Research",
  description:
    "Meet the team dedicated to creating a comprehensive knowledge base where every piece of data is traceable back to its source.",
};

const TEAM: TeamMember[] = [
  {
    name: "Ilias Tagkopoulos",
    position: "Principal Investigator",
    pathToPortrait: "/images/ilias.webp",
    section: "research",
    linkToWebsite:
      "https://www.aifs.ucdavis.edu/about/people?s=ilias-tagkopoulos",
    linkToLinkedIn: "https://www.linkedin.com/in/ilias-tagkopoulos-97a3342/",
  },
  {
    name: "Fangzhou Li",
    position: "Graduate Student Researcher",
    pathToPortrait: "/images/fang.webp",
    section: "research",
    linkToLinkedIn: "https://www.linkedin.com/in/fangzhou-li-8a9359155/",
  },
  {
    name: "Pranav Gupta",
    position: "Graduate Student Researcher",
    pathToPortrait: "/images/pranav.webp",
    section: "research",
    linkToLinkedIn: "https://www.linkedin.com/in/pranavgupta0001/",
  },
  {
    name: "Shanghyeon Kim",
    position: "Postdoctoral Researcher",
    pathToPortrait: "/images/shanghyeon.webp",
    section: "research",
  },
  {
    name: "Lukas Masopust",
    position: "Fullstack Engineer",
    pathToPortrait: "/images/lukas.webp",
    section: "development",
    linkToWebsite: "https://www.aifs.ucdavis.edu/about/people?s=lukas-masopust",
    linkToLinkedIn: "https://www.linkedin.com/in/lukasmaxim/",
  },
  {
    name: "Kaichi Xie",
    position: "Graduate Student Researcher",
    pathToPortrait: "/images/kaichi.webp",
    section: "research",
    linkToLinkedIn: "https://www.linkedin.com/in/kaichi-xie-nicholas/",
  },
];

// Names only, per direction from the group. No photos, no roles, no
// links — the section exists to credit past contributions without
// implying active involvement.
const FORMER_MEMBERS: string[] = ["Jason Youn", "Arielle Yoo"];

const About = () => {
  return (
    <div>
      {/* heading & caption */}
      <div>
        <Heading type="h1" variant="display">About FoodAtlas</Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          <i>FoodAtlas</i> is an ongoing, USDA-NSF-funded research project
          dedicated to creating a comprehensive knowledge base where every piece
          of data is traceable back to its source. We use AI to survey the
          ever-expanding body of peer-reviewed scientific literature and capture
          the relationships between foods and their chemical components. For an
          in-depth description of FoodAtlas, please refer to our{" "}
          <Link href={"/technical-background"} isExternal={false}>
            background
          </Link>{" "}
          page or to the full{" "}
          <Link href={doiUrl(CANONICAL_PUBLICATION.doi)} isExternal={true}>
            publication
          </Link>
          .
          <br />
          <br />
          For questions, or any other inquiry, such as requesting data access,
          please use our{" "}
          <Link href={"/contact"} isExternal={false}>
            contact form
          </Link>
          .
        </p>
      </div>
      {/* inner-workings pointer — a dedicated signpost for readers
       * who came here for the how rather than the who. */}
      <div className="mt-12">
        <Heading type="h2" variant="chip">
          Inner workings
        </Heading>
        <Card className="mt-10">
          <p className="font-serif italic text-light-200 leading-relaxed">
            Curious how FoodAtlas turns literature into a
            knowledge graph? The background page walks through the
            pipeline — filtering, extraction, entity linking, and
            metadata injection.
          </p>
          <div className="mt-4">
            <Link href="/technical-background" isExternal={false}>
              Read the technical background →
            </Link>
          </div>
        </Card>
      </div>

      {/* team */}
      <div className="mt-16 flex flex-col gap-12">
        {/* researchers */}
        <div>
          <Heading type="h2" variant="chip">
            Research Team
          </Heading>
          <div className="mt-10 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-x-5 gap-y-10">
            {TEAM.filter((member) => member.section === "research").map(
              (member) => (
                <Person key={member.name} member={member} />
              )
            )}
          </div>
        </div>
        {/* developers */}
        <div>
          <Heading type="h2" variant="chip">
            Software Engineering Team
          </Heading>
          <div className="mt-10 grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-x-5 gap-y-10">
            {TEAM.filter((member) => member.section === "development").map(
              (member) => (
                <Person key={member.name} member={member} />
              )
            )}
          </div>
        </div>
        {/* former team members — names only, no photos or roles */}
        {FORMER_MEMBERS.length > 0 && (
          <div>
            <Heading type="h2" variant="chip">
              Former Team Members
            </Heading>
            <p className="mt-10 text-base leading-relaxed text-light-300">
              {FORMER_MEMBERS.join(", ")}
            </p>
          </div>
        )}
        {/* publications */}
        <div>
          <Heading type="h2" variant="chip">
            Publications
          </Heading>
          <Card className="mt-10">
            <ol className="flex flex-col gap-4 list-decimal list-inside leading-relaxed text-light-200 marker:text-light-400">
              {PUBLICATIONS.map((publication) => (
                <li key={publication.doi}>
                  <Citation publication={publication} />
                </li>
              ))}
            </ol>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default About;

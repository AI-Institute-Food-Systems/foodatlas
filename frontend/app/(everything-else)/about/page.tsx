import { Metadata } from "next";

import Person from "@/components/about/Person";
import Divider from "@/components/basic/Divider";
import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";
import Link from "@/components/basic/Link";
import { TeamMember } from "@/types";

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
    name: "Jason Youn",
    position: "Graduate Student Researcher",
    pathToPortrait: "/images/jason.webp",
    section: "research",
    linkToLinkedIn: "https://www.linkedin.com/in/jaesungyoun/",
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
    name: "Arielle Yoo",
    position: "Graduate Student Researcher",
    pathToPortrait: "/images/arielle.webp",
    section: "research",
    linkToLinkedIn: "https://www.linkedin.com/in/arielle-soomi-yoo-78016812a/",
  },
  {
    name: "Shanghyeon Kim",
    position: "Postdoctoral Researcher",
    pathToPortrait: "/images/shanghyeon.webp",
    section: "research",
  },
  {
    name: "Lukas Masopust",
    position: "Frontend Engineer",
    pathToPortrait: "/images/lukas.webp",
    section: "development",
    linkToWebsite: "https://www.aifs.ucdavis.edu/about/people?s=lukas-masopust",
    linkToLinkedIn: "https://www.linkedin.com/in/lukasmaxim/",
  },
  {
    name: "Kaichi Xie",
    position: "Backend Engineer",
    pathToPortrait: "/images/kaichi.webp",
    section: "development",
    linkToLinkedIn: "https://www.linkedin.com/in/kaichi-xie-nicholas/",
  },
];

const About = () => {
  return (
    <div>
      {/* heading & caption */}
      <div>
        <Heading type="h1">About FoodAtlas</Heading>
        <SubHeading>
          Meet and connect with the team behind <i>FoodAtlas</i>
        </SubHeading>
        <p className="mt-8 text-lg leading-loose text-light-200">
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
          <Link
            href={
              "https://drive.google.com/file/d/1e9hfCT3Og-Mvsch5Rse0oNq_V4ua_mKo/view?usp=sharing"
            }
            isExternal={true}
          >
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
      <Divider />
      {/* team */}
      <div className="mt-20 flex flex-col gap-16">
        {/* researchers */}
        <div>
          <Heading type="h2" variant="boxed">
            Research Team
          </Heading>
          <div className="mt-10 grid grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-x-5 gap-y-10">
            {TEAM.filter((member) => member.section === "research").map(
              (member) => (
                <Person key={member.name} member={member} />
              )
            )}
          </div>
        </div>
        {/* developers */}
        <div>
          <Heading type="h2" variant="boxed">
            Software Engineering Team
          </Heading>
          <div className="mt-10 grid grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-x-5 gap-y-10">
            {TEAM.filter((member) => member.section === "development").map(
              (member) => (
                <Person key={member.name} member={member} />
              )
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default About;

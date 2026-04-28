import Image from "next/image";
import { FaLinkedin } from "react-icons/fa6";
import { MdLanguage } from "react-icons/md";

import { TeamMember } from "@/types";

interface PersonProps {
  member: TeamMember;
}

const Person = ({ member }: PersonProps) => {
  return (
    <div className="flex flex-col w-full">
      <div className="relative h-52 rounded-2xl overflow-hidden">
        <Image
          className="object-cover"
          fill
          src={member.pathToPortrait}
          alt={`A portrait of ${member.name}`}
        />
      </div>
      <p className="mt-2 text-center text-xl leading-relaxed">
        {member.name}
      </p>
      <p className="mt-0.5 text-center text-sm font-mono text-light-400 leading-relaxed">
        {member.position}
      </p>
      <div className="mt-3 flex justify-center items-center gap-3">
        {member.linkToWebsite && (
          <a
            href={member.linkToWebsite}
            target="_blank"
            tabIndex={0}
            aria-label={`${member.name}'s personal website`}
          >
            <MdLanguage className="h-5 w-5 text-light-300" />
          </a>
        )}
        {member.linkToLinkedIn && (
          <a
            href={member.linkToLinkedIn}
            target="_blank"
            tabIndex={0}
            aria-label={`${member.name} on LinkedIn`}
          >
            <FaLinkedin className="h-5 w-5 text-light-300" />
          </a>
        )}
      </div>
    </div>
  );
};

Person.displayName = "Person";

export default Person;

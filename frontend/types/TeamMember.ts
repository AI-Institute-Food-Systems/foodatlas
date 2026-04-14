export type TeamMember = {
  name: string;
  position: string;
  pathToPortrait: string;
  section: "research" | "development";
  linkToWebsite?: string;
  linkToLinkedIn?: string;
};

import { Metadata } from "next";

import Heading from "@/components/basic/Heading";
import ContactForm from "@/components/contact/ContactForm";
import { buildMetadata } from "@/utils/site";

export const metadata: Metadata = buildMetadata({
  title: "Contact the Research Team",
  description:
    "Contact the FoodAtlas team with questions about our research, data, or methodology, or to request API access for your project.",
  path: "/contact",
});

interface ContactPageProps {
  params: { id: string };
  searchParams: { [key: string]: string | string[] | undefined };
}

const Contact = ({ searchParams }: ContactPageProps) => {
  const isApiAccessRequest = searchParams.hasOwnProperty("api-access");

  return (
    <div>
      <div>
        <Heading type="h1" variant="display">
          Contact Us
        </Heading>
        <p className="mt-6 text-base leading-relaxed text-light-200">
          Ask about our research, data or methods, report a data issue, or
          request an API key. Pick a topic below and we&apos;ll get back to
          you.
        </p>
      </div>
      <div className="mt-12">
        <ContactForm isApiAccessRequest={isApiAccessRequest} />
      </div>
    </div>
  );
};

export default Contact;

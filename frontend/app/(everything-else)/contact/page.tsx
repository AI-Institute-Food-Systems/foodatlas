import { Metadata } from "next";

import Heading from "@/components/basic/Heading";
import SubHeading from "@/components/basic/SubHeading";
import ContactForm from "@/components/contact/ContactForm";

export const metadata: Metadata = {
  title: "Contact FoodAtlas | Get in Touch with the Research Team",
  description:
    "Contact the FoodAtlas team with questions about our research, data, or methodology, or to request API access for your project.",
};

interface ContactPageProps {
  params: { id: string };
  searchParams: { [key: string]: string | string[] | undefined };
}

const Contact = ({ searchParams }: ContactPageProps) => {
  const isApiAccressRequest = searchParams.hasOwnProperty("api-access");

  return (
    <div>
      {/* content container */}
      <div className="flex flex-col md:flex-row gap-10">
        <div className="md:w-1/2">
          <Heading type="h1">Contact Us</Heading>
          <SubHeading>Get in touch with our team</SubHeading>
          <p className="mt-8 text-lg leading-loose text-light-200">
            We love hearing from you! Whether you have a general question about
            our research, methods or data, please use the form to get in touch
            with us and we&apos;re happy to assist you. <br />
            <br />
            For those interested in integrating our data into your own projects,
            you can request API access by detailing your use case, and
            we&apos;ll provide the necessary credentials. If you&apos;ve noticed
            any issues with our data or need further clarification, please
            report them using the form, and we&apos;ll address them promptly.
          </p>
        </div>
        <div className="md:w-1/2">
          <ContactForm isApiAccressRequest={isApiAccressRequest} />
        </div>
      </div>
    </div>
  );
};

export default Contact;

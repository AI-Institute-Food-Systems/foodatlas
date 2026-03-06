"use client";

import { ChangeEvent, useState } from "react";
import {
  Field,
  Fieldset,
  Input,
  Label,
  Select,
  Textarea,
} from "@headlessui/react";
import { MdKeyboardArrowDown } from "react-icons/md";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";
import { twMerge } from "tailwind-merge";

interface ContactFormProps {
  isApiAccressRequest: boolean;
}

const ContactForm = ({ isApiAccressRequest }: ContactFormProps) => {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [affiliation, setAffiliation] = useState("");
  const [topic, setTopic] = useState(
    isApiAccressRequest ? "API Access Request" : "General Inquiry"
  );
  const [message, setMessage] = useState("");

  const handleSubmit = async (e: any) => {
    e.preventDefault();

    const response = await fetch("/contact/send", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ name, email, affiliation, topic, message }),
    });

    if (response.ok) {
      alert(
        "Message sent successfully. A member of our team will be in touch with you soon."
      );
    } else {
      alert("Failed to send message. Please try again later.");
    }
  };

  // handle name change
  const handleNameFieldChange = (e: ChangeEvent<HTMLInputElement>) => {
    setName(e.target.value);
  };

  // handle email change
  const handleEmailFieldChange = (e: ChangeEvent<HTMLInputElement>) => {
    setEmail(e.target.value);
  };

  // handle affiliation change
  const handleAffiliationFieldChange = (e: ChangeEvent<HTMLInputElement>) => {
    setAffiliation(e.target.value);
  };

  // handle topic change
  const handleTopicFieldChange = (e: ChangeEvent<HTMLSelectElement>) => {
    setTopic(e.target.value);
  };

  // handle message change
  const handleMessageFieldChange = (e: ChangeEvent<HTMLTextAreaElement>) => {
    setMessage(e.target.value);
  };

  return (
    <form className="mx-auto flex gap-4" onSubmit={handleSubmit}>
      <Card>
        <Fieldset className="space-y-6">
          {/* name */}
          <Field>
            <Label className="text-sm/6 font-medium text-white">Name</Label>
            <Input
              className={twMerge(
                "mt-3 block w-full rounded-lg bg-light-800 border-light-700/50 border py-1.5 px-3 text-sm/6 text-light-50",
                "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
              )}
              required
              value={name}
              maxLength={40}
              onChange={handleNameFieldChange}
            />
          </Field>
          {/* email */}
          <Field>
            <Label className="text-sm/6 font-medium text-white">Email</Label>
            <Input
              className={twMerge(
                "mt-3 block w-full rounded-lg bg-light-800 border-light-700/50 border py-1.5 px-3 text-sm/6 text-light-50",
                "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
              )}
              required
              value={email}
              maxLength={40}
              onChange={handleEmailFieldChange}
            />
          </Field>
          {/* affiliation */}
          <Field>
            <Label className="text-sm/6 font-medium text-white">
              Affiliation
            </Label>
            <Input
              className={twMerge(
                "mt-3 block w-full rounded-lg bg-light-800 border-light-700/50 border py-1.5 px-3 text-sm/6 text-light-50",
                "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
              )}
              value={affiliation}
              maxLength={40}
              onChange={handleAffiliationFieldChange}
            />
          </Field>
          {/* topic */}
          <Field>
            <Label className="text-sm/6 font-medium text-white">Topic</Label>
            <div className="relative">
              <Select
                className={twMerge(
                  "mt-3 block w-full appearance-none rounded-lg border border-light-700/50 bg-light-800 py-1.5 px-3 text-sm/6 text-light-50",
                  "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25",
                  // Make the text of each option black on Windows
                  "*:text-black"
                )}
                defaultValue={topic}
                value={topic}
                onChange={handleTopicFieldChange}
              >
                <option>General Inquiry</option>
                <option>API Access Request</option>
                <option>Data Issue</option>
              </Select>
              <MdKeyboardArrowDown
                className="group pointer-events-none absolute top-2.5 right-2.5 size-4 fill-white/60"
                aria-hidden="true"
              />
            </div>
          </Field>
          <Field>
            <Label className="text-sm/6 font-medium text-white">Message</Label>
            <Textarea
              className={twMerge(
                "mt-3 block w-full resize-none rounded-lg border border-light-700/50 bg-light-800 py-1.5 px-3 text-sm/6 text-light-50",
                "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
              )}
              required
              rows={5}
              value={message}
              maxLength={2000}
              onChange={handleMessageFieldChange}
            />
          </Field>
          <Button variant="filled">Send Message</Button>
        </Fieldset>
      </Card>
    </form>
  );
};

ContactForm.displayName = "ContactForm";

export default ContactForm;

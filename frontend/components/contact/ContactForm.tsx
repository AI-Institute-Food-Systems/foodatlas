"use client";

import { ChangeEvent, FormEvent, useState } from "react";
import { Field, Fieldset, Input, Label, Textarea } from "@headlessui/react";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";
import ApiAccessFields from "@/components/contact/ApiAccessFields";
import {
  FIELD_CLASS,
  HINT_CLASS,
  LABEL_CLASS,
} from "@/components/contact/fieldStyles";
import FormListbox from "@/components/contact/FormListbox";
import {
  API_ACCESS_TOPIC,
  ApiAccess,
  EMPTY_API_ACCESS,
  isApiAccessComplete,
} from "@/utils/apiAccessFields";
import { track } from "@/utils/umami";

interface ContactFormProps {
  isApiAccessRequest: boolean;
}

// Topics ordered by likely user intent; General Inquiry stays the
// default landing option so first-time visitors don't have to think.
const TOPICS = ["General Inquiry", API_ACCESS_TOPIC, "Data Issue"] as const;

type Status = "idle" | "sending" | "sent" | "error" | "incomplete";

const ContactForm = ({ isApiAccessRequest }: ContactFormProps) => {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [affiliation, setAffiliation] = useState("");
  const [topic, setTopic] = useState<(typeof TOPICS)[number]>(
    isApiAccessRequest ? API_ACCESS_TOPIC : "General Inquiry",
  );
  const [message, setMessage] = useState("");
  const [apiAccess, setApiAccess] = useState<ApiAccess>(EMPTY_API_ACCESS);
  const [status, setStatus] = useState<Status>("idle");

  const isApi = topic === API_ACCESS_TOPIC;

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    // The listboxes and checkbox group can't use native `required`.
    if (isApi && !isApiAccessComplete(apiAccess)) {
      setStatus("incomplete");
      return;
    }
    setStatus("sending");
    try {
      const response = await fetch("/contact/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          email,
          affiliation,
          topic,
          message,
          ...(isApi && { apiAccess }),
        }),
      });
      // Topic + outcome only — never the name, email, message or answers.
      track("contact_submit", {
        topic,
        outcome: response.ok ? "sent" : "error",
      });
      setStatus(response.ok ? "sent" : "error");
    } catch {
      track("contact_submit", { topic, outcome: "error" });
      setStatus("error");
    }
  };

  const on = <T extends HTMLInputElement | HTMLTextAreaElement>(
    setter: (v: string) => void,
  ) => (e: ChangeEvent<T>) => setter(e.target.value);

  return (
    <form className="w-full" onSubmit={handleSubmit}>
      <Card>
        <Fieldset className="flex flex-col gap-5">
          {/* Topic — first so the rest of the form is framed by it. */}
          <FormListbox
            label="What can we help with?"
            options={TOPICS}
            value={topic}
            onChange={setTopic}
          />
          {/* -mt-3 pulls it under the listbox, matching the old in-Field mt-2. */}
          {isApi && (
            <p className="-mt-3 text-xs italic text-accent-500 font-serif">
              The API is under construction. Send your request anyway —
              we&apos;ll reach out once keys are available again.
            </p>
          )}

          {/* Name + email side-by-side once there's room. */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
            <Field>
              <Label className={LABEL_CLASS}>Your name</Label>
              <Input
                className={FIELD_CLASS}
                required
                value={name}
                maxLength={40}
                placeholder="Ada Lovelace"
                onChange={on(setName)}
              />
            </Field>
            <Field>
              <Label className={LABEL_CLASS}>Email</Label>
              <Input
                type="email"
                className={FIELD_CLASS}
                required
                value={email}
                maxLength={80}
                placeholder="you@example.com"
                onChange={on(setEmail)}
              />
            </Field>
          </div>

          {/* Required for API requests: the PI checks it against the email. */}
          <Field>
            <Label className={LABEL_CLASS}>
              Affiliation{" "}
              {!isApi && <span className={HINT_CLASS}>(optional)</span>}
            </Label>
            <Input
              className={FIELD_CLASS}
              required={isApi}
              value={affiliation}
              maxLength={80}
              placeholder="Lab, company, or school"
              onChange={on(setAffiliation)}
            />
          </Field>

          {isApi && (
            <ApiAccessFields value={apiAccess} onChange={setApiAccess} />
          )}

          <Field>
            <Label className={LABEL_CLASS}>
              {isApi ? "Describe your project" : "Your message"}
            </Label>
            <Textarea
              className={twMerge(FIELD_CLASS, "resize-none")}
              required
              rows={6}
              value={message}
              maxLength={2000}
              placeholder={
                isApi
                  ? "What are you building, and how will FoodAtlas data fit in?"
                  : "Tell us a bit about what you're working on…"
              }
              onChange={on(setMessage)}
            />
          </Field>

          <div className="flex items-center justify-between gap-4 flex-wrap">
            <p className="text-xs italic text-light-400 font-serif">
              We usually reply within a few days.
            </p>
            <Button variant="filled" isDisabled={status === "sending"}>
              {status === "sending" ? "Sending…" : "Send message"}
            </Button>
          </div>

          {status === "sent" && (
            <p
              role="status"
              className="text-sm text-accent-500 font-serif italic"
            >
              Thanks — your message is on its way. We&apos;ll be in touch.
            </p>
          )}
          {status === "incomplete" && (
            <p role="alert" className="text-sm text-rose-400">
              Please answer the API access questions: use, volume, commercial
              use, and at least one data type.
            </p>
          )}
          {status === "error" && (
            <p role="alert" className="text-sm text-rose-400">
              Something went wrong sending your message. Please try again.
            </p>
          )}
        </Fieldset>
      </Card>
    </form>
  );
};

ContactForm.displayName = "ContactForm";
export default ContactForm;

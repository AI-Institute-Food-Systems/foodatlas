"use client";

import { ChangeEvent, FormEvent, useState } from "react";
import {
  Field,
  Fieldset,
  Input,
  Label,
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
  Textarea,
} from "@headlessui/react";
import { MdCheck, MdKeyboardArrowDown } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";

interface ContactFormProps {
  isApiAccessRequest: boolean;
}

// Topics ordered by likely user intent; General Inquiry stays the
// default landing option so first-time visitors don't have to think.
const TOPICS = [
  "General Inquiry",
  "API Access Request",
  "Data Issue",
] as const;

// Shared class stack for text inputs — keeps every Field visually
// aligned without repeating the ring/focus/border rules five times.
const FIELD_CLASS = twMerge(
  "mt-2 block w-full rounded-lg bg-light-800 border-light-700/50 border py-2 px-3 text-sm/6 text-light-50 placeholder-light-500",
  "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25",
);
const LABEL_CLASS = "text-sm/6 font-medium text-white";

const ContactForm = ({ isApiAccessRequest }: ContactFormProps) => {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [affiliation, setAffiliation] = useState("");
  const [topic, setTopic] = useState<(typeof TOPICS)[number]>(
    isApiAccessRequest ? "API Access Request" : "General Inquiry",
  );
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState<"idle" | "sending" | "sent" | "error">(
    "idle",
  );

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setStatus("sending");
    try {
      const response = await fetch("/contact/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, email, affiliation, topic, message }),
      });
      setStatus(response.ok ? "sent" : "error");
    } catch {
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
          <Field>
            <Label className={LABEL_CLASS}>What can we help with?</Label>
            <Listbox value={topic} onChange={setTopic}>
              <div className="relative mt-2">
                <ListboxButton
                  className={twMerge(
                    FIELD_CLASS,
                    "mt-0 pl-3 pr-9 text-left",
                  )}
                >
                  {topic}
                  <MdKeyboardArrowDown
                    aria-hidden
                    className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 size-4 text-white/60"
                  />
                </ListboxButton>
                <ListboxOptions
                  anchor="bottom start"
                  className="mt-1 w-[var(--button-width)] rounded-lg border border-light-700/50 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
                >
                  {TOPICS.map((opt) => (
                    <ListboxOption
                      key={opt}
                      value={opt}
                      className="group flex items-center gap-2 px-3 py-2 text-sm text-light-200 data-[focus]:bg-light-900/60 data-[selected]:text-light-50 cursor-pointer"
                    >
                      <MdCheck className="size-4 opacity-0 group-data-[selected]:opacity-100 text-accent-500" />
                      <span>{opt}</span>
                    </ListboxOption>
                  ))}
                </ListboxOptions>
              </div>
            </Listbox>
          </Field>

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

          <Field>
            <Label className={LABEL_CLASS}>
              Affiliation{" "}
              <span className="font-normal text-light-400">(optional)</span>
            </Label>
            <Input
              className={FIELD_CLASS}
              value={affiliation}
              maxLength={80}
              placeholder="Lab, company, or school"
              onChange={on(setAffiliation)}
            />
          </Field>

          <Field>
            <Label className={LABEL_CLASS}>Your message</Label>
            <Textarea
              className={twMerge(FIELD_CLASS, "resize-none")}
              required
              rows={6}
              value={message}
              maxLength={2000}
              placeholder="Tell us a bit about what you're working on…"
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

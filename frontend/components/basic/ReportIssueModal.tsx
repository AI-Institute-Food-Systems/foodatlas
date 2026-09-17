"use client";

import { ChangeEvent, FormEvent, useEffect, useMemo, useState } from "react";
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
import Modal from "@/components/basic/Modal";
import {
  REPORT_CATEGORIES,
  type ReportCategory,
  type ReportContext,
  type ReportRequestBody,
} from "@/types/Report";

interface ReportIssueModalProps {
  isOpen: boolean;
  onClose: () => void;
  context: ReportContext;
}

// Mirrors ContactForm's field styling so the two forms feel like a set.
// Duplicated (not shared) on purpose — small; extracting to a shared
// module would obscure that this modal deliberately matches Contact.
const FIELD_CLASS = twMerge(
  "mt-2 block w-full rounded-lg bg-light-800 border-light-700/50 border py-2 px-3 text-sm/6 text-light-50 placeholder-light-500",
  "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25",
);
const LABEL_CLASS = "text-sm/6 font-medium text-white";

const AUTO_CLOSE_MS = 1800;

// Turn the ReportContext into a two-column preview so the reporter
// sees exactly what's leaving with their message. Skips undefined/
// empty fields for cleanliness.
const contextRows = (context: ReportContext): [string, string][] => {
  return Object.entries(context)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(([k, v]) => [k, String(v)]);
};

const ReportIssueModal = ({
  isOpen,
  onClose,
  context,
}: ReportIssueModalProps) => {
  const [category, setCategory] = useState<ReportCategory>(
    REPORT_CATEGORIES[0],
  );
  const [description, setDescription] = useState("");
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<
    "idle" | "sending" | "sent" | "error"
  >("idle");

  // Reset state whenever the modal reopens so a stale success banner
  // from a prior report doesn't linger for the next data point.
  useEffect(() => {
    if (isOpen) {
      setCategory(REPORT_CATEGORIES[0]);
      setDescription("");
      setEmail("");
      setStatus("idle");
    }
  }, [isOpen]);

  // Auto-dismiss shortly after a successful send so the reporter isn't
  // stuck clicking the close button after a completed action.
  useEffect(() => {
    if (status !== "sent") return;
    const t = setTimeout(onClose, AUTO_CLOSE_MS);
    return () => clearTimeout(t);
  }, [status, onClose]);

  const rows = useMemo(() => contextRows(context), [context]);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (status === "sending") return;
    setStatus("sending");
    const body: ReportRequestBody = {
      category,
      description: description.trim(),
      email: email.trim() || undefined,
      context,
      pageUrl:
        typeof window !== "undefined" ? window.location.href : undefined,
    };
    try {
      const response = await fetch("/report/issue/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
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
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title="Report an issue"
      description={
        <span className="text-sm">
          Flag something that looks off with this data point — a wrong
          value, a bad extraction, a duplicate. We&apos;ll take a look.
        </span>
      }
    >
      <form className="w-full" onSubmit={handleSubmit}>
        <Fieldset className="flex flex-col gap-5">
          <Field>
            <Label className={LABEL_CLASS}>What&apos;s wrong?</Label>
            <Listbox value={category} onChange={setCategory}>
              <div className="relative mt-2">
                <ListboxButton
                  className={twMerge(FIELD_CLASS, "mt-0 pl-3 pr-9 text-left")}
                >
                  {category}
                  <MdKeyboardArrowDown
                    aria-hidden
                    className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 size-4 text-white/60"
                  />
                </ListboxButton>
                <ListboxOptions
                  anchor="bottom start"
                  className="mt-1 w-[var(--button-width)] rounded-lg border border-light-700/50 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
                >
                  {REPORT_CATEGORIES.map((opt) => (
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

          <Field>
            <Label className={LABEL_CLASS}>What did you see?</Label>
            <Textarea
              className={twMerge(FIELD_CLASS, "resize-none")}
              required
              rows={5}
              value={description}
              maxLength={2000}
              placeholder="e.g. 'Si' extracted as 'ser-ile peptide'"
              onChange={on(setDescription)}
            />
          </Field>

          <Field>
            <Label className={LABEL_CLASS}>
              Your email{" "}
              <span className="font-normal text-light-400">
                (optional — lets us follow up if it&apos;s fixed)
              </span>
            </Label>
            <Input
              type="email"
              className={FIELD_CLASS}
              value={email}
              maxLength={80}
              placeholder="you@example.com"
              onChange={on(setEmail)}
            />
          </Field>

          {rows.length > 0 && (
            <details className="text-xs text-light-400">
              <summary className="cursor-pointer select-none py-1 hover:text-light-200">
                What gets sent with this report
              </summary>
              <dl className="mt-2 grid grid-cols-[max-content_1fr] gap-x-3 gap-y-1 rounded-md border border-light-700/40 bg-light-900/40 p-3 font-mono">
                {rows.map(([k, v]) => (
                  <div key={k} className="contents">
                    <dt className="text-light-500">{k}</dt>
                    <dd className="text-light-200 break-all">{v}</dd>
                  </div>
                ))}
              </dl>
            </details>
          )}

          <div className="flex items-center justify-end gap-4 flex-wrap">
            <Button variant="filled" isDisabled={status === "sending"}>
              {status === "sending" ? "Sending…" : "Send report"}
            </Button>
          </div>

          {status === "sent" && (
            <p
              role="status"
              className="text-sm text-accent-500 font-serif italic"
            >
              Thanks — we&apos;ll take a look.
            </p>
          )}
          {status === "error" && (
            <p role="alert" className="text-sm text-rose-400">
              Something went wrong sending the report. Please try again.
            </p>
          )}
        </Fieldset>
      </form>
    </Modal>
  );
};

ReportIssueModal.displayName = "ReportIssueModal";
export default ReportIssueModal;

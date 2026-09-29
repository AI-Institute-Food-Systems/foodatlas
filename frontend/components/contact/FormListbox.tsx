"use client";

import {
  Field,
  Label,
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
} from "@headlessui/react";
import { MdCheck, MdKeyboardArrowDown } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import { FIELD_CLASS, LABEL_CLASS } from "@/components/contact/fieldStyles";

interface FormListboxProps<T extends string> {
  label: string;
  options: readonly T[];
  value: T | "";
  onChange: (value: T) => void;
  placeholder?: string;
}

// Labelled single-choice dropdown in the contact form's field style.
const FormListbox = <T extends string>({
  label,
  options,
  value,
  onChange,
  placeholder = "Select…",
}: FormListboxProps<T>) => (
  <Field>
    <Label className={LABEL_CLASS}>{label}</Label>
    <Listbox value={value} onChange={onChange}>
      <div className="relative mt-2">
        <ListboxButton
          className={twMerge(
            FIELD_CLASS,
            "mt-0 pl-3 pr-9 text-left",
            value === "" && "text-light-500",
          )}
        >
          {value || placeholder}
          <MdKeyboardArrowDown
            aria-hidden
            className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 size-4 text-white/60"
          />
        </ListboxButton>
        <ListboxOptions
          anchor="bottom start"
          className="mt-1 w-[var(--button-width)] rounded-lg border border-light-700/50 bg-light-950 shadow-lg shadow-black/40 focus:outline-none z-50 py-1"
        >
          {options.map((opt) => (
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
);

export default FormListbox;

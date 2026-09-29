"use client";

import { Field, Input, Label } from "@headlessui/react";

import {
  FIELD_CLASS,
  HINT_CLASS,
  LABEL_CLASS,
} from "@/components/contact/fieldStyles";
import FormListbox from "@/components/contact/FormListbox";
import {
  ApiAccess,
  COMMERCIAL,
  DATA_NEEDED,
  DataNeeded,
  USE_CATEGORIES,
  VOLUMES,
} from "@/utils/apiAccessFields";

interface ApiAccessFieldsProps {
  value: ApiAccess;
  onChange: (next: ApiAccess) => void;
}

const CHOICE_CLASS =
  "flex items-center gap-2 text-sm text-light-200 cursor-pointer";
const LEGEND_CLASS = `${LABEL_CLASS} mb-2`;

// The structured questions shown only for "API Access Request". Each maps to
// a column in the PI's review sheet, so answers are fixed choices rather
// than free text wherever possible.
const ApiAccessFields = ({ value, onChange }: ApiAccessFieldsProps) => {
  const set = <K extends keyof ApiAccess>(key: K, v: ApiAccess[K]) =>
    onChange({ ...value, [key]: v });

  const toggleData = (d: DataNeeded) =>
    set(
      "dataNeeded",
      value.dataNeeded.includes(d)
        ? value.dataNeeded.filter((x) => x !== d)
        : [...value.dataNeeded, d],
    );

  return (
    <>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
        <FormListbox
          label="How will you use the API?"
          options={USE_CATEGORIES}
          value={value.useCategory}
          onChange={(v) => set("useCategory", v)}
        />
        <FormListbox
          label="Expected volume"
          options={VOLUMES}
          value={value.volume}
          onChange={(v) => set("volume", v)}
        />
      </div>

      <fieldset>
        <legend className={LEGEND_CLASS}>
          Will this be used commercially?
        </legend>
        <div className="flex flex-wrap gap-x-6 gap-y-2">
          {COMMERCIAL.map((opt) => (
            <label key={opt} className={CHOICE_CLASS}>
              <input
                type="radio"
                name="commercial"
                required
                className="accent-accent-500"
                checked={value.commercial === opt}
                onChange={() => set("commercial", opt)}
              />
              {opt}
            </label>
          ))}
        </div>
      </fieldset>

      <fieldset>
        <legend className={LEGEND_CLASS}>
          Which data do you need?{" "}
          <span className={HINT_CLASS}>(pick all that apply)</span>
        </legend>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-y-2 gap-x-6">
          {DATA_NEEDED.map((opt) => (
            <label key={opt} className={CHOICE_CLASS}>
              <input
                type="checkbox"
                className="accent-accent-500"
                checked={value.dataNeeded.includes(opt)}
                onChange={() => toggleData(opt)}
              />
              {opt}
            </label>
          ))}
        </div>
      </fieldset>

      <Field>
        <Label className={LABEL_CLASS}>
          Project or lab URL <span className={HINT_CLASS}>(optional)</span>
        </Label>
        <Input
          type="url"
          className={FIELD_CLASS}
          value={value.projectUrl}
          maxLength={200}
          placeholder="https://"
          onChange={(e) => set("projectUrl", e.target.value)}
        />
      </Field>
    </>
  );
};

export default ApiAccessFields;

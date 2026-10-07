"use client";

import { HINT_CLASS, LABEL_CLASS } from "@/components/contact/fieldStyles";
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
      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-5">
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
        <fieldset>
          <legend className={LABEL_CLASS}>Used commercially?</legend>
          {/* min-h matches a text field, so the radios line up with the
           * dropdowns beside them. */}
          <div className="mt-2 flex min-h-[42px] flex-wrap items-center gap-x-6 gap-y-2">
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
      </div>

      <fieldset>
        <legend className={LEGEND_CLASS}>
          Which data do you need?{" "}
          <span className={HINT_CLASS}>(pick all that apply)</span>
        </legend>
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-y-2 gap-x-6">
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
    </>
  );
};

export default ApiAccessFields;

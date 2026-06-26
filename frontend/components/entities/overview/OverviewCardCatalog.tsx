// Variant A — "Library Card Catalog"
// Cream-chip section tags poke out the card's left edge like glued labels.
// Double-rule dividers, mono-uppercase term/value rows.

import { ReactNode } from "react";

import Link from "@/components/basic/Link";
import Synonyms from "@/components/entities/food/Synonyms";
import { Metadata } from "@/types";
import type { EntityType } from "@/components/entities/EntityTabs";

interface Props {
  entityType: EntityType;
  data: Metadata;
  // When rendered inside a tab card we don't want a second card wrapper
  // around our own content (cards-in-cards). `naked` drops the outer
  // bg-light-950 / border / rounded shell and emits just the sections.
  naked?: boolean;
}

type ExternalIdShape = Metadata["external_ids"][string];

const Section = ({
  label,
  first,
  children,
}: {
  label: string;
  first?: boolean;
  children: ReactNode;
}) => (
  <section
    className={
      first
        ? "flex flex-col gap-3"
        : "flex flex-col gap-3 pt-5 border-t-2 border-double border-light-700/60"
    }
  >
    <span className="self-start -ml-3 bg-light-200 shadow-inner shadow-light-50 rounded-r-md px-2.5 py-0.5 font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase">
      {label}
    </span>
    <div className="text-sm text-light-200 leading-relaxed">{children}</div>
  </section>
);

const Field = ({
  term,
  children,
}: {
  term: string;
  children: ReactNode;
}) => (
  <div className="flex gap-3 items-baseline">
    <dt className="font-mono text-[10px] uppercase tracking-[0.12em] text-light-500 shrink-0 w-20">
      {term}
    </dt>
    <dd className="text-light-100 break-all">{children}</dd>
  </div>
);

// External-id payloads from upstream ontologies (FoodOn, CDNO, …) store the
// full PURL URL in `id.id` (e.g. http://purl.obolibrary.org/obo/FOODON_00003443).
// The row label already names the ontology, so showing the URL — or even the
// `FOODON_` prefix — is redundant noise. Strip both and keep just the local
// code as the clickable label; falls back to the raw value if the pattern
// doesn't match.
const cleanExternalIdLabel = (raw: string): string => {
  const lastSlash = raw.lastIndexOf("/");
  const bare = lastSlash >= 0 ? raw.slice(lastSlash + 1) : raw;
  const parts = bare.split("_");
  if (parts.length >= 2 && /^[A-Za-z]+$/.test(parts[0])) {
    return parts.slice(1).join("_");
  }
  return bare;
};

const OverviewCardCatalog = ({ entityType, data, naked }: Props) => {
  const externalEntries = Object.entries(
    (data.external_ids ?? {}) as Record<string, ExternalIdShape>
  ).filter(([, value]) => value?.ids?.length);

  const classification =
    entityType === "food"
      ? data.food_classification
      : entityType === "chemical"
      ? data.chemical_classification
      : undefined;

  const inner = (
    <div className="flex flex-col gap-5">
        {data.description && (
          <p className="text-sm text-light-300 leading-relaxed">
            {data.description}
          </p>
        )}

        <Section label="Identifiers" first={!data.description}>
          <dl className="flex flex-col gap-1.5">
            <Field term="FoodAtlas">
              <span className="font-mono">{data.id}</span>
            </Field>
            {externalEntries.map(([key, ref]) => (
              <Field key={key} term={ref.display_name}>
                {ref.ids.map((id, i) => {
                  const label = cleanExternalIdLabel(id.id);
                  return (
                    <span key={`${id.id}-${i}`}>
                      {i > 0 && ", "}
                      {id.url ? (
                        <Link href={id.url} isExternal>
                          <span className="font-mono">{label}</span>
                        </Link>
                      ) : (
                        <span className="font-mono">{label}</span>
                      )}
                    </span>
                  );
                })}
              </Field>
            ))}
          </dl>
        </Section>

        {classification && classification.length > 0 && (
          <Section label="Classification">
            <span className="capitalize">{classification.join(", ")}</span>
          </Section>
        )}

        {data.flavor_descriptors && data.flavor_descriptors.length > 0 && (
          <Section label="Flavor">
            <span className="capitalize">
              {data.flavor_descriptors.join(", ")}
            </span>
          </Section>
        )}

      {data.synonyms && data.synonyms.length > 0 && (
        <Section label="Synonyms">
          <Synonyms synonyms={data.synonyms} naked />
        </Section>
      )}
    </div>
  );

  if (naked) return inner;

  return (
    <div className="relative bg-light-950 border-[1.5px] border-light-50/[0.08] rounded-xl shadow-[inset_0_5px_8px_rgba(255,249,242,0.02)]">
      <div className="px-5 md:px-6 pt-7 pb-7">{inner}</div>
    </div>
  );
};

OverviewCardCatalog.displayName = "OverviewCardCatalog";

export default OverviewCardCatalog;

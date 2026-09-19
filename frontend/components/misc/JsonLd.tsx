// Schema.org structured data, the way Google Dataset Search and the LLM
// crawlers expect it. Server component; `<` is escaped so a value can never
// close the script tag.
interface Props {
  data: Record<string, unknown>;
}

const JsonLd = ({ data }: Props) => (
  <script
    type="application/ld+json"
    dangerouslySetInnerHTML={{
      __html: JSON.stringify(data).replace(/</g, "\\u003c"),
    }}
  />
);

JsonLd.displayName = "JsonLd";

export default JsonLd;

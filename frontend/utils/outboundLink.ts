// Declarative umami attributes for external anchors: the tracker script
// picks up `data-umami-event` on click, so a server component can count
// outbound links without shipping any client JS. `url` is the exact link
// (Properties → url answers "which paper / which database entry"); `host`
// is there for the coarse breakdown.
export const outboundLinkAttrs = (
  href: string
): {
  "data-umami-event"?: string;
  "data-umami-event-host"?: string;
  "data-umami-event-url"?: string;
} => {
  let host: string;
  try {
    host = new URL(href).hostname;
  } catch {
    // mailto:, relative, or malformed — nothing worth counting.
    return {};
  }
  if (!host) return {};
  return {
    "data-umami-event": "outbound_link",
    "data-umami-event-host": host,
    "data-umami-event-url": href,
  };
};

// Declarative umami attributes for external anchors: the tracker script
// picks up `data-umami-event` on click, so a server component can count
// outbound links without shipping any client JS. Hostname only — the full
// URL would make the property one value per link.
export const outboundLinkAttrs = (
  href: string
): { "data-umami-event"?: string; "data-umami-event-host"?: string } => {
  let host: string;
  try {
    host = new URL(href).hostname;
  } catch {
    // mailto:, relative, or malformed — nothing worth counting.
    return {};
  }
  if (!host) return {};
  return { "data-umami-event": "outbound_link", "data-umami-event-host": host };
};

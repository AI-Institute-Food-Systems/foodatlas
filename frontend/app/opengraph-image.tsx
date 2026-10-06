import { ImageResponse } from "next/og";

import { OG_IMAGE, SITE_NAME } from "@/utils/site";

// The site-wide share card, 1200×630. Every page names it in its openGraph
// block via buildMetadata. Colours are the site's own: light-1000 ground,
// accent-600 bar, light-300 body text. Satori (next/og) supports flexbox
// only, so every div carries an explicit display.
export const alt = OG_IMAGE.alt;
export const size = { width: OG_IMAGE.width, height: OG_IMAGE.height };
export const contentType = "image/png";

const OpengraphImage = () =>
  new ImageResponse(
    (
      <div
        style={{
          height: "100%",
          width: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          backgroundColor: "#0D0C0C",
          padding: "72px",
          fontFamily: "sans-serif",
        }}
      >
        <div
          style={{
            display: "flex",
            width: "160px",
            height: "14px",
            borderRadius: "7px",
            backgroundColor: "#F4511E",
          }}
        />
        <div style={{ display: "flex", flexDirection: "column" }}>
          <div
            style={{
              display: "flex",
              fontSize: 96,
              fontWeight: 700,
              color: "#fff9f2",
              lineHeight: 1.1,
            }}
          >
            {SITE_NAME}
          </div>
          <div
            style={{
              display: "flex",
              fontSize: 40,
              color: "#dad3cb",
              marginTop: "24px",
              lineHeight: 1.35,
            }}
          >
            Evidence-based knowledge graph of foods, chemicals, diseases and
            bioactivities.
          </div>
        </div>
        <div
          style={{
            display: "flex",
            fontSize: 28,
            color: "#b2aca5",
          }}
        >
          foodatlas.ai · AI Institute for Next Generation Food Systems, UC Davis
        </div>
      </div>
    ),
    size
  );

export default OpengraphImage;

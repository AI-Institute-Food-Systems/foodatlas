import { readFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

// Browsers, feed readers and Google's favicon fetcher request /favicon.ico
// directly, whatever the <link> tags say; app/icon.svg alone left it a 404.
// The ICO carries its own 16/32 frames, drawn with heavier strokes, because
// the SVG's hairlines rasterise to a grey smudge at tab size.
describe("favicon.ico", () => {
  const buf = readFileSync(join(process.cwd(), "app/favicon.ico"));

  it("is an ICO with tab and taskbar sizes", () => {
    expect(buf.readUInt16LE(0)).toBe(0); // reserved
    expect(buf.readUInt16LE(2)).toBe(1); // type: icon
    const count = buf.readUInt16LE(4);
    // ICONDIRENTRY is 16 bytes from offset 6; width 0 means 256.
    const sizes = Array.from({ length: count }, (_, i) => buf[6 + i * 16]);
    expect(sizes).toEqual(expect.arrayContaining([16, 32]));
  });
});

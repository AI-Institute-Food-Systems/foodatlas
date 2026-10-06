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

// iOS home-screen icon (F1) and the web manifest (F2).
describe("apple-icon and manifest", () => {
  const pngSize = (p: string) => {
    const b = readFileSync(join(process.cwd(), p));
    // PNG signature, then IHDR: width and height at bytes 16 and 20.
    expect(b.subarray(1, 4).toString()).toBe("PNG");
    return [b.readUInt32BE(16), b.readUInt32BE(20)];
  };

  it("ships a 180×180 apple-icon", () => {
    expect(pngSize("app/apple-icon.png")).toEqual([180, 180]);
  });

  it("lists 192 and 512 icons that exist at those sizes", async () => {
    const { default: manifest } = await import("@/app/manifest");
    const m = manifest();
    expect(m.short_name).toBe("FoodAtlas");
    for (const size of [192, 512]) {
      const icon = m.icons?.find((i) => i.sizes === `${size}x${size}`);
      expect(icon, `${size} icon`).toBeDefined();
      expect(pngSize(`public${icon!.src}`)).toEqual([size, size]);
    }
  });
});

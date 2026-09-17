import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { clearApiCache } from "@/utils/apiFetch";
import {
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";

/**
 * The wire format between the two filter implementations.
 *
 * The backend splits every multi-select filter on a literal `+`
 * (`repositories/_sources.py::parse_sources`, and the `.split("+")` in the
 * bioactivity predicates). The frontend has to produce exactly that, and
 * nothing on either side checks it: the backend tests call repository
 * functions with an already-joined string, and the component tests mock
 * `getFoodCompositionData` entirely, so neither ever looks at the URL.
 *
 * Change the join to a comma and every test in the repo still passes while
 * multi-select silently degrades to "one big unknown source" — which is
 * one keystroke away from the bug this whole effort was about. These tests
 * assert the actual query string.
 *
 * `%2B` rather than a bare `+`: in a query string a literal `+` decodes to
 * a space, so the encoded form is the only one that survives to the server
 * as a separator.
 */

const ok = (body: unknown) =>
  ({ ok: true, json: async () => body }) as unknown as Response;

// Structurally typed rather than `ReturnType<typeof vi.spyOn>`: that
// resolves to a spy whose signature must match `fetch` exactly, and
// reading `calls[0][0]` off it doesn't typecheck.
type CallRecorder = { mock: { calls: unknown[][] } };

const urlOf = (spy: CallRecorder): string => String(spy.mock.calls[0][0]);

describe("composition filter URL contract", () => {
  beforeEach(() => {
    vi.stubEnv("NEXT_PUBLIC_API_URL", "http://api.test");
    vi.stubEnv("NEXT_PUBLIC_API_KEY", "k");
    clearApiCache();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("joins multiple sources with %2B, the separator the API splits on", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: [], metadata: { total_rows: 0 } }));

    await getFoodCompositionData(
      "pepper (raw)",
      1,
      ["fdc", "ptfi"],
      "",
      { column: "median_concentration", direction: "desc" },
      true
    );

    expect(urlOf(spy)).toContain("filter_source=fdc%2Bptfi");
  });

  it("sends a single source without a trailing separator", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: [], metadata: { total_rows: 0 } }));

    await getFoodCompositionData(
      "pepper (raw)",
      1,
      ["ptfi"],
      "",
      { column: "common_name", direction: "asc" },
      true
    );

    const url = urlOf(spy);
    expect(url).toContain("filter_source=ptfi&");
    expect(url).not.toContain("ptfi%2B");
  });

  it("joins multiple classifications with the same separator", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: [], metadata: { total_rows: 0 } }));

    await getFoodCompositionData(
      "pepper (raw)",
      1,
      ["ptfi"],
      "",
      { column: "common_name", direction: "asc" },
      true,
      ["flavonoid", "alkaloid"]
    );

    expect(urlOf(spy)).toContain(
      "filter_classification=flavonoid%2Balkaloid"
    );
  });

  it("encodes a classification containing the separator character", async () => {
    // "n/a" is a real option and the slash must not terminate the value.
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: [], metadata: { total_rows: 0 } }));

    await getFoodCompositionData(
      "pepper (raw)",
      1,
      ["ptfi"],
      "",
      { column: "common_name", direction: "asc" },
      true,
      ["n/a"]
    );

    expect(urlOf(spy)).toContain("filter_classification=n%2Fa");
  });

  it("counts endpoint uses the same separator as the rows endpoint", async () => {
    // These are two separate query builders. When they disagree the
    // sidebar counts describe a different row set than the table shows —
    // the precise class of drift that produced the empty-table bug.
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: { source_counts: {} } }));

    await getFoodCompositionCounts("pepper (raw)", {
      sourceFilters: ["fdc", "ptfi"],
      classificationFilters: ["flavonoid", "alkaloid"],
    });

    const url = decodeURIComponent(urlOf(spy));
    expect(url).toContain("filter_source=fdc+ptfi");
    expect(url).toContain("filter_classification=flavonoid+alkaloid");
  });

  it("does not send a filter param when nothing is selected", async () => {
    const spy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(ok({ data: { source_counts: {} } }));

    await getFoodCompositionCounts("pepper (raw)", {});

    const url = urlOf(spy);
    expect(url).not.toContain("filter_source=");
    expect(url).not.toContain("filter_classification=");
  });
});

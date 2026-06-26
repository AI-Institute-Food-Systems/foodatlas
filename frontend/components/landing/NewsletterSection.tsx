"use client";

import { FormEvent, useState } from "react";

type Status = "idle" | "submitting" | "success" | "error";

const NewsletterSection = () => {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<Status>("idle");

  const onSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!email || status === "submitting") return;
    setStatus("submitting");
    try {
      // TODO: wire to backend (e.g. /api/newsletter or Resend). For now this
      // resolves locally so the UX flow can be QA'd end-to-end.
      await new Promise((r) => setTimeout(r, 500));
      setStatus("success");
    } catch {
      setStatus("error");
    }
  };

  return (
    <section className="bg-light-1000 w-full">
      <div className="px-3 md:px-12">
        <div className="relative max-w-6xl mx-auto py-20 md:py-24">
          {/* top double rule — letterpress slip framing */}
          <div className="border-t-2 border-double border-light-700/60" />

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 lg:gap-16 items-center py-10 md:py-14">
            {/* LEFT: pitch + subscribe slip */}
            <div className="flex flex-col gap-6 order-2 lg:order-1">
              <div className="flex flex-col gap-3">
                <span className="font-mono italic uppercase text-light-500 text-[11px] tracking-[0.22em]">
                  Weekly Dispatch · No charge
                </span>
                <h2 className="font-serif text-3xl md:text-5xl text-light-100 leading-tight">
                  The Weekly Bulletin
                </h2>
                <p className="font-serif italic text-light-400 text-base md:text-lg max-w-md">
                  Dispatches from the FoodAtlas knowledge graph — new
                  entities, fresh evidence, and the occasional methodology
                  note, every Sunday.
                </p>
              </div>

              <form
                onSubmit={onSubmit}
                className="w-full max-w-[26rem] flex flex-col gap-3"
                aria-label="Newsletter subscription"
              >
                {status === "success" ? (
                  <div className="flex flex-col gap-1.5 border-y-2 border-double border-light-500/60 py-4 px-1">
                    <span className="font-mono italic text-[10px] uppercase tracking-[0.22em] text-accent-500">
                      Subscribed
                    </span>
                    <p className="font-serif italic text-light-100 text-lg">
                      You&apos;re on the list. The next dispatch is on its way.
                    </p>
                  </div>
                ) : (
                  <>
                    {/* the input sits ON the rule, like a fillable form slip */}
                    <div className="relative flex items-end gap-3 border-b-2 border-double border-light-500/70 pb-2">
                      <label
                        htmlFor="newsletter-email"
                        className="font-mono italic text-[10px] uppercase tracking-[0.22em] text-light-500 pb-0.5"
                      >
                        Email
                      </label>
                      <input
                        id="newsletter-email"
                        type="email"
                        required
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        placeholder="reader@example.org"
                        autoComplete="email"
                        className="flex-1 min-w-0 bg-transparent outline-none text-light-100 placeholder:text-light-600 font-serif text-base pb-0.5"
                      />
                      <button
                        type="submit"
                        disabled={status === "submitting"}
                        className="self-end -mb-[1.5px] px-3.5 py-1 rounded-t-md bg-light-200 text-light-900 text-xs font-mono italic font-semibold border-[1.5px] border-light-200 shadow-[inset_0_1px_2px_rgba(255,249,242,0.6)] hover:bg-light-100 transition-colors disabled:opacity-50"
                      >
                        {status === "submitting" ? "Sending…" : "Subscribe →"}
                      </button>
                    </div>
                    {status === "error" ? (
                      <p className="font-mono italic text-[11px] uppercase tracking-[0.2em] text-red-400">
                        Something went wrong. Please try again.
                      </p>
                    ) : (
                      <p className="font-mono italic text-[10px] uppercase tracking-[0.2em] text-light-600">
                        Weekly · Free · Opt out anytime
                      </p>
                    )}
                  </>
                )}
              </form>
            </div>

            {/* RIGHT: preview of the weekly bulletin as a tilted paper */}
            <div className="order-1 lg:order-2 relative">
              <BulletinPreview />
            </div>
          </div>

          {/* bottom double rule */}
          <div className="border-t-2 border-double border-light-700/60" />
        </div>
      </div>
    </section>
  );
};

// Static visual of the bulletin — top portion only, fades at the bottom
// to suggest more below. Tilted a hair so it reads as a printed page
// propped against the section. Chrome mirrors newsletter.html exactly
// (cream chip section labels, double-rule dividers, serif headline,
// mono-italic captions, accent-orange deltas) so subscribers' first
// email matches what they were sold here.
const PREVIEW_STATS: { value: string; label: string; delta: string }[] = [
  { value: "1,284", label: "associations", delta: "+312" },
  { value: "147", label: "papers", delta: "+24" },
  { value: "58", label: "foods", delta: "+9" },
];
const PREVIEW_HIGHLIGHTS: { name: string; chemicals: string; count: string }[] =
  [
    {
      name: "Black tea",
      chemicals: "theaflavin · gallic acid · L-theanine",
      count: "21",
    },
    {
      name: "Walnut",
      chemicals: "juglone · ellagic acid · α-linolenic acid",
      count: "17",
    },
    {
      name: "Saffron",
      chemicals: "crocin · safranal · picrocrocin",
      count: "14",
    },
  ];

const BulletinPreview = () => (
  <div
    className="relative mx-auto w-full max-w-[26rem] lg:max-w-md rotate-[-1.6deg] hover:rotate-0 transition-transform duration-500 ease-out"
    aria-hidden
  >
    {/* paper shadow */}
    <div
      className="absolute inset-0 translate-y-3 translate-x-1 rounded-xl bg-black/40 blur-2xl"
      aria-hidden
    />
    <div className="relative rounded-xl border border-light-50/[0.08] bg-light-950 shadow-[inset_0_5px_8px_rgba(255,249,242,0.02)] overflow-hidden">
      <div className="p-6 pb-0 flex flex-col gap-4">
        {/* masthead */}
        <div className="flex items-baseline justify-between">
          <span className="font-serif text-light-100 text-lg font-semibold">
            FoodAtlas
          </span>
          <span className="font-mono italic text-[9px] uppercase tracking-[0.22em] text-light-500">
            Weekly&nbsp;Bulletin
          </span>
        </div>
        <div className="border-t-2 border-double border-light-700/60" />

        {/* chip + headline */}
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded px-2 py-[2px] font-mono italic font-semibold text-[9px] tracking-[0.12em] uppercase text-light-900">
          No. Sun · Jun 30
        </span>
        <h3 className="font-serif text-light-100 text-xl leading-tight">
          1,284 new associations entered the atlas this week
        </h3>

        {/* stats tile row */}
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded px-2 py-[2px] font-mono italic font-semibold text-[9px] tracking-[0.12em] uppercase text-light-900 mt-1">
          At a glance
        </span>
        <div className="grid grid-cols-3 border-t-2 border-b-2 border-double border-light-700/60 py-3">
          {PREVIEW_STATS.map((s) => (
            <div key={s.label} className="text-center">
              <div className="font-serif text-light-100 text-xl leading-none">
                {s.value}
              </div>
              <div className="mt-1 font-mono italic text-[9px] uppercase tracking-[0.12em] text-light-500">
                {s.label}
              </div>
              <div className="mt-1 font-mono italic text-[10px] font-semibold text-accent-600">
                {s.delta}
              </div>
            </div>
          ))}
        </div>

        {/* highlights chip + list */}
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded px-2 py-[2px] font-mono italic font-semibold text-[9px] tracking-[0.12em] uppercase text-light-900 mt-1">
          Foods with most new discoveries
        </span>
        <div className="flex flex-col">
          {PREVIEW_HIGHLIGHTS.map((h, idx) => (
            <div
              key={h.name}
              className={`flex items-start justify-between gap-4 py-2.5 ${
                idx > 0 ? "border-t border-light-800" : ""
              }`}
            >
              <div className="min-w-0">
                <div className="font-serif text-light-100 text-sm">
                  {h.name}{" "}
                  <span className="text-accent-600 font-mono italic">→</span>
                </div>
                <div className="mt-1 font-mono italic text-[10px] text-light-400 truncate">
                  {h.chemicals}
                </div>
              </div>
              <div className="font-mono italic text-[10px] font-semibold text-accent-600 whitespace-nowrap pt-0.5">
                {h.count} new
              </div>
            </div>
          ))}
        </div>
        {/* bottom spacer — paper continues below the fade */}
        <div className="h-10" />
      </div>
      <div className="pointer-events-none absolute inset-x-0 bottom-0 h-24 bg-gradient-to-t from-light-950 to-transparent" />
    </div>
    {/* small paper meta caption */}
    <div className="mt-4 text-center font-mono italic text-[10px] uppercase tracking-[0.22em] text-light-600">
      A peek at last Sunday&apos;s bulletin
    </div>
  </div>
);

NewsletterSection.displayName = "NewsletterSection";

export default NewsletterSection;

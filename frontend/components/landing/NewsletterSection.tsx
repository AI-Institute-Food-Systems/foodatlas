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
      <div className="px-4 md:px-24">
        <div className="relative max-w-5xl mx-auto py-20 md:py-24">
          {/* top double rule — letterpress slip framing */}
          <div className="border-t-2 border-double border-light-700/60" />

          <div className="grid grid-cols-1 md:grid-cols-[1fr_auto] gap-10 md:gap-16 items-end py-10 md:py-14">
            {/* masthead */}
            <div className="flex flex-col gap-3">
              <span className="font-mono italic uppercase text-light-500 text-[11px] tracking-[0.22em]">
                Weekly Dispatch · No charge
              </span>
              <h2 className="font-serif text-3xl md:text-5xl text-light-100 leading-tight">
                The Weekly Bulletin
              </h2>
              <p className="font-serif italic text-light-400 text-base md:text-lg max-w-md">
                Dispatches from the FoodAtlas knowledge graph — new entities,
                fresh evidence, and the occasional methodology note, every
                Sunday.
              </p>
            </div>

            {/* subscription slip */}
            <form
              onSubmit={onSubmit}
              className="w-full md:w-[26rem] flex flex-col gap-3"
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

          {/* bottom double rule */}
          <div className="border-t-2 border-double border-light-700/60" />
        </div>
      </div>
    </section>
  );
};

NewsletterSection.displayName = "NewsletterSection";

export default NewsletterSection;

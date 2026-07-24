import { NextRequest, NextResponse } from "next/server";
import { SESv2Client, SendEmailCommand } from "@aws-sdk/client-sesv2";

import {
  REPORT_CATEGORIES,
  type ReportCategory,
  type ReportContext,
  type ReportRequestBody,
} from "@/types/Report";

const ses = new SESv2Client({
  region: process.env.AWS_REGION || "us-west-2",
});

// Serialise the auto-captured context as human-readable key/value lines
// so ops can eyeball the report without JSON-parsing it. Skip empty
// fields — the surface types are permissive on purpose (see Report.ts).
const formatContext = (ctx: ReportContext | undefined): string => {
  if (!ctx) return "(no context)";
  const entries = Object.entries(ctx).filter(
    ([, v]) => v !== undefined && v !== null && v !== "",
  );
  return entries.map(([k, v]) => `  ${k}: ${v}`).join("\n");
};

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as ReportRequestBody;
    const { category, description, email, context, pageUrl } = body;

    if (!category || !description) {
      return NextResponse.json(
        { error: "Category and description are required" },
        { status: 400 },
      );
    }

    if (!REPORT_CATEGORIES.includes(category as ReportCategory)) {
      return NextResponse.json(
        { error: "Unknown category" },
        { status: 400 },
      );
    }

    // Same CONTACT_EMAIL fan-out convention as /contact/send — reports
    // land in the same team inbox, just under a different subject prefix.
    const recipients = JSON.parse(process.env.CONTACT_EMAIL!) as string[];

    // Reply-To lands the reporter's address on the email if they gave
    // one. When they didn't, we skip Reply-To entirely rather than
    // sending a reply header pointing at contact@foodatlas.ai — the
    // FROM address on this account isn't a real mailbox.
    const replyTo = email && email.trim() ? [email.trim()] : undefined;

    const contextBlock = formatContext(context);
    const kind = context?.kind ?? "unknown-surface";

    await ses.send(
      new SendEmailCommand({
        FromEmailAddress: `FoodAtlas Report <${process.env.CONTACT_FROM_EMAIL}>`,
        Destination: { ToAddresses: recipients },
        ...(replyTo ? { ReplyToAddresses: replyTo } : {}),
        Content: {
          Simple: {
            Subject: { Data: `[FoodAtlas Report] ${category} — ${kind}` },
            Body: {
              Text: {
                Data:
                  `Category: ${category}\n` +
                  `Surface: ${kind}\n` +
                  (pageUrl ? `Page: ${pageUrl}\n` : "") +
                  (email && email.trim()
                    ? `Reporter: ${email.trim()}\n`
                    : "Reporter: (anonymous)\n") +
                  `\n--- Context ---\n${contextBlock}\n` +
                  `\n--- Reporter description ---\n${description}\n`,
              },
            },
          },
        },
      }),
    );

    return NextResponse.json({ success: true });
  } catch (error) {
    console.error("SES send failed:", error);
    return NextResponse.json(
      { error: "Failed to send report" },
      { status: 500 },
    );
  }
}

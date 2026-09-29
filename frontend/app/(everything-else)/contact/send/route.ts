import { NextRequest, NextResponse } from "next/server";

import {
  LIMITS,
  badRequest,
  crossSite,
  isCrossSite,
  isEmail,
  misconfigured,
  recipients,
  str,
} from "@/utils/formGuard";
import { SESv2Client, SendEmailCommand } from "@aws-sdk/client-sesv2";
import { API_ACCESS_TOPIC } from "@/utils/apiAccessFields";
import { apiAccessBlock, parseApiAccess } from "@/utils/apiAccessGuard";

const ses = new SESv2Client({
  region: process.env.AWS_REGION || "us-west-2",
});

const KNOWN_TOPICS = [
  "General Inquiry",
  API_ACCESS_TOPIC,
  "Data Issue",
] as const;

export async function POST(request: NextRequest) {
  try {
    if (isCrossSite(request)) return crossSite();

    const body: unknown = await request.json();
    const { name, email, affiliation, topic, message, apiAccess } = (body ??
      {}) as Record<string, unknown>;

    // Bounded server-side. The caps previously existed only as maxLength on
    // the client inputs, so a direct POST could send arbitrary volumes of text
    // into the team inbox.
    const nameV = str(name, LIMITS.name);
    const emailV = str(email, LIMITS.email);
    const messageV = str(message, LIMITS.message);
    const affiliationV = str(affiliation, LIMITS.affiliation, {
      required: false,
    });

    if (!nameV || !emailV || !messageV || affiliationV === null) {
      return badRequest("Name, email, and message are required");
    }
    // Unvalidated, this went straight into ReplyToAddresses, where SES
    // rejects it and the failure surfaces as a 500.
    if (!isEmail(emailV)) return badRequest("Invalid email address");

    const topicLabel = KNOWN_TOPICS.includes(
      topic as (typeof KNOWN_TOPICS)[number],
    )
      ? (topic as string)
      : "General Inquiry";

    // API requests carry the structured answers the PI reviews; without them
    // (or an affiliation) the request can't be triaged, so refuse it here
    // rather than forwarding a half-filled row.
    let apiAccessSection = "";
    if (topicLabel === API_ACCESS_TOPIC) {
      const apiAccessV = parseApiAccess(apiAccess);
      if (!apiAccessV || !affiliationV) {
        return badRequest(
          "API access requests need an affiliation and all API access fields",
        );
      }
      apiAccessSection = apiAccessBlock(apiAccessV);
    }

    // CONTACT_EMAIL is a JSON array so ops can fan-out to multiple
    // recipients without touching code.
    const to = recipients();
    if (!to) return misconfigured();

    const affiliationLine = affiliationV
      ? `Affiliation: ${affiliationV}\n`
      : "";

    await ses.send(
      new SendEmailCommand({
        FromEmailAddress: `FoodAtlas Contact Form <${process.env.CONTACT_FROM_EMAIL}>`,
        Destination: { ToAddresses: to },
        ReplyToAddresses: [emailV],
        Content: {
          Simple: {
            Subject: { Data: `[FoodAtlas: ${topicLabel}] from ${nameV}` },
            Body: {
              Text: {
                Data:
                  `Name: ${nameV}\n` +
                  `Email: ${emailV}\n` +
                  affiliationLine +
                  `Topic: ${topicLabel}\n\n` +
                  messageV +
                  apiAccessSection,
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
      { error: "Failed to send message" },
      { status: 500 },
    );
  }
}

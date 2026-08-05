import { NextRequest, NextResponse } from "next/server";
import { SESv2Client, SendEmailCommand } from "@aws-sdk/client-sesv2";

const ses = new SESv2Client({
  region: process.env.AWS_REGION || "us-west-2",
});

const KNOWN_TOPICS = [
  "General Inquiry",
  "API Access Request",
  "Data Issue",
] as const;

export async function POST(request: NextRequest) {
  try {
    const { name, email, affiliation, topic, message } = await request.json();

    if (!name || !email || !message) {
      return NextResponse.json(
        { error: "Name, email, and message are required" },
        { status: 400 },
      );
    }

    const topicLabel = KNOWN_TOPICS.includes(topic)
      ? topic
      : "General Inquiry";

    // CONTACT_EMAIL is a JSON array so ops can fan-out to multiple
    // recipients without touching code. Parse defensively — malformed
    // JSON aborts with a 500 rather than a silent single-recipient send.
    const recipients = JSON.parse(process.env.CONTACT_EMAIL!) as string[];

    const affiliationLine = affiliation
      ? `Affiliation: ${affiliation}\n`
      : "";

    await ses.send(
      new SendEmailCommand({
        FromEmailAddress: `FoodAtlas Contact Form <${process.env.CONTACT_FROM_EMAIL}>`,
        Destination: { ToAddresses: recipients },
        ReplyToAddresses: [email],
        Content: {
          Simple: {
            Subject: { Data: `[FoodAtlas: ${topicLabel}] from ${name}` },
            Body: {
              Text: {
                Data:
                  `Name: ${name}\n` +
                  `Email: ${email}\n` +
                  affiliationLine +
                  `Topic: ${topicLabel}\n\n` +
                  message,
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

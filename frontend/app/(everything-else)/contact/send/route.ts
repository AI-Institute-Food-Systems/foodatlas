import { Resend } from "resend";

export async function POST(req: Request) {
  const resend = new Resend(process.env.RESEND_API_KEY);
  const { name, email, affiliation, topic, message } = await req.json();

  try {
    const data = await resend.emails.send({
      from: `FoodAtlas Contact Form <${process.env.EMAIL_FROM}>`,
      reply_to: email,
      to: process.env.EMAIL_TO?.split(',') || [],
      subject: `[FoodAtlas: ${topic}] ${message}`,
      text: `New message from: ${name}, affiliated with ${affiliation}, from ${email}, about ${topic}: ${message}`,
    });

    return Response.json(data);
  } catch (error) {
    return Response.json({ error });
  }
}

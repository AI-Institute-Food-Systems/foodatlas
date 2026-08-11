# Contact form — PTO handover (Kaichi)

Covers foodatlas.ai/contact and the in-app "Report an issue" modal while Lukas is out.

## How it works

Both forms POST to a Next.js route on Vercel (`food-atlas` project) that sends one email via
AWS SES (AIFS account `055907695769`, us-west-2). Nothing is stored — **if the email doesn't
arrive, the message is gone.** Recipients come from the `CONTACT_EMAIL` env var (a JSON array);
as of 2026-08-10 that's Pranav, Kaichi, Fangzhou, and Lukas — every message goes to all four,
so say in the thread when you've picked one up to avoid four people replying at once.

| Source | Subject line |
| --- | --- |
| `/contact` form | `[FoodAtlas: <Topic>] from <Name>` |
| Report-an-issue modal | `[FoodAtlas Report] <category> — <surface>` |

**Replying:** just hit Reply — the sender's address is on `Reply-To`. Your reply goes from your
own mailbox. `contact@foodatlas.ai` is a send-only identity with no inbox, so never expect
replies there and don't promise it as a contact address.

## Triage by topic

**General Inquiry** — answer if you can. Anything about roadmap, partnerships, funding, or
data licensing: acknowledge ("Lukas is out until <date>, he'll follow up") and park it.

**Data Issue** and all `[FoodAtlas Report]` mail — open a GitHub issue in `foodatlas` with the
email pasted in, label it, and reply to the reporter that it's logged. No data fixes needed
while Lukas is out unless the site is visibly broken.

**API Access Request** — **don't issue keys.** Key issuance needs prod AWS access that only
Lukas has. Acknowledge, then park the email in a folder so nothing gets lost:

> Thanks for your interest in the FoodAtlas API. Lukas handles key issuance and is out until
> <date> — I've put you at the front of the queue and you'll hear from him shortly after.
> In the meantime the endpoints and response shapes are documented at
> foodatlas.ai/food-composition-api.

Forward the whole batch to Lukas when he's back.

## If the form breaks (500 on submit)

Check Vercel → `food-atlas` → Logs for `SES send failed`. In order of likelihood:

1. **`CONTACT_EMAIL` is malformed JSON.** It must parse as an array, e.g.
   `["lmasopust@ucdavis.edu","kcxie@ucdavis.edu"]`. A stray quote 500s every send.
2. **OIDC Federation got re-enabled** on the Vercel project (Settings → Advanced). It must
   stay **off** — it hijacks the AWS SDK credential chain and SES rejects the request.
   This exact bug took the form down for four days in July.
3. **AWS keys rotated.** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in Vercel belong to
   the shared `aifs-mailer` IAM user in the AIFS account.

Any env var change needs a **redeploy** to take effect. This is a config problem, not a code
problem — don't ship a code fix for it.

## For Lukas

Working the API-key queue on return:

```
cd backend/api
uv run python scripts/issue_public_key.py
```

Prompts for the requester's email + notes, then prints the plaintext key and an
`aws secretsmanager` one-liner — run it with `--profile foodatlas-prod-admin`. It merges into
`foodatlas/public-api-keys` (account `030635937737`, us-west-1). Live within ~5 min, no
redeploy. Only the sha256 hash is stored, so a lost key means issuing a new one; revoking is
deleting that hash's entry. Requesters get `Authorization: Bearer <key>` on `/v1/*`, 60 req/min
sustained with a burst of 10.

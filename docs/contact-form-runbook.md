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

Working the API-key queue on return. All three commands run from `backend/api` and take
`--profile foodatlas-prod-admin`; they read and write `foodatlas/public-api-keys`
(account `030635937737`, us-west-1), which is the **only** record of who holds a key.

See who has one:

```
uv run python -m scripts.keys list --profile foodatlas-prod-admin
```

Issue one:

```
uv run python -m scripts.keys issue --profile foodatlas-prod-admin --api-url https://api.foodatlas.ai
```

It prompts for the requester's email and notes, writes the record compare-and-swap, reads it
back to confirm it landed, then polls `/v1/stats` until twelve consecutive 200s — and only
then prints the plaintext key. **Wait for it.** Prod runs two Fargate tasks, each with its own
in-process key cache on an independent 300s timer, and the ALB round-robins between them; for
up to one refresh interval after the merge a new key fails *intermittently*, not cleanly.
Emailing it during that window makes the recipient's first calls 401 at random, which reads as
a broken key. Worst case the command waits ~5 minutes. If it gives up, it still prints the key
with a warning — re-check later with `scripts.keys probe <key> --api-url https://api.foodatlas.ai`.

Revoke one (by key prefix, email, or full hash — take the value from `list`):

```
uv run python -m scripts.keys revoke aaaa1111 --profile foodatlas-prod-admin
```

Revoking flips the record to `revoked` rather than deleting it, so the ledger keeps the history
of who once had access. The key stops working within one refresh interval.

Only the sha256 hash and an 8-character prefix are stored, never the key itself — a lost key
means issuing a new one. Requesters get `Authorization: Bearer <key>` on `/v1/*`, 60 req/min
sustained with a burst of 10.

## Who is actually using the API

Every `/v1/*` request writes one JSON line to the API's CloudWatch log group (six-month
retention), tagged `"log": "v1_access"` and attributed to the key's email. Logs Insights →
log group `/aws/ecs/...ApiLogGroup...`.

Requests per person per day:

```
fields @timestamp, email
| filter log = "v1_access"
| stats count(*) as requests by email, bin(1d)
| sort requests desc
```

What one key is hitting. Filter by `email`, not `key_prefix`: keys issued before the ledger
existed have no prefix (it derives from the plaintext, which was never stored), so they log
`key_prefix` as empty while still being attributed by email.

```
fields @timestamp, route, status
| filter log = "v1_access" and email = "alice@u.edu"
| stats count(*) as calls by route, status
| sort calls desc
```

Rejected traffic — repeated 401s from one source usually means a key was emailed before it went
live, or someone is guessing:

```
fields @timestamp, client_ip, key_prefix
| filter log = "v1_access" and status = 401
| stats count(*) as attempts by client_ip, key_prefix
| sort attempts desc
```

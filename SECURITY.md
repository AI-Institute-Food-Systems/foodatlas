# Security Policy

## Reporting a vulnerability

Please report security issues privately. Do not open a public GitHub issue for
anything that could be exploited before it is fixed.

- **Preferred:** [GitHub private vulnerability reporting](https://github.com/AI-Institute-Food-Systems/foodatlas/security/advisories/new)
- **Email:** aifs@ucdavis.edu — put "FoodAtlas security" in the subject

Please include what you found, the steps to reproduce it, and what an attacker
could do with it. A proof of concept helps. If you need to share something
sensitive, say so and we will arrange a channel.

We aim to acknowledge a report within five working days.

## Scope

In scope:

| Surface | Where |
| --- | --- |
| Website | `https://www.foodatlas.ai` |
| Public API | `https://api.foodatlas.ai/v1` |
| Data bundles | `https://www.foodatlas.ai/food-composition-downloads` |
| This repository | frontend, backend API, DB loader, CDK infrastructure |

Out of scope:

- `dev.foodatlas.ai` — a preview deployment behind Vercel SSO, not a product
- Denial of service, volumetric or otherwise. Please do not test it
- Automated scanner output with no demonstrated impact
- Findings that require a compromised account or physical access
- Missing hardening headers on endpoints that serve no sensitive data, unless
  you can show an exploit

## Testing courtesies

The API is rate limited and the site sits behind a bot filter. If you need room
to test, tell us first rather than working around them. Please use your own
API key, keep request volume low, and do not access, modify or exfiltrate data
belonging to anyone else.

## What this project holds

FoodAtlas serves public research data — food composition and bioactivity
evidence drawn from published literature and public databases. There is no
end-user personal data in the knowledge graph. The parts worth protecting are
API credentials, the contact-form pipeline, and the integrity of the data
itself: a silent change to an attestation is a more serious outcome here than
an outage.

## Disclosure

We will keep you updated while we work on a fix and will credit you when it
ships, unless you would rather stay anonymous. Please give us a chance to
release a fix before publishing.

## Supported versions

FoodAtlas is a continuously deployed service. Only the version currently live
in production is supported; there are no maintained release branches. Data
bundles are versioned and published, but older bundles do not receive fixes.

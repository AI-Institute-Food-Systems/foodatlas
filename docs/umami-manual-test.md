# Umami events — manual test script

Prod-only: the tracker is gated on `VERCEL_ENV === "production"` and the API sink on `API_UMAMI_WEBSITE_ID`, so nothing here works on a preview deploy or localhost (locally, client events print `[umami] <name> {…}` in the browser console instead — that is the pre-merge check).

Where to look: `https://umami.aifs.ucdavis.edu` → website **FoodAtlas** → **Realtime** for the live feed, **Events** for per-name counts, **Properties** for the per-event breakdown. Realtime lags a few seconds; Events/Properties can lag a minute.

Before starting, note the **Views** and **Visitors** numbers on Overview (today). Neither should move because of anything below except the two pageviews you cause yourself (steps 8, 9).

| # | Event | Do | Expect in umami |
|---|---|---|---|
| 1 | `bundle_download` | On `www.foodatlas.ai/food-composition-downloads` click **Download** on any row. | Zip starts downloading. Realtime: `bundle_download`. Properties: `version`, `kgc_run`, `file_size` match the row. Session is *yours* (same visitor as your pageviews), not a new one. |
| 1b | `bundle_download` 404 | Open `www.foodatlas.ai/food-composition-downloads/v0.0` | JSON `{"error":"unknown version"}`, HTTP 404. No event. |
| 2 | `search_select` | On the home page type `tomato`, click the **Tomato** suggestion. Then type `vitamin c`, arrow-down to a suggestion, press **Enter**. | Two `search_select` events. Properties: `query` = `tomato` / `vitamin c` (lowercased, trimmed), `entity_type`, `id` (`e…`). |
| 3 | `search_no_results` | Type `zzzqqq` and wait ~2 s without typing. Then keep typing `zzzqqqx`, wait again. Then clear and type `zzzqqq` again. | Exactly two events (`zzzqqq`, `zzzqqqx`); the repeat of `zzzqqq` is not re-reported. Nothing fires while you are still typing. |
| 4 | `contact_submit` | `www.foodatlas.ai/contact` → topic **Data Issue**, fill the required fields, send. | One event, Properties `topic` = `Data Issue`, `outcome` = `sent`. Open the raw event (Realtime → click) and confirm no name/email/message anywhere. |
| 5 | `issue_report_submit` | On any entity page open the report FAB (bottom-right), pick a row, submit. | One event, `outcome` = `sent`. |
| 6 | `tab_switch` | On `www.foodatlas.ai/food/tomato` click **Bioactivities**, then **Overview**. Repeat on `/chemical/quercetin`. | Four events. Properties: `entity_type` ∈ {food, chemical}, `tab` = tab id. No entity name/id present. |
| 7 | `outbound_link` | `www.foodatlas.ai/about` → click a team member's website or LinkedIn icon. On a food page, click a PubMed/FDC source link in an evidence table. | One `outbound_link` per click, Properties `host` = `linkedin.com` / `pubmed.ncbi.nlm.nih.gov` etc. Full URL never appears. |
| 8 | `error_page` (404) | Open `www.foodatlas.ai/food/this-does-not-exist-xyz`. | `error_page`, `kind` = `not_found`, `path` = `/food/this-does-not-exist-xyz`. One pageview counted (that's expected). |
| 9 | `error_page` (error) | Hard to force in prod without breaking something; skip unless one is already reproducible. If you have one: `kind` = `error`. | — |
| 10 | `api_fetch_error` | Needs the API to fail from the browser. Easiest: DevTools → Network → **Offline**, then switch a tab on an entity page you have not opened before. | `api_fetch_error`, `status` = `network`, `path` = endpoint with ids collapsed to `{id}` (e.g. `/_proxy-api/food/{id}/…` or `/food/bioactivity`). Query string stripped. Re-enable network afterwards. |

## API usage (`api_request`)

Needs a **public** `/v1` key (`cd backend/api && uv run python -m scripts.keys list` shows prefixes; issue one with `issue` if needed). `KEY` below is that key; `PREFIX` its first 8 characters.

```
curl -s -A python-requests/2.31 -H "Authorization: Bearer KEY" https://api.foodatlas.ai/v1/stats
curl -s -A python-requests/2.31 -H "Authorization: Bearer KEY" https://api.foodatlas.ai/v1/foods/1
```

| # | Do | Expect |
|---|---|---|
| A1 | the two curls above | Realtime: two `api_request`. Properties: `route` = `/v1/stats`, `/v1/foods/{food_id}`; `key_prefix` = PREFIX; `org` = the key's org; `ua` = `python-requests/2.31`; `status` = 200; `method` = GET. |
| A2 | Insights → filter **Tag** = PREFIX | The same two events, nothing else. |
| A3 | Overview → Visitors | Unchanged, or +1 the very first time the API posts (one synthetic session for *all* API traffic). Never +1 per key. |
| A4 | same curl with `Bearer wrong-key` | 401 from the API; **no** event (401s are not mirrored). |
| A5 | `curl -s https://api.foodatlas.ai/health` | 200; no event. |
| A6 | Frontend-driven traffic: reload `www.foodatlas.ai/food/tomato` twice | No new `api_request` (internal key is filtered). |
| A7 | Bot-filter control — post one raw event with a bot UA: | Must **not** appear in Realtime. If it does, `DISABLE_BOT_CHECK` is set on the umami VM and the synthetic UA in `umami_sink.py` is redundant (harmless). |

A7 command (website id is the prod one from `frontend/utils/umami.ts`):

```
curl -s -A curl/8.4 -H 'Content-Type: application/json' -H 'User-Agent: curl/8.4' \
  -d '{"type":"event","payload":{"website":"a63b88b0-aa17-4ca1-a3c6-62a568fe0757","hostname":"api.foodatlas.ai","url":"/control","name":"bot_control","userAgent":"curl/8.4"}}' \
  https://umami.aifs.ucdavis.edu/api/send
```

## Failure behaviour

| # | Do | Expect |
|---|---|---|
| F1 | Stop umami on the VM (`docker compose stop umami` in `aifs-monitoring`), then click **Download** on the downloads page | Zip still downloads; the redirect takes ≤ 1.5 s longer than usual. |
| F2 | With umami still stopped, run the A1 curls | Both return 200 in normal time. CloudWatch API log group: one `umami_sink: send to … failed` warning, not one per request. |
| F3 | Start umami again, run A1 once more | Events flow again; the drop is not backfilled (by design — CloudWatch `v1_access` lines are the record). |

## Sign-off

- Every row above observed once → done.
- Overview **Views** moved only by the pageviews you caused (steps 1, 4, 5, 6, 7, 8 each load a page; the events themselves add zero).
- Optional after a day: umami VM CPU / Postgres size unchanged in shape; if not, set `API_UMAMI_SAMPLE_RATE` (e.g. `0.2`) on the API task definition.

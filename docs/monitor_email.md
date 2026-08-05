# GDACS & PDC monitor email

The storm monitoring email sent from this repo. Unlike the rest of `docs/`,
this documents something we *operate* rather than an external API we consume.

| | |
|---|---|
| Entry point | `scripts/daily_gdacs_monitor_email.py` |
| Rendering | `src/gdacs_monitor_email.py` |
| Workflow | `.github/workflows/daily-gdacs-monitor-email.yml` |
| Schedule | `20 3,9,15,21 * * *` — 4×/day |
| Recipients | Listmonk list **101**, "Storm Alerts - Monitoring" |
| Verified | 2026-08-05 |

**This email ships from the exploratory repo, not the production alerting
path.** `ds-storms-alerts` is the hardened system. This one exists because the
harmonisation work needs a way to see live storms against historical context,
and it is deliberately marked as such — see the `[test]` prefix below.

## Why 03/09/15/21 UTC + 20 minutes

The cron fires 20 minutes after each NHC/NOAA Tropical Cyclone Message (TCM)
synoptic cycle, which gives GDACS time to ingest the bulletin. Sending on the
synoptic cycle itself would race the upstream data.

## The two GDACS query traps

Both are encoded as named constants with comments. They are the kind of thing
that fails silently, so they are repeated here.

**1. Alert level must be passed explicitly.** When the `alertlevel` parameter
is *omitted*, GDACS returns orange and red events only — green is dropped with
no indication. Alert level scores GDACS's own impact estimate rather than storm
intensity, so green is not "minor": on 2026-08-03 both currently-active storms
were green, one a Category 2 typhoon with 1.3M exposed in Japan, and this email
reported no active storms at all.

```python
ALERT_LEVELS_ALL = ["green", "orange", "red"]   # never rely on the default
```

The shared default in `src/datasets/gdacs.py` was deliberately **not** changed,
because other callers do want the orange/red subset. It is a per-caller
decision — be deliberate about which you want.

**2. The query must be date-bounded.** With all three alert levels, the
unfiltered call returns exactly 100 events — the per-page cap, with no
pagination available — so it is *silently truncated*. `LOOKBACK_DAYS = 30`
stays well under the cap and cannot miss a currently-active storm, whose start
date is necessarily recent.

## PDC is queried live, not from the capture archive

The 3-hourly poller (`scripts/poll_pdc_cyclones.py`) exists to build a
historical record. The email does **not** read it. PDC publishes at bulletin
issuance while the poller runs every 3 hours, so the archive can be up to 3 h
stale at send time. Archive-building and alert freshness are different jobs.

## PDC failures fail the run

`fetch_pdc_for_storms()` raises rather than degrading. An earlier version
caught everything and continued without PDC, which meant a missing
`PDC_API_KEY` produced an email indistinguishable from one where PDC genuinely
had no data for the storm — and that shipped for several sends before anyone
noticed.

The distinction the code enforces:

- **Absence is information.** A storm PDC does not carry, or carries with no
  exposure computed, is a real state of the world and is reported as such.
- **Failure is an error.** A missing key or an unreachable API raises.

`--allow-missing-pdc` opts into degradation when PDC is known to be down and
the GDACS content is still wanted. Degradation is deliberate, never default.

## The `[test]` prefix is intentional — do not remove it

`TEST_PREFIX = "[test] "` is applied to **both** the campaign name and the
subject, and each does a different job:

- the **name** prefix triggers OCHA Listmonk's test-variant template (Go side)
- the **subject** prefix lets recipients filter these into a dedicated folder

Setting it to `""` is the prod switch, and it changes which template Listmonk
renders. It is not leftover scaffolding.

## Running it

```bash
uv run python scripts/daily_gdacs_monitor_email.py --dry-run   # HTML to disk, no Listmonk
uv run python scripts/daily_gdacs_monitor_email.py --inspect   # draft campaign + preview, not sent
uv run python scripts/daily_gdacs_monitor_email.py --auto-send # headless; skips the confirm prompt
```

| Flag | Effect |
|---|---|
| `--dry-run` | Writes HTML to `artefacts/daily_email_previews/`. No Listmonk call at all. |
| `--inspect` | Creates a **draft** campaign, prints resolved recipients, opens the server-rendered preview. Leaves the draft unsent. |
| `--list-id N` | Target a different Listmonk list. Default is `MONITOR_LIST_ID`. |
| `--allow-missing-pdc` | Send GDACS-only when PDC is unavailable. Off by default. |
| `--auto-send` | Skip `ocha-relay`'s interactive type-the-name confirmation. Required in CI. |

Secrets the workflow needs: `DSCI_AZ_BLOB_DEV_SAS`, `DSCI_LISTMONK_BASE_URL`,
`DSCI_LISTMONK_API_USERNAME`, `DSCI_LISTMONK_API_KEY`, `PDC_API_KEY`.

## Past incident worth not repeating

The workflow's `list_id` input once carried `default: "25"`. A non-empty
workflow input is always passed through as `--list-id`, so it **overrode**
`MONITOR_LIST_ID` on every scheduled run and campaign 1269 went to the wrong
list. It was caught only by checking which list the campaign actually hit, not
from any error.

The input now defaults to `""` and the override is applied only when non-empty.
Verify the destination on the campaign itself after any change here — the
script's constant is not the last word.

## What the panel shows

A ridgeline per country: one density ridge per wind threshold (34 / 64 kt)
showing that country's historical exposure, a dot for the current storm, and
PDC as a stacked damage-class bar anchored to its total on the shared
population axis.

PDC is deliberately **not** a dot on a ridge. Its damage classes are a modelled
damage ratio, proven not to be wind rings (`pdc_api.md`), so placing it on a
threshold ridge would assert a wind mapping that does not exist.

The framing: GDACS answers *how many* and, with 25 years of history, *how
unusual*. PDC answers *how bad*.

## See also

- [`pdc_api.md`](pdc_api.md) — PDC endpoints, the retention model, lag
- [`gdacs_adam_wind_footprint.md`](gdacs_adam_wind_footprint.md) — why GDACS
  reads systematically higher than CHD
- `book/11-pdc-2026-season.qmd` — how PDC came to be in this email
- `docs/decisions/0005-capture-pdc-cyclones-without-integrating-them.md`

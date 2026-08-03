# GDACS / ADAM wind-footprint methodology

Why GDACS (and therefore ADAM) reports systematically **more** exposed population
than CHD for the same storm. Sources verified **2026-07-31** — see
[How this was verified](#how-this-was-verified) to re-crawl cheaply.

## The claim

> Both CHD and GDACS start from the **same** NHC/JTWC advisory wind radii. GDACS
> keeps only the **maximum** of the four quadrant radii and sweeps it as a
> **symmetric circle**; CHD interpolates all four quadrants into an **asymmetric
> polygon**. A max-radius circle always encloses the quadrant polygon it was built
> from, so **CHD ≤ GDACS is structural, not incidental**.

This is the mechanism behind the median CHD/GDACS ratio of ~0.5 in book chapter
[09](../book/09-source-comparison.qmd).

## Primary source — cite this one

European Commission, Joint Research Centre. Masante, D., Barantiev, D., Destro, E.,
Mastronunzio, M., Paris, S., Proietti, C., Salvitti, V. & Santini, M.
*Global Disaster Alert and Coordination System (GDACS) Multi-hazard Early Warning
System.* Publications Office of the European Union, Luxembourg, 2025.
**doi:10.2760/1461943 · JRC141661**

> "Although the bulletins list wind field information for each of the four cardinal
> quadrants (describing an asymmetrical wind field), the GDACS system takes **only
> the maximum of the four values, discarding the rest**, to build a maximalist
> symmetrical wind field. A custom algorithm modulates the wind radii data to
> prevent large jumps between timesteps."

| | |
|---|---|
| PDF | `https://www.gdacs.org/documents/2025/GDACS_MHEWS_guide.pdf` |
| Locator | §"Tropical cyclones", para. following the SSHS classification note |
| PDF revision fetched | dated **22/07/2026** — an updated version of the 2025 manual, served from the same `/2025/` URL |
| SHA-256 | `b1eb7f23ab71dd6dcc5aae6d8697061dab75de7a695e9d5a15df369da5bcd2de` (1,635,813 bytes) |
| Wayback | snapshot exists `20251113165914` — **predates** the revision we read |

⚠️ **The URL is not stable content.** A document labelled 2025, at a `/2025/`
path, was silently revised in 2026. Always re-hash before assuming the quote is
still at the same place; cite the DOI/JRC number, not the URL.

## Corroborating source — better on *intent*

Vernaccini, L., De Groeve, T. & Gadenz, S. *Humanitarian Impact of Tropical
Cyclones.* JRC, Institute for the Protection and Security of the Citizen.
**EUR 23083 EN, 2007.**

> "Some organisations (TSR and NHC) use all four forecasted radii to draw irregular
> shapes (asymmetric winds buffer). PDC takes the minimum radius to draw a circle
> around the track point. However, **at JRC we take the maximum radius** to draw a
> circle around the track."

> "**To be conservative on the number of people affected, JRC takes the maximum
> value.**"

> "PDC and JRC use symmetric buffers, whereby PDC uses the minimum radius and JRC
> the maximum radius. … while **for JRC the affected area is overestimated**."

| | |
|---|---|
| PDF | `https://publications.jrc.ec.europa.eu/repository/bitstream/JRC42518/reqno_jrc42518_humanitarian%20impact%20of%20tropical%20cyclones%20(final)%5B2%5D.pdf` |
| Landing page | `https://publications.jrc.ec.europa.eu/repository/handle/JRC42518` |
| Locator | §2.2 "Wind buffers", around Figures 3–4 |
| SHA-256 | `c0de2e91a519e47b2081e005ee3ddd9d5ce416a69377c0e3897365a9be0ab4ac` (3,189,365 bytes) |
| Wayback | **no snapshot** — worth submitting one |

This is the stronger card in a room with JRC/WFP present: the overestimate is
**deliberate and self-declared**, which reframes "CHD reads half" from an
accusation into a documented design difference. Note the 2007 report contains an
internal inconsistency in its own worked example (it says PDC would take 30 from
`{25,45,10,30}`, where the minimum is 10) — quote the rule, not that example.

## ⚠️ Do NOT cite for this claim

**Probst, P. & Annunziato, A., "Tropical Cyclones in GDACS: Data sources",
EUR 28331 EN, 2016, doi:10.2788/504291.**

Full text extracted and searched (1,015 lines, 2026-07-31): it **does not**
describe the max-radius circle. It only *defines* wind radii ("the maximum radial
extent … of winds reaching 34, 50 and 64 knots in each quadrant"), and every
"circle" passage concerns **NHC forecast cones**, not wind buffers.

It remains a good citation for: GDACS's data sources (NOAA + JTWC bulletins
scraped ~every 30 min), the 34/50/64 kt thresholds, and the storm-surge
(Holland + HyFlux2) / rainfall chain.

**Two places in this repo currently mis-attribute the max-radius claim to it** and
should be corrected:
- `book/07-exposure-method-comparison.qmd` §A.9
- `artefacts/01_merge_cerf_exposure/gdacs_endpoint_comparison.md` §6

## Worked example — Franklin 2023 over Hispaniola

The cleanest demonstration in the record, because it has a built-in control.
Hurricane Franklin (`AL082023`) tracked up the **eastern** side of Hispaniola.
Every advisory during the crossing (advisories 9–15) carried 34 kt radii of
**NE 100, SE 100, SW 0, NW 0 nm** — Haiti sits in the two zero quadrants.

| geometry | covers Haiti | covers Dom. Rep. |
|---|---|---|
| NHC's own advisory wind field | **0.0%** | 53.1% |
| CHD (asymmetric quadrants) | **0.0%** | 57.6% |
| GDACS method (max radius, circle) | **45.2%** | 87.1% |

Exposure at 34 kt follows directly:

| | CHD | GDACS | ADAM |
|---|---|---|---|
| Dominican Republic | 9.14M | 9.30M | 9.59M |
| **Haiti** | **0** | **5.87M** | **6.48M** |

The Dominican Republic is the control: all three agree within 2%, so CHD plainly
*has* the storm — the Haitian zero is geometry, not a coverage gap.

**The result that cuts against us:** we expected NHC's own published wind field to
resemble the GDACS circle and support the larger figure. It does not — NHC's
initial-radii polygon covers **0% of Haiti**, the same as CHD. Built from NHC's
GIS archive (`al082023_fcst_001..020`, `*_initialradii.shp`, `RADII == 34`,
unioned).

**But neither number is verified.** The NHC Tropical Cyclone Report for Franklin
(`https://www.nhc.noaa.gov/data/tcr/AL082023_Franklin.pdf`) states that reliable
stations in the *Dominican Republic* recorded **no sustained tropical-storm-force
winds** (peak gust 45 kt at Barahona, landfall intensity revised down to 40 kt),
and that **no wind or rainfall data are available for Haiti**. NHC did issue
tropical-storm warnings for Haiti's south coast. So the max-radius rule converts
*an absence of data* in two quadrants into *a presence of population*, and on this
storm nobody can adjudicate.

Reproduce: `uv run python artefacts/slides/franklin_case.py` (writes
`artefacts/slides/cache/franklin_stats.json`).

**Melissa 2025 is not this case** — its wind field was broad and NE-dominant
(`[170, 130, 60, 80]`), so CHD did place 618k in Haiti against GDACS's 1.22M: a
2× gap, not a miss.

## ADAM — not documented

WFP states only that ADAM "pulls information from a variety of sources, including
WFP's databases and the European Commission Joint Research Centre"
([FAO/KORE](https://www.fao.org/in-action/kore/news-and-events/news-details/en/c/885017/)).
**No public documentation of how ADAM constructs its wind footprint was found.**

That ADAM reuses the GDACS buffer is **inference from correlation** (adm0 log-r
0.94, median ratio 0.95, ~90% of ADAM storms inside the GDACS positive set), not
a citable fact. Flag it as inference in any external-facing product. Open
question for WFP — it decides whether the AAC has two independent estimates or
three.

## How this was verified

Reproduce in ~2 minutes:

```bash
# 1. the two supporting sources
curl -sL -o mhews.pdf  "https://www.gdacs.org/documents/2025/GDACS_MHEWS_guide.pdf"
curl -sL -o humimp.pdf "https://publications.jrc.ec.europa.eu/repository/bitstream/JRC42518/reqno_jrc42518_humanitarian%20impact%20of%20tropical%20cyclones%20(final)%5B2%5D.pdf"
for f in mhews humimp; do pdftotext -layout $f.pdf $f.txt; done
grep -niE "maximum of the four|minimum radius|maximum radius|symmetric" mhews.txt humimp.txt

# 2. the negative result — confirm EUR 28331 EN does NOT support it
curl -sL -o jrc2016.pdf "https://publications.jrc.ec.europa.eu/repository/bitstream/JRC104836/2016_tc_data_in_gdacs_final(1).pdf"
pdftotext -layout jrc2016.pdf jrc2016.txt
grep -niE "maximum|quadrant|symmetric|circle" jrc2016.txt   # all hits are forecast cones
```

Search queries that worked (WebSearch, 2026-07-31):
- `GDACS tropical cyclone wind buffer maximum radius circle quadrant radii methodology JRC` → surfaced the MHEWS guide + JRC42518 in the first result set
- `Probst Annunziato "Tropical Cyclones in GDACS" EUR 28331 EN 2016 data sources` → the 2016 report

Dead ends, so you can skip them: `gdacs.org/knowledge/models_tc.aspx` (mentions
only the Holland/Monte-Carlo storm-surge model, nothing on buffer geometry);
WebFetch on the JRC PDFs returns unusable text — download and `pdftotext` instead.

## What's NOT here

- CHD's own buffer construction — `ocha_lens.utils.storm.calculate_wind_buffers_gdf`
  and `wind_radii_polygon(method="asymmetric"|"symmetric")`; the latter replicates
  GDACS deliberately.
- The empirical size of the divergence — book chapter
  [09](../book/09-source-comparison.qmd).
- The zero-vs-NaN rule for GDACS `-1` values — chapter 09 §"Method" and
  [`0002-admin-level-conservation-of-source-exposure.md`](decisions/0002-admin-level-conservation-of-source-exposure.md).

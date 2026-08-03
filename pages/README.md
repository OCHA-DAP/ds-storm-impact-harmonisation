# Pages site

One GitHub Pages site, many products. Each product owns one directory and
appears on the landing page automatically.

**Live:** https://ocha-dap.github.io/ds-storm-impact-harmonisation/

## Why it is built this way

A repo gets exactly one Pages site, so everything the team publishes has to
share it. The obvious approach — a hand-written `index.html` and a deploy script
that names each product — puts every contributor in the same two files, so two
people adding pages in the same week conflict.

Instead, products are discovered by globbing `pages/products/*/page.toml`. There
is no central list to edit. Adding a product creates files no other branch
contains, which git merges without conflict.

This is the same drop-in directory convention as `/etc/nginx/conf.d/` or
`systemd`'s `.d/` overrides, for the same reason.

## Adding a page

Create a directory under `pages/products/` with a `page.toml`:

```toml
title = "My Thing"
blurb = "One or two sentences. Shown on the card."
path  = "my-thing"        # served at /my-thing/
order = 50                # ascending; ties break on title

[source]
type = "local"            # files live next to this manifest
dir  = "."
```

Put `index.html` (and anything it needs) in the same directory. Open a PR. On
merge the deploy runs and the card appears — no workflow changes, no edits to
anyone else's files.

### Source types

| `type`   | Where files come from                       | `dir` is relative to |
|----------|---------------------------------------------|----------------------|
| `local`  | next to the manifest                        | the manifest         |
| `repo`   | somewhere else in this repo (e.g. `app/`)   | the repo root        |

Everything in the source directory is copied except `page.toml`. To copy only
part of it, list what you want:

```toml
[source]
type    = "repo"
dir     = "app"
include = ["index.html", "assets"]
```

### Linking to something hosted elsewhere

Give a `url` instead of a `path`, and omit `[source]`. Nothing is copied; the
card links out and is marked **External**.

```toml
title     = "The Book"
blurb     = "Published on Netlify."
url       = "https://example.netlify.app"
url_label = "netlify.app"     # optional, shown on the card
order     = 30
```

### Marking something retired

```toml
status = "deprecated"     # default is "active"
```

The card renders muted, without hover, and is conventionally given a high
`order` so it sorts last. Nothing is removed — deprecated things stay reachable.

### Nested paths

`path = "slides/aac"` serves at `/slides/aac/`, so related products can share a
prefix.

## Checking your change locally

```bash
python3 pages/_build/assemble.py --out site
python3 -m http.server -d site 8000
```

No dependencies — standard library only, on the Python 3.11+ this repo already
requires. The same command runs in CI.

The build **fails loudly** rather than producing a broken site if two manifests
claim the same `path`, a required key is missing, `path` tries to escape the
output tree, `source.dir` does not exist or points outside the repo, or both
`url` and `path` are set. A collision is a red CI run, not a silently
overwritten page.

## How it deploys

`.github/workflows/deploy-app.yml` runs on push to `main` (paths `app/**`,
`pages/**`), daily at 06:00 UTC, and on manual dispatch. It exports the
comparison app's data from the database, runs `assemble.py`, and publishes the
result.

The workflow does not name individual products. The one exception is the
comparison app's database export, which is a product-specific build step; a
future product needing its own build step would require a workflow edit. This is
deliberate — manifests declare data, not commands to run, because that job holds
database credentials.

## Files

```
pages/
  _build/
    assemble.py     discovery, validation, copying, rendering
    template.html   landing page shell; cards are injected at <!--CARDS-->
  products/
    <slug>/page.toml   one per product
```

Styling follows the HDX v2 design tokens, kept in step with
`artefacts/slides/aac_slides.scss` so the site and the decks read as one system.

## Reusing this in another repo

Copy `pages/_build/`, create `pages/products/`, and add the assemble step to
your Pages workflow. Change the title, hero copy and footer link in
`template.html`. Nothing in `assemble.py` is specific to this project.

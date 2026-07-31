---
status: proposed
date: 2026-07-31
decision-makers: Zack Arno
consulted: Tristan Downing
informed: OCHA CHD Data Science team
---

# Manifest-driven multi-product GitHub Pages site

## Context and Problem Statement

GitHub allows one Pages site per repository. This repo already publishes two
things through it — the Cyclone Exposure Dashboard at `/` and the storm exposure
comparison app at `/compare/` — and we want to add more, starting with the AAC
slide deck. The team also wants this to be a pattern other repos can copy, since
"we have several web things and one Pages site" is a recurring problem.

The deploy workflow introduced in PR #10 established the pattern of serving
several products from one Pages site, listing both in a shared `rsync` block and
reaching one of them with a branch-specific `actions/checkout`. That is the right
shape for two products landing together, and it is what proved the approach
works. Extending it to a growing number of independently contributed pages is
where it runs out: every addition edits the same lines of the same two files, so
concurrent contributions conflict, and a mistake silently overwrites another
product's output rather than failing.

How should products be registered so that contributors can add pages without
coordinating, and without any risk of clobbering each other?

## Decision Drivers

* Several people must be able to add pages in the same period without merge conflicts.
* A path collision must fail the build, not silently overwrite a page.
* The pattern must be liftable into other OCHA-DAP repos with minimal editing.
* Some products are hosted here; others (the Quarto book on Netlify, the CERF
  predictor on Azure) are not, and must still be reachable from one index.
* Adding a page should not require understanding or editing GitHub Actions YAML.

## Considered Options

* **A — Drop-in manifest directory.** Each product owns `pages/products/<slug>/page.toml`;
  a generator globs them, validates, copies, and renders the landing page.
* **B — Convention-only discovery.** Generator scans subdirectories and reads title
  and description from each `index.html`'s `<meta>` tags. No manifest files.
* **C — Hand-edited `index.html` plus the existing hardcoded workflow.** Status quo,
  extended by hand for each new product.

## Decision Outcome

Chosen option: **A, drop-in manifest directory**, because it is the only option
that both eliminates the shared-file conflict and can represent products we do
not host.

Discovery is `pages/products/*/page.toml`. There is no central registry file, so
adding a product creates files that no other branch contains — the case git
merges cleanly without human intervention. This is the established drop-in
configuration convention (`/etc/nginx/conf.d/`, `systemd` `.d/` overrides,
`sources.list.d`), adopted for the same reason those exist.

The generator (`pages/_build/assemble.py`) uses the standard library only
(`tomllib` is stdlib on the Python 3.11+ this repo requires), so the assemble
step runs on bare `python3` and needs no dependency install in CI.

The site root becomes the landing page. The Cyclone Exposure Dashboard, which
previously occupied `/`, is retired from the site rather than moved to a
subpath: it is deprecated, superseded by the comparison app, and its source was
never on `main` — it lived on the unmerged `gdacs-adam-data` branch, reached by
an `actions/checkout` step whose own comment noted the deploy fails silently if
that branch is deleted.

Publishing it would have meant either keeping that branch dependency or
committing 1.5 MB of its frozen JSON to `main`, which has no committed datasets.
Both costs existed solely to keep a retired application online. Dropping it
removes the branch dependency, the committed data, and the only product whose
source was not on `main`.

Nothing is deleted: the branch, its commits, and PR #2's diff all remain
reachable. The previously published URL does not break in the way that matters —
visitors to `/` now get an index of current work rather than a 404.

### Consequences

* Good, because adding a product touches only files its author created.
* Good, because duplicate paths, missing keys, path traversal, and non-existent
  source directories all fail the build with a named file and reason.
* Good, because `url`-only manifests put externally hosted work in the same index.
* Good, because the pattern is portable: `assemble.py` contains nothing
  project-specific, and only `template.html` needs editing per repo.
* Bad, because it introduces a build script that can itself break, where
  previously three lines of `rsync` could not.
* Bad, because manifests declare data only. A product needing its own build step
  (as the comparison app does, for its database export) still requires a workflow
  edit. Allowing manifests to specify commands was rejected: that job holds
  `DSCI_AZ_DB_DEV_*` credentials, and executing arbitrary strings from manifest
  files in it is not a trade worth making for a problem we do not yet have.
* Good, because the workflow no longer checks out a feature branch, removing the
  silent-failure mode if that branch is ever deleted.
* Neutral, because the retired dashboard is no longer published; `/` remains a
  valid entry point and its source remains on its branch.

### Confirmation

`python3 pages/_build/assemble.py --out site` succeeds locally and in CI, and the
five expected failure modes were each verified to exit non-zero: duplicate
`path`, missing required key, `path` traversal, `source.dir` outside the repo,
and both `url` and `path` set. The deployed site serves `/`, `/compare/`,
`/dashboard/` and `/slides/aac/`.

## Pros and Cons of the Options

### A — Drop-in manifest directory

* Good, because contributions are disjoint new files; no shared list to edit.
* Good, because metadata is declarative and validated.
* Good, because external links are first-class.
* Neutral, because it adds one TOML file per product.
* Bad, because it is ~180 lines of Python that must be maintained.

### B — Convention-only discovery

* Good, because there is nothing to write beyond the page itself.
* Good, because it also avoids the shared-file conflict.
* Bad, because it cannot represent anything we do not host, and two of five
  products are hosted elsewhere.
* Bad, because metadata hides inside markup, which is harder to document as an
  SOP and easy to get wrong silently.

### C — Hand-edited index plus hardcoded workflow

* Good, because it requires no new tooling.
* Bad, because it is precisely the conflict-prone arrangement we are replacing.
* Bad, because a mistake overwrites another product's output silently.

## More Information

Contributor documentation lives in `pages/README.md`, including how to reuse the
pattern in another repository. The decision to vendor the deprecated dashboard,
rather than merge or check out its source branch, is recorded separately in
[0004](0004-vendor-the-deprecated-dashboard.md).

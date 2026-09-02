# Release process

Step-by-step for cutting a tagged release of `github.com/iodesystems/gwag/v2`.

## Before the first 1.0

Once 1.0 ships, the `// Stability: stable` markers and
[`docs/stability.md`](./docs/stability.md) lock the public surface
under SemVer. Drop renames, signature changes, and behavioural
breaks need to land **before** the 1.0 tag — after the tag they
become 2.0 work with a deprecation cycle.

## Cutting a release

1. **Working tree clean, main up-to-date.**

   ```
   git switch main
   git fetch origin && git rebase origin/main
   git status                  # no uncommitted changes
   ```

2. **Update `CHANGELOG.md`.**

   Move the `## Unreleased` section to a dated version section:

   ```markdown
   ## Unreleased

   ## v1.0.0 — 2026-MM-DD

   ### Added
   …
   ```

   Keep `Unreleased` empty (the `Item shape` reminder in
   `AGENTS.md` covers the next-PR convention).

3. **Verify the build.**

   ```
   bin/build                   # full build (UI codegen + Vite + Go)
   go test ./...               # full suite, ~60s for the gw/ package
   go vet ./...
   ```

   Everything green. If the perf matrix changed, optionally:

   ```
   bin/bench perf              # regenerates docs/perf.md
   ```

4. **Commit the changelog flip.**

   ```
   git add CHANGELOG.md
   git commit -m "docs(release): cut v1.0.0"
   ```

5. **Tag and push.**

   ```
   git tag -a v1.0.0 -m "v1.0.0"
   git push origin main
   git push origin v1.0.0
   ```

   Tags are annotated, not lightweight — `git describe` and Go
   module proxies expect the metadata.

6. **Create the GitHub release.**

   ```
   gh release create v1.0.0 \
     --title "v1.0.0" \
     --notes-file <(awk '/^## v1\.0\.0/,/^## /' CHANGELOG.md | sed '$d')
   ```

   The awk slice extracts everything from the v1.0.0 header up to
   the next `## ` line (the next-older release), then `sed '$d'`
   drops the trailing separator. Eyeball before publishing.

7. **Verify the Go proxy picked it up.**

   ```
   go list -m -versions github.com/iodesystems/gwag/v2
   ```

   `v1.0.0` should appear within a minute. If not, the proxy is
   lagging; `GOPROXY=direct` to bypass.

## Patch and minor releases

Same flow, different version bump. Reminder of the policy from
[`docs/stability.md`](./docs/stability.md):

- **PATCH** (`1.x.y → 1.x.(y+1)`) — bug fixes, doc fixes, internal
  performance improvements. No surface change.
- **MINOR** (`1.y.z → 1.(y+1).0`) — adds stable symbols, promotes
  experimental → stable, additive proto / metric fields, raises
  defaults.
- **MAJOR** (`1.y.z → 2.0.0`) — drops or renames stable symbols,
  changes wire format, demotes stable → experimental. Bring up
  a `v2/` import path under `gw/v2/`; never break the existing
  module's tag history.

## Backports

We don't currently maintain release branches. Bug fixes ride main;
patch releases tag main. If a downstream pinning makes a backport
necessary, branch `release/v1.x` off the tag, cherry-pick the fix,
tag `v1.x.(y+1)`, push the branch.

## Future flow (not yet wired)

- **Container image publish.** When the project ships a public
  container, this section names the registry, the build / push
  script, and the tag conventions (`v1.0.0`, `1.0`, `1`, `latest`).
- **`bin/release`.** A wrapper around the manual flow above when
  the steps stabilise enough that scripting them adds value.
- **CI release verification.** A GitHub Actions workflow that
  re-runs the full test matrix on every tag and refuses to publish
  the release if anything regresses.

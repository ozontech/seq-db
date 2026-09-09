---
name: perf-loop
description: Autonomous performance-optimization loop for seq-db driven by seqbazooka. Runs a scenario, collects seq-db profiles + metrics + the latency report, delegates analysis to the perf-analyzer subagent, applies the top optimization itself, re-runs, compares against the baseline, and keeps iterating until it stops improving. Use when the user wants the agent to hunt for and land perf wins on its own. For one-off "analyze these profiles" requests, call the perf-analyzer subagent directly instead.
---

# perf-loop

Autonomously find and land performance wins in seq-db. You drive the loop and
apply fixes yourself; you delegate the heavy profile reading to the
`perf-analyzer` subagent so it stays out of this context. The goal is **ideas
that measurably help** — code churn is expected, so try things, keep what wins,
revert what doesn't.

Read `reference/seqbazooka.md` (bundled with this skill) before the first run —
it has the commands, the report schema, and how to scrape seq-db profiles.

## Guardrails (non-negotiable — this loop is autonomous)

- **Never run on `main`.** If on `main`, create and switch to a branch named
  `0-perf-<label>` first. All work happens there.
- **One change per iteration, each its own commit.** Land an accepted win as a
  `perf: <what>` commit so every idea is visible and revertible. If a change
  regresses or is neutral, `git restore`/revert it and record why — do not stack
  unverified edits.
- **Keep a run log** (`.perf-runs/log.md`) of every iteration: hypothesis, change,
  measured delta, kept/reverted. This log is the real deliverable.
- **Compare apples to apples.** Pin scenario params and `--bootstrap.cpu`/
  `--bootstrap.memory` for the whole session. Changing the workload invalidates
  the baseline.

## Prerequisites (check once, up front)

- `docker`, `seqbazooka`, `go tool pprof`, `jq`, `curl` available.
- A bootstrap config env file (`key=value`, see reference). Ask the user for its
  path if you can't find one; do not invent seq-db config.
- Ensure `.perf-runs/` is git-ignored (add it to `.gitignore` if missing) so run
  artifacts never get committed.

## Run directory layout

Each run gets `.perf-runs/<NNN>-<label>/` (zero-padded, monotonic) containing:
`report.json`, `cpu.pprof`, `heap.pprof`, `allocs.pprof`, `fgprof.pprof`,
`metrics.txt`, and `meta.json` (scenario, all flags, git SHA, image tag,
timestamp from `date`). The previous run's dir is the **baseline** for the next.

## The loop

### 0. Baseline
1. Pick or confirm the scenario + params with the user's intent (e.g. `bulk` for
   write perf, `search-regular`/`search-aggregation` for query perf). Default to
   a bounded `--duration` (a few minutes) so iterations are tractable.
2. Build a docker image from the **current** working tree and tag it
   (e.g. `seq-db:perf-local`) — this is what makes local edits measurable
   (`docker build -f build/package/Dockerfile -t seq-db:perf-local .`).
3. Run seqbazooka with `--bootstrap.image=seq-db:perf-local`, writing
   `report.json` into the run dir (§ run mechanics below).
4. This first run is `000-baseline`. No analysis-driven change yet.

### 1..N. Iterate
For each iteration:
1. **Analyze.** Spawn `perf-analyzer` (Agent tool, `subagent_type:
   "perf-analyzer"`) with the paths to the latest run's artifacts **and** the
   baseline/previous run's artifacts, plus the scenario. It returns a ranked
   list of optimization opportunities.
2. **Apply the top viable one** yourself — edit the seq-db source. Respect
   `CLAUDE.md`/`CONTRIBUTING.md` (style, "why" comments, reuse over allocation).
   Keep the change focused; if the analyzer's top idea is risky or speculative,
   pick the next one it rates high-confidence.
3. **Rebuild** the image, **re-run** the identical scenario into a new run dir.
4. **Compare** the new `report.json` to the previous run's on the metrics that
   matter for the scenario (write → bulk `mean`/`p(99)`; search → per-query
   `p(99)`/`mean`, hot and cold). Also diff profiles
   (`go tool pprof -diff_base`) to confirm the hotspot actually shrank.
5. **Decide**: if it's a real improvement beyond noise, `git commit` it as
   `perf: …` and this run becomes the new baseline. Otherwise revert the edit
   (run dir stays as evidence) and try the analyzer's next idea.
6. Append to the run log.

### Run mechanics (per run)
- Start `seqbazooka <cmd> … --duration=<D> --report.path=<dir>/report.json` in
  the background.
- Once seq-db is ready and the execute phase is in steady state (poll
  `http://localhost:9200/ready`, and for ingest-then-search scenarios wait past
  the bulk phase), scrape into the run dir: a CPU profile
  (`profile?seconds=<S>`, `S` < remaining duration), then `heap`, `allocs`,
  `fgprof`, and `metrics`. See reference for exact curls. The container is
  destroyed at teardown, so profiles must be taken **mid-run**.
- Wait for seqbazooka to finish and write the report.

## Stop conditions

Stop and summarize when any holds:
- No improvement over **2** consecutive iterations (analyzer's remaining ideas
  are exhausted or don't pan out).
- A hard cap (default **6** iterations) unless the user set another.
- Profiles show the scenario is dominated by genuine I/O or already-tight code.
- The user interrupts.

## Final summary

Report: the branch and its `perf:` commits, per-commit measured delta, total
improvement vs `000-baseline`, ideas tried-and-reverted (with why), and what to
try next. Point to `.perf-runs/log.md` for the full trail.

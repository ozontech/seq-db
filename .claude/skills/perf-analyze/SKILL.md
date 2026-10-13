---
name: perf-analyze
description: One-shot profile analysis for seq-db — turns pprof profiles (and optional metrics/report) into a ranked list of concrete, profile-grounded optimization opportunities for the code. Delegates to the perf-analyzer subagent and relays its findings. Use when you have profiles and want to know what to optimize (e.g. "analyze these CPU/heap profiles", "what's the hot path costing us?"). This proposes; it does not apply fixes.
---

# perf-analyze

A one-shot, profile-grounded analysis. Delegate the heavy profile reading to the
`perf-analyzer` subagent so the pprof output stays out of this context; you scope
and relay.

(For an autonomous run → analyze → apply → measure loop, use the `perf-loop`
skill, which drives this same subagent per iteration. Use *this* skill for a
standalone analysis of profiles you already have.)

## Steps

1. **Locate the artifacts.** Profiles the user brought, or a run directory under
   `.perf-loop/<run>/` (`cpu.pprof`, `allocs.pprof`, `heap.pprof`, …) plus its
   `report.json`/`window.env`. If there are none yet, point the user at
   `perf-loop`/`scripts/stack.sh` to generate a run first.

2. **Spawn the `perf-analyzer` subagent** (Agent tool, `subagent_type:
   "perf-analyzer"`) with the artifact paths, the scenario (write / search /
   aggregation / mixed), and a baseline run to diff against if one exists.

3. **Relay the ranked findings** — the overall summary, then each hotspot with
   its evidence / cause / proposed change / confidence. Keep your own commentary
   minimal; the findings are the deliverable.

4. **Do not apply fixes.** After presenting, offer to implement a specific one on
   request.

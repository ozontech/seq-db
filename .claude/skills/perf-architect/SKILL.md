---
name: perf-architect
description: Design-level performance review for seq-db — proposes data-structure, algorithmic, memory-layout, and architectural alternatives for a hot path, each with reasoning and a validation plan. Delegates to the perf-architector subagent (top-down design lens) and relays ranked proposals. Use when you want design-level performance ideas or to rethink an approach (e.g. "rethink the search path", "is the postings representation right?"). This proposes; it does not implement.
---

# perf-architect

A deliberate, top-down design review. Use it when the question is "is there a
fundamentally better design for this hot path?" — a better data structure,
algorithm, layout, or execution strategy for the workload.

Delegate the heavy reading (subsystem + architecture docs + profiles) to the
`perf-architector` subagent so it stays out of this context; you scope and relay.

## Steps

1. **Scope the target** from the user's request: a subsystem, a query/scenario
   type, or a path (e.g. "the regular-search read path"). If they didn't name
   one, ask — design review needs a concrete target.
2. **Point at evidence if it exists.** If there are recent runs under `.perf-loop/`
   (profiles, baseline numbers), pass those paths so the subagent can see where
   cost concentrates. If not, it proceeds from code + architecture docs + the
   documented workload/data characteristics — that's fine.
3. **Spawn the `perf-architector` subagent** (Agent tool, `subagent_type:
   "perf-architector"`) with the target, the workload being optimized for, and any
   evidence paths. One agent; fan out only for genuinely independent subsystems.
4. **Relay the ranked design proposals** — current-design summary, then each
   proposal with its why/impact/risk/validation. Keep your own commentary minimal.
5. **Do not implement.** These are hypotheses. After presenting, offer to
   prototype a chosen one — that goes through the normal edit → build → benchmark
   flow (and can reuse the perf-loop observability stack to measure), NOT the
   autonomous micro-loop, since architectural changes need a real prototype and
   wider validation.

## Notes

- If the subagent concludes the current design is already appropriate, report
  that as the result; a "no rewrite needed" answer is valid and valuable.

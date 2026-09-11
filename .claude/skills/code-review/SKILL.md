---
name: code-review
description: Review the current code changes (branch diff vs main, a commit range, or a PR) for seq-db. Delegates to the code-reviewer subagent so the review's file reading and test runs stay out of the main context, then relays ranked findings. Use when the user asks to review changes, a branch, or a diff before opening a PR.
---

# Review changes

Delegate the review to a subagent so the diff reading, context gathering, and
any test/lint runs stay out of this conversation's context. You orchestrate and
relay; you do not read the whole diff yourself.

## Steps

1. **Determine scope** from the user's request:
   - No argument → current branch vs `main` plus uncommitted changes.
   - A commit/range, path(s), or PR number → pass that through verbatim.
2. **Spawn the `code-reviewer` subagent** (Agent tool, `subagent_type:
   "code-reviewer"`). Give it the scope and any focus the user asked for
   (e.g. "concentrate on the sealing path"). One agent is enough; only fan out
   into several code-reviewer agents if the diff spans clearly independent
   subsystems and the user asked for a thorough pass.
3. **Relay the findings.** Present the subagent's verdict and findings to the
   user, ranked most-severe first. If code-review reporting via `ReportFindings`
   is available, call it once with the verified findings for structured
   rendering; otherwise present them as a concise markdown list. Do not pad the
   output — a clean review is a valid result.
4. **Do not fix anything unless asked.** Reviewing and editing are separate
   steps. After presenting findings, offer to fix specific ones on request.

## Notes

- The subagent's report is not shown to the user automatically — you must relay
  what matters.
- Keep your own commentary minimal; the findings are the deliverable.

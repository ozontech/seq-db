---
name: code-navigation
description: How to navigate and explore Go code in this repo. Use the `gopls` CLI (definition, references, implementation, call_hierarchy, symbols, workspace_symbol) as the PRIMARY way to answer "where is this defined / used / implemented / called". Fall back to Grep/Read only when gopls cannot answer. Invoke when exploring the codebase, tracing a symbol, or answering a "where/who/what-calls" question about Go code.
---

# code-navigation

For Go code, **navigate with `gopls` first.** It resolves symbols semantically —
across packages, through interfaces, following the type system — which text
search cannot. `Grep`/`Read` are the fallback, not the default: reach for them
only when gopls genuinely can't answer (see below). `gopls` is allowlisted, so
these run without a prompt; on this repo a call is ~0.3s.

## Map the question to a gopls command

Positions are `<file>:<line>:<col>`, **1-based**, with `col` pointing **at the
identifier** (or `<file>:#<byteoffset>`).

| You want to… | Command |
|---|---|
| Find a symbol by name when you don't know where it lives | `gopls workspace_symbol <query>` |
| List what's declared in a file (outline) | `gopls symbols <file>` |
| Jump to where a symbol is defined | `gopls definition <file>:<line>:<col>` |
| See a type/func's signature + doc | `gopls definition -markdown <pos>` |
| Find every use of a symbol | `gopls references <pos>` (`-d` to include the declaration) |
| Find implementations of an interface (or interfaces a type satisfies) | `gopls implementation <pos>` |
| See callers/callees of a function | `gopls call_hierarchy <pos>` |

Add `-json` (on `definition`) when you want machine-readable output to parse.

## Typical flow

1. **Bootstrap without grepping.** The two commands that take a name/file
   instead of a position are your entry points:
   - Know the name only → `gopls workspace_symbol MetaData` → gives the
     declaration's `file:line:col`.
   - Know the file → `gopls symbols path/to/file.go` → outline with positions.
2. **Navigate** from that position: `definition` / `references` /
   `implementation` / `call_hierarchy`.
3. **Read** the location(s) gopls returns to see the actual code. Finding *where*
   is gopls's job; viewing the span is `Read`'s.

## When Grep/Read are the right tool (gopls insufficient)

- **Non-symbol text**: string literals, log messages, error text, struct tags,
  config keys, metric names, magic constants → `Grep`.
- **Non-Go files**: YAML/proto/Makefile/markdown/`.env` → `Grep`/`Read`.
- **Generated or build-excluded code** gopls doesn't resolve (e.g. files behind
  build tags it didn't load).
- **gopls returned nothing** for a symbol you're sure exists → confirm with
  `Grep`, then retry.
- **Reading a known span**: once a location is in hand, `Read` it directly.

## Notes

- `workspace_symbol` / `symbols` need no position — prefer them to grep for
  *locating* a Go symbol.
- Point `col` at the start of the identifier, not the line start, or the command
  resolves the wrong (or no) symbol.
- This is a Go-navigation policy; for text-shaped searches across the tree (any
  language) `Grep` remains correct — the rule is "don't grep for something gopls
  can resolve semantically," not "never grep".
- Equivalent structured alternative: the harness `LSP` tool (same gopls engine,
  JSON results, adds split incoming/outgoing calls + hover) — use it instead if
  you prefer structured output; results are identical.

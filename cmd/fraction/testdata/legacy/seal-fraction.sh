#!/usr/bin/env bash
# Seals a fraction in a given format version. Used by tests and CI to run
# the cmd/fraction tests against fractions produced by older seq-db code.
#
# Usage:
#   seal-fraction.sh <version> <frac-base-name> < docs.jsonl
#
# Optional flag:
#   --mapping=<file>  mapping YAML for the indexed fields; the file must
#                     be readable by the version's seq.ReadMapping. Without
#                     the flag a built-in default mapping is used.
#
# <version> is a fraction version: v2, v3, v4, v5, ... or "current".
#   v2..v5   — a git worktree is created at that version's sealer commit
#              (a version commit with cmd/sealer added on top, see the
#              sealer_commit table below) and the committed standalone
#              sealer is run there; old code has no `fraction seal`.
#   v6+      — a git worktree is created at the version commit found
#              dynamically (the parent of the first commit introducing
#              the next version in config/frac_version.go) and the version's
#              own `go run ./cmd/fraction seal` is used (it exists since
#              v6 and seals in the code's current format). Needs no
#              per-version support: a new vN+1 works with zero script
#              changes.
#   current  — the local code's own `go run ./cmd/fraction seal` is used.
#
# <frac-base-name> is the output fraction base name (without suffixes).
# Documents (one JSON per line) are read from stdin.
#
# Prints the fraction base name on stdout.

set -euo pipefail

VERSION=""
FRAC=""
for arg in "$@"; do
	case "$arg" in
	--mapping=*) MAPPING="${arg#--mapping=}" ;;
	*) if [[ -z "$VERSION" ]]; then VERSION="$arg"; else FRAC="$arg"; fi ;;
	esac
done
VERSION="${VERSION:?usage: seal-fraction.sh [--mapping=file] <version> <frac-base-name> < docs.jsonl}"
FRAC="${FRAC:?usage: seal-fraction.sh [--mapping=file] <version> <frac-base-name> < docs.jsonl}"
MAPPING="${MAPPING:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

# Version commit discovery needs history; a PR checkout has no local "main"
# branch (detached HEAD), so prefer origin/main, then main, then HEAD.
history_ref() {
	for ref in origin/main main HEAD; do
		if git -C "$REPO" rev-parse -q --verify "$ref" >/dev/null; then
			echo "$ref"
			return
		fi
	done
	echo "no git history ref found (tried origin/main, main, HEAD)" >&2
	return 1
}

# the mapping file is opened by sealers running from other cwd's
[[ -z "$MAPPING" || "$MAPPING" == /* ]] || MAPPING="$(pwd)/$MAPPING"

# Last commit of version: the parent of the first commit that
# introduced the next version (BinaryDataV<n+1>) in config/frac_version.go.
# When the next version is not committed yet (e.g. a locally added V7),
# fall back to the history ref tip. Requires real
# history: in a shallow CI clone the caller must fetch it first
# (fetch-depth: 0 or git fetch --unshallow).
version_commit() {
	local next=$((VERSION_NUM + 1))
	local history
	history="$(history_ref)"
	for commit in $(git -C "$REPO" rev-list --reverse "$history" -- config/frac_version.go); do
		if git -C "$REPO" show "${commit}:config/frac_version.go" 2>/dev/null | grep -q "BinaryDataV${next}\b"; then
			git -C "$REPO" rev-parse --short "${commit}^"
			return
		fi
	done
	if [[ "$VERSION_NUM" -ge "$(max_known_version_num)" ]]; then
		git -C "$REPO" rev-parse --short "$history"
		return
	fi
	echo "cannot find the commit introducing BinaryDataV${next}: version $VERSION is too old (supported: v6+)" >&2
	return 1
}

# The highest BinaryDataVN declared in the current config/frac_version.go.
max_known_version_num() {
	local history
	history="$(history_ref)"
	git -C "$REPO" show "${history}:config/frac_version.go" | grep -oE 'BinaryDataV[0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1
}

# Sealer commits for the pre-v6 formats: version commit with the
# standalone sealer (cmd/sealer) committed on top.
sealer_commit() {
	case "$VERSION" in
	v2) echo "98d29f2af902ee37ac24da18255a0a69a0da8266" ;;
	v3) echo "1e214d0a282f29f577f38dcc920a8b8c6d95eab1" ;;
	v4) echo "113f8663b80ce44149c12f1333237313b535e1d1" ;;
	v5) echo "02e864f6a26b193fbfdcc973a5de3b8b9dd15f60" ;;
	esac
}

if [[ "$VERSION" == "current" ]]; then
	if [[ -n "$MAPPING" ]]; then
		(cd "$REPO" && go run ./cmd/fraction seal --mapping="$MAPPING" "$FRAC")
	else
		(cd "$REPO" && go run ./cmd/fraction seal "$FRAC")
	fi
else
	[[ "$VERSION" =~ ^v[0-9]+$ ]] || {
		echo "unknown version: $VERSION (expected vN or current)" >&2
		exit 2
	}
	VERSION_NUM="${VERSION#v}"
	[[ "$VERSION_NUM" -ge 2 ]] || {
		echo "version $VERSION is too old (supported: v2+)" >&2
		exit 2
	}

	if [[ "$VERSION_NUM" -ge 6 ]]; then
		# v6+ seal with their own cmd/fraction
		commit="$(version_commit)"
	else
		commit="$(sealer_commit)"
		[[ -n "$commit" ]] || {
			echo "no sealer commit for version $VERSION" >&2
			exit 2
		}
	fi

	wt="${TMPDIR:-/tmp}/fraction-seal-$VERSION"

	if [[ -d "$wt/.git" || -f "$wt/.git" ]]; then
		git -C "$wt" checkout -q --detach "$(git -C "$REPO" rev-parse "$commit")"
	else
		git -C "$REPO" worktree add --detach "$wt" "$commit" >/dev/null
	fi

	if [[ "$VERSION_NUM" -ge 6 ]]; then
		# v6+ have their own cmd/fraction seal
		if [[ -n "$MAPPING" ]]; then
			(cd "$wt" && go run ./cmd/fraction seal --mapping="$MAPPING" "$FRAC")
		else
			(cd "$wt" && go run ./cmd/fraction seal "$FRAC")
		fi
	else
		# the sealer is committed at (cmd/sealer)
		if [[ -n "$MAPPING" ]]; then
			(cd "$wt" && CGO_ENABLED=0 go run ./cmd/sealer "$FRAC" --mapping="$MAPPING")
		else
			(cd "$wt" && CGO_ENABLED=0 go run ./cmd/sealer "$FRAC")
		fi
	fi
fi

echo "$FRAC"

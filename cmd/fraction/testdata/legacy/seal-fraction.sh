#!/usr/bin/env bash
# Seals a fraction in a given format version. Used by tests and CI to run
# the cmd/fraction tests against fractions produced by older seq-db code.
#
# Usage:
#   seal-fraction.sh <version> <frac-base-name> < docs.jsonl
#
# Optional flag:
#   --mapping=<file>  mapping YAML for the indexed fields; the file must
#                     be readable by the era's seq.ReadMapping. Without
#                     the flag a built-in default mapping is used.
#
# <version> is a fraction version: v2, v3, v4, v5, ... or "current".
#   v2..v5   — a git worktree is created at the last commit of that
#              version's era (found dynamically: the parent of the first
#              commit introducing the next version in config/frac_version.go)
#              and a standalone sealer is run there; old code has no
#              `fraction seal`. The sealer is the single shared file
#              (testdata/legacy/sealer/main.go), patched for the
#              era: the few lines that differ between the eras are
#              replaced with sed.
#   v6+      — a git worktree is created at the era commit the same way,
#              but the era's own `go run ./cmd/fraction seal` is used
#              (it exists since v6 and seals in the code's current
#              format). Needs no per-version support: a new vN+1 works
#              with zero script changes.
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

# Era commit discovery needs history; a PR checkout has no local "main"
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

# Last commit of version's era: the parent of the first commit that
# introduced the next version (BinaryDataV<n+1>) in config/frac_version.go.
# When the next version is not committed yet (e.g. a locally added V7),
# the era has not ended: fall back to the history ref tip. Versions whose
# next one appeared before the file existed (v0, v1) are not discoverable
# and are not supported. Requires real history: in a shallow CI clone the
# caller must fetch it first (fetch-depth: 0 or git fetch --unshallow).
era_commit() {
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
	echo "cannot find the commit introducing BinaryDataV${next}: version $VERSION is too old (supported: v2+)" >&2
	return 1
}

# The highest BinaryDataVN declared in the current config/frac_version.go.
max_known_version_num() {
	local history
	history="$(history_ref)"
	git -C "$REPO" show "${history}:config/frac_version.go" | grep -oE 'BinaryDataV[0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1
}

# Patches the shared sealer for a given pre-v6 format era: replaces the
# lines that differ between the eras. The file as committed targets the
# newest sealer era (v5); older eras get a sed-reduced version. v6+ eras
# do not reach this function: they seal with their own cmd/fraction.
patch_sealer() {
	local ver="$1" file="$2"
	if [[ "$ver" == "v5" || "$ver" == "v4" ]]; then
		# v4 sealer is identical to the v5 one
		cat "$file"
		return
	fi
	if [[ "$ver" == "v3" ]]; then
		# v3: sealing lived in frac/sealed/sealing, and SealParams
		# had no LIDBlockSize/TokenBlockSize
		sed -e 's|"github.com/ozontech/seq-db/sealing"|"github.com/ozontech/seq-db/frac/sealed/sealing"|' \
			-e '/LIDBlockSize:/d' -e '/TokenBlockSize:/d' \
			"$file"
		return
	fi
	# v2: on top of the v3 differences, the skip mask provider interface
	# had no GetIDsBitmapByFrac (and roaring is not in the go.mod), and
	# GetIDsIteratorByFrac returned no cleanup func
	sed -e 's|"github.com/ozontech/seq-db/sealing"|"github.com/ozontech/seq-db/frac/sealed/sealing"|' \
		-e '/LIDBlockSize:/d' -e '/TokenBlockSize:/d' -e '/LIDsBitmapThreshold:/d' \
		-e '/"github.com\/RoaringBitmap\/roaring\/v2"/d' \
		-e '/GetIDsBitmapByFrac(_ string, _, _ uint32)/,/^}/d' \
		-e 's|return node.NewStatic(nil, reverse), false, func() error { return nil }, nil|return node.NewStatic(nil, reverse), false, nil|' \
		-e 's|(_ string, _, _ uint32, reverse bool) (node.Node, bool, func() error, error)|(_ string, _, _ uint32, reverse bool) (node.Node, bool, error)|' \
		"$file"
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

	commit="$(era_commit)"
	wt="${TMPDIR:-/tmp}/fraction-seal-$VERSION"

	if [[ -d "$wt/.git" || -f "$wt/.git" ]]; then
		git -C "$wt" checkout -q --detach "$(git -C "$REPO" rev-parse "$commit")"
	else
		git -C "$REPO" worktree add --detach "$wt" "$commit" >/dev/null
	fi

	if [[ "$VERSION_NUM" -ge 6 ]]; then
		# v6+ eras have their own cmd/fraction seal
		if [[ -n "$MAPPING" ]]; then
			(cd "$wt" && go run ./cmd/fraction seal --mapping="$MAPPING" "$FRAC")
		else
			(cd "$wt" && go run ./cmd/fraction seal "$FRAC")
		fi
	else
		sealer="$SCRIPT_DIR/sealer/main.go"
		[[ -f "$sealer" ]] || {
			echo "standalone sealer is missing: $sealer" >&2
			exit 2
		}

		mkdir -p "$wt/sealer"
		patch_sealer "$VERSION" "$sealer" > "$wt/sealer/main.go"

		if [[ -n "$MAPPING" ]]; then
			(cd "$wt" && CGO_ENABLED=0 go run ./sealer "$FRAC" --mapping="$MAPPING")
		else
			(cd "$wt" && CGO_ENABLED=0 go run ./sealer "$FRAC")
		fi
	fi
fi

echo "$FRAC"

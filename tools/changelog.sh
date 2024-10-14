#!/bin/bash


generate() {
    echo "# Changelog"
    echo
    TAGS=$(git tag | grep -E '^v\d+\.\d+\.\d+$' | sed -e 's/v//' | sort -t '.' -n -r -k 1 -k 2 -k 3)
    for v in $TAGS; do
        VERSION="v$v"
        if [[ $PREV != "" ]]; then
            echo " - **[$PREV]**"
            echo
            echo "$(git log --pretty=format:'   - %s' --no-merges $VERSION..$PREV)"
            echo
        fi
        PREV=$VERSION
    done
}

DST=$(dirname $0)/../CHANGELOG.md
TMP=$(mktemp '/tmp/'$(basename $0)'.XXXXXX')

generate > $TMP || exit 1
mv $TMP $DST

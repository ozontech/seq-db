#!/bin/bash

LAST_VERSION=$(git describe --abbrev=4 --dirty --always --tags | sed -E 's/-.+$//')

MAJOR=$(echo "$LAST_VERSION" | sed -E 's/v([0-9]+)\.([0-9]+)\.([0-9]+)/\1/')
MINOR=$(echo "$LAST_VERSION" | sed -E 's/v([0-9]+)\.([0-9]+)\.([0-9]+)/\2/')
PATCH=$(echo "$LAST_VERSION" | sed -E 's/v([0-9]+)\.([0-9]+)\.([0-9]+)/\3/')

if [[ $1 = "major" ]]; then
    let MAJOR++
    NEW_VERSION="v$MAJOR.0.0"
elif [[ $1 = "minor" ]]; then
    let MINOR++
    NEW_VERSION="v$MAJOR.$MINOR.0"
elif [[ $1 = "patch" ]]; then
    let PATCH++
    NEW_VERSION="v$MAJOR.$MINOR.$PATCH"
else
    echo
    echo "Inrements version, generate changelog and add local tag"
    echo
    echo "Usage:"
    echo "  ./release.sh major - increment major version"
    echo "  ./release.sh minor - increment minor version"
    echo "  ./release.sh patch - increment patch version"
    echo
    echo "Pass second argument 'skip-edit' to skip editing tag message:"
    echo "  ./release.sh patch skip-edit - increment patch version without editing tag message"
    echo
    echo "then execute command to push tags into origin:"
    echo "   git push origin --tags"
    echo

    exit
fi

CHANGELOG=$(git log --pretty=format:'  * %s' --no-merges $LAST_VERSION..HEAD)

if [[ -z "$2" ]]; then
    git tag -a $NEW_VERSION -m "$CHANGELOG" -e
elif [[ $2 = "skip-edit" ]]; then
    git tag -a $NEW_VERSION -m "$CHANGELOG"
else
    echo
    echo "If you don't want to edit tag message, pass second arg as 'skip-edit'"
    echo
fi

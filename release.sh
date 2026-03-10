#!/bin/bash
set -euo pipefail

# Release script for fs-perf-loader
# Usage: ./release.sh [--dry-run] [next-version]
#
# Arguments:
#   --dry-run       Preview without pushing or creating releases
#   next-version    Next development version (without -SNAPSHOT suffix).
#                   If omitted, auto-increments the minor version.
#                   Example: ./release.sh 2.0.0  ->  sets 2.0.0-SNAPSHOT
#
# Steps:
#   1. Remove -SNAPSHOT from version in build.gradle
#   2. Commit the version change
#   3. Create git tags: release-<version>, v<version>, <version>
#   4. Push commit and tags to origin
#   5. Build fat JAR with Gradle
#   6. Upload fat JAR to GitHub Releases
#   7. Set next development version (<next>-SNAPSHOT), commit and push

DRY_RUN=false
NEXT_VERSION_ARG=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) NEXT_VERSION_ARG="$arg" ;;
  esac
done

if [[ "$DRY_RUN" == true ]]; then
  echo "=== DRY RUN MODE ==="
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Ensure we're in a clean git state
if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: Working directory is not clean. Commit or stash changes first."
  exit 1
fi

# Extract current version from build.gradle
CURRENT_VERSION=$(grep "^version = " build.gradle | sed "s/version = '//;s/'//")
if [[ -z "$CURRENT_VERSION" ]]; then
  echo "ERROR: Could not extract version from build.gradle"
  exit 1
fi

if [[ "$CURRENT_VERSION" != *-SNAPSHOT ]]; then
  echo "ERROR: Current version '$CURRENT_VERSION' is not a SNAPSHOT version"
  exit 1
fi

RELEASE_VERSION="${CURRENT_VERSION%-SNAPSHOT}"
echo "Releasing version: $RELEASE_VERSION (was $CURRENT_VERSION)"

# Step 1: Remove -SNAPSHOT from version
echo ""
echo "=== Step 1: Update version in build.gradle ==="
sed -i "s/version = '${CURRENT_VERSION}'/version = '${RELEASE_VERSION}'/" build.gradle
echo "Updated version to $RELEASE_VERSION"

# Step 2: Commit version change
echo ""
echo "=== Step 2: Commit release version ==="
git add build.gradle
git commit -m "make a non SNAPSHOT version - release-v${RELEASE_VERSION}"

# Step 3: Create git tags
echo ""
echo "=== Step 3: Create git tags ==="
git tag "release-${RELEASE_VERSION}"
git tag "v${RELEASE_VERSION}"
git tag "${RELEASE_VERSION}"
echo "Created tags: release-${RELEASE_VERSION}, v${RELEASE_VERSION}, ${RELEASE_VERSION}"

# Step 4: Push to GitHub
echo ""
echo "=== Step 4: Push to origin ==="
if [[ "$DRY_RUN" == true ]]; then
  echo "[DRY RUN] Would push branch and tags to origin"
else
  git push origin HEAD --force
  git push origin "refs/tags/release-${RELEASE_VERSION}" "refs/tags/v${RELEASE_VERSION}" "refs/tags/${RELEASE_VERSION}" --force
  echo "Pushed commit and tags to origin"
fi

# Step 5: Build fat JAR
echo ""
echo "=== Step 5: Build fat JAR ==="
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk}" ./gradlew clean build fatJar
FAT_JAR=$(ls -1 build/libs/*-fat.jar 2>/dev/null | head -1)
if [[ -z "$FAT_JAR" ]]; then
  echo "ERROR: Fat JAR not found in build/libs/"
  exit 1
fi
echo "Built: $FAT_JAR"

# Step 6: Create GitHub release and upload fat JAR
echo ""
echo "=== Step 6: Create GitHub release ==="
if [[ "$DRY_RUN" == true ]]; then
  echo "[DRY RUN] Would create GitHub release v${RELEASE_VERSION} with $FAT_JAR"
else
  gh release create "v${RELEASE_VERSION}" "$FAT_JAR" \
    --title "v${RELEASE_VERSION}" \
    --notes "Release ${RELEASE_VERSION}" \
    --repo "$(gh repo view --json nameWithOwner -q .nameWithOwner)"
  echo "GitHub release created: v${RELEASE_VERSION}"
fi

# Step 7: Set next development version
echo ""
echo "=== Step 7: Set next development version ==="
if [[ -n "$NEXT_VERSION_ARG" ]]; then
  NEXT_VERSION="${NEXT_VERSION_ARG}-SNAPSHOT"
else
  # Auto-increment: bump the last numeric segment
  # e.g. 1.0.31 -> 1.0.32-SNAPSHOT
  VERSION_PREFIX="${RELEASE_VERSION%.*}"
  VERSION_MINOR="${RELEASE_VERSION##*.}"
  NEXT_MINOR=$((VERSION_MINOR + 1))
  NEXT_VERSION="${VERSION_PREFIX}.${NEXT_MINOR}-SNAPSHOT"
fi

sed -i "s/version = '${RELEASE_VERSION}'/version = '${NEXT_VERSION}'/" build.gradle
git add build.gradle
git commit -m "Set the version up to ${NEXT_VERSION}"
echo "Updated version to $NEXT_VERSION"

if [[ "$DRY_RUN" == true ]]; then
  echo "[DRY RUN] Would push next development version to origin"
else
  git push origin HEAD
  echo "Pushed next development version to origin"
fi

echo ""
echo "=== Release $RELEASE_VERSION complete. Next development version: $NEXT_VERSION ==="

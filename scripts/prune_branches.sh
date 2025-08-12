#!/usr/bin/env bash
set -euo pipefail

BASE_BRANCH="${1:-main}"

# Prune remote-tracking branches if any remotes are configured
if git remote | grep -q .; then
  git fetch --prune >/dev/null
fi

# Delete local branches already merged into the base branch, excluding the current branch
current_branch=$(git rev-parse --abbrev-ref HEAD)
for branch in $(git branch --merged "$BASE_BRANCH" | sed 's/^..//' | grep -v "^$BASE_BRANCH$" | grep -v "^$current_branch$"); do
  git branch -d "$branch"
done

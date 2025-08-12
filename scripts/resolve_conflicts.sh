#!/usr/bin/env bash
set -euo pipefail

POLICY="${POLICY:-PREFER_BASE}"  # PREFER_BASE or PREFER_PR
BASE_DEFAULT=$(git rev-parse --abbrev-ref HEAD)
REPO="${GITHUB_REPOSITORY:-$(git remote get-url origin 2>/dev/null | sed -E 's#.*[:/]([^/]+/[^/]+)(\.git)?$#\1#')}"

echo "Policy: $POLICY"
echo "Base default: $BASE_DEFAULT"
echo "Repository: $REPO"

# List open PRs with conflicts (mergeable_state = "dirty")
prs_json=$(gh api -H "Accept: application/vnd.github+json" \
  "/repos/$REPO/pulls" --paginate -q '.[] | select(.state=="open") | {number,head:.head.ref,base:.base.ref}')
 mapfile -t prs < <(jq -r '.number as $n | .head as $h | .base as $b | [$n,$h,$b] | @tsv' <<<"$prs_json")

if [ ${#prs[@]} -eq 0 ]; then
  echo "No open PRs found."
  exit 0
fi

for row in "${prs[@]}"; do
  IFS=$'\t' read -r pr head_ref base_ref <<<"$row" || continue
  mergeable_state=$(gh api "/repos/$REPO/pulls/$pr" -q .mergeable_state || echo "unknown")
  if [ "$mergeable_state" != "dirty" ]; then
    echo "PR #$pr is not dirty (state: $mergeable_state). Skipping."
    continue
  fi

  echo "::group::Attempting PR #$pr ($head_ref <- $base_ref)"
  git fetch origin "$head_ref" "$base_ref"
  git checkout -B "auto-resolve/$pr" "origin/$head_ref"
  # Merge base into PR branch to surface conflicts locally
  if ! git merge --no-commit --no-ff "origin/$base_ref"; then
    # Gather conflicted files
    conflicted=$(git diff --name-only --diff-filter=U || true)
    if [ -z "$conflicted" ]; then
      echo "No conflicted files detected, continuing merge."
    else
      echo "Conflicted files:"
      echo "$conflicted"
      while IFS= read -r f; do
        if [ "$POLICY" = "PREFER_BASE" ]; then
          git checkout --theirs -- "$f"
        else
          git checkout --ours -- "$f"
        fi
        git add -- "$f"
      done <<< "$conflicted"
    fi
    git commit -m "chore: auto-resolve merge conflicts (policy: $POLICY) [skip ci]" || true
  else
    # No conflicts; complete merge
    git commit -m "chore: merge $base_ref into $head_ref (no conflicts) [skip ci]" || true
  fi

  # Push back to PR branch
  git push -u origin "auto-resolve/$pr":"$head_ref"

  # Re-check mergeability and comment
  new_state=$(gh api "/repos/$REPO/pulls/$pr" -q .mergeable_state || echo "unknown")
  gh pr comment "$pr" --body "Auto-conflict-resolver applied **$POLICY**. New mergeable_state: \`$new_state\`. Branch updated."
  echo "::endgroup::"
done


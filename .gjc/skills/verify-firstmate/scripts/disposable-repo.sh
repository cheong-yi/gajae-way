#!/usr/bin/env bash
# Deterministic. No model call. Creates or destroys a throwaway git repository
# for the lane model switch smoke.
#
#   disposable-repo.sh create  <path>   fresh repo, no remotes, one setup commit, /.worktrees ignored
#   disposable-repo.sh destroy <path>   remove its worktrees, then the repo itself
#
# Both subcommands refuse a path inside this checkout, inside the shared source
# checkout, inside the test home, or inside any git repository that has a remote.
# destroy also refuses a repository that create did not make.
set -euo pipefail

SOURCE_ROOT=/home/cheongnet/gajaeway/state/workspace/source
TEST_HOME=${VERIFY_GAJAEWAY_TEST_HOME:-/home/cheongnet/gajaeway/test-state-firstmate-20261015}
MARKER_NAME=verify-gajaeway-disposable

die() {
	echo "disposable-repo: $*" >&2
	exit 2
}

usage() {
	echo "usage: $0 create|destroy <absolute-path>" >&2
	exit 64
}

[[ $# -eq 2 ]] || usage
command=$1
[[ $2 == /* ]] || die "path must be absolute: $2"
target=$(realpath -m -- "$2")
[[ $target != / ]] || die "refusing /"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
checkout=$(git -C "$script_dir" rev-parse --show-toplevel)
checkout=$(realpath -m -- "$checkout")

inside() {
	# inside <path> <root>: true when path equals root or is below it.
	[[ $1 == "$2" || $1 == "$2"/* ]]
}

refuse_protected() {
	local path=$1
	inside "$path" "$checkout" && die "refusing a path inside this checkout ($checkout): $path"
	inside "$path" "$(realpath -m -- "$SOURCE_ROOT")" && die "refusing a path inside $SOURCE_ROOT: $path"
	inside "$path" "$(realpath -m -- "$TEST_HOME")" && die "refusing a path inside the test home $TEST_HOME: $path"
	# The nearest existing ancestor decides whether the path lands inside another repository.
	local probe=$path
	while [[ ! -e $probe ]]; do probe=$(dirname -- "$probe"); done
	[[ -d $probe ]] || die "nearest existing ancestor is not a directory: $probe"
	if git -C "$probe" rev-parse --git-dir >/dev/null 2>&1; then
		local remotes
		remotes=$(git -C "$probe" remote)
		[[ -z $remotes ]] || die "refusing a path in a repository with remotes ($remotes): $path"
	fi
	return 0
}

create() {
	refuse_protected "$target"
	if [[ -e $target ]]; then
		[[ -d $target ]] || die "target exists and is not a directory: $target"
		[[ -z $(ls -A -- "$target") ]] || die "target exists and is not empty: $target"
	fi
	mkdir -p -- "$target"
	git -C "$target" init --quiet --initial-branch=main
	git -C "$target" config user.name "verify-gajaeway"
	git -C "$target" config user.email "verify-gajaeway@localhost"
	git -C "$target" config commit.gpgsign false
	printf '/.worktrees\n' >"$target/.gitignore"
	printf '# Disposable lane model switch smoke\n\nThis repository is scratch state for verify-gajaeway.\n' >"$target/README.md"
	git -C "$target" add .gitignore README.md
	git -C "$target" commit --quiet -m "verify-gajaeway setup"
	date -u +%Y-%m-%dT%H:%M:%SZ >"$(git -C "$target" rev-parse --absolute-git-dir)/$MARKER_NAME"
	# Post-conditions, so a broken create never reaches the live run.
	[[ -z $(git -C "$target" remote) ]] || die "post-check: remote present"
	[[ $(git -C "$target" rev-list --count HEAD) == 1 ]] || die "post-check: expected exactly one commit"
	git -C "$target" check-ignore -q .worktrees/probe || die "post-check: /.worktrees is not ignored"
	echo "created $target head=$(git -C "$target" rev-parse --short HEAD) branch=main remotes=none"
}

destroy() {
	refuse_protected "$target"
	if [[ ! -e $target ]]; then
		echo "absent $target"
		return 0
	fi
	local git_dir
	git_dir=$(git -C "$target" rev-parse --absolute-git-dir 2>/dev/null) || die "not a git repository: $target"
	[[ $(realpath -m -- "$(git -C "$target" rev-parse --show-toplevel)") == "$target" ]] ||
		die "path is not the repository root: $target"
	[[ -f $git_dir/$MARKER_NAME ]] || die "refusing a repository create did not make (no $MARKER_NAME marker): $target"
	# Remove every linked worktree this repository registered, wherever it lives.
	local line worktree
	local -a linked=()
	while IFS= read -r line; do
		[[ $line == "worktree "* ]] || continue
		worktree=$(realpath -m -- "${line#worktree }")
		[[ $worktree == "$target" ]] && continue
		linked+=("$worktree")
	done < <(git -C "$target" worktree list --porcelain)
	for worktree in "${linked[@]}"; do
		refuse_protected "$worktree"
		git -C "$target" worktree remove --force --force -- "$worktree" ||
			die "failed to remove worktree $worktree"
		echo "removed worktree $worktree"
	done
	git -C "$target" worktree prune
	rm -rf -- "$target"
	echo "destroyed $target"
}

case $command in
create) create ;;
destroy) destroy ;;
*) usage ;;
esac

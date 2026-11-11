#!/usr/bin/env bash
# Deterministic. No model call. Read-only: never writes the test home, never
# signals a process, never opens gateway.db.
#
#   doctor-test-home.sh [test-home]
#
# Passes (exit 0) only when every check below passes. Prints one line per check.
set -uo pipefail

TEST_HOME=$(realpath -m -- "${1:-${VERIFY_GAJAEWAY_TEST_HOME:-/home/cheongnet/gajaeway/test-state-firstmate-20261015}}")
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
checkout=$(git -C "$script_dir" rev-parse --show-toplevel) || exit 2
PATCH=$checkout/packages/cli/src/patches/gajae-code/0.19.3-lane-switch.patch
BINARY=$TEST_HOME/bin/gajaeway-gateway.new
CONFIG=$TEST_HOME/config.json
STAMP=$TEST_HOME/gjc/0.19.3/.lane-switch-patch.sha256

failures=0
pass() { echo "PASS $*"; }
fail() {
	echo "FAIL $*"
	failures=$((failures + 1))
}

# 1. Lane switch policy is on in the file the gateway loads.
if [[ $(jq -r '.work.switchAfterEdit' "$CONFIG" 2>/dev/null) == true ]]; then
	pass "config.work.switchAfterEdit=true ($CONFIG)"
else
	fail "config.work.switchAfterEdit is not true ($CONFIG)"
fi

# 2. The installed GJC 0.19.3 was patched with this checkout's patch bytes.
expected=$(sha256sum -- "$PATCH" 2>/dev/null | cut -d' ' -f1)
recorded=$(tr -d '[:space:]' <"$STAMP" 2>/dev/null)
if [[ -n $expected && $recorded == "$expected" ]]; then
	pass "patch sha256 $expected matches $STAMP"
else
	fail "patch sha256 mismatch: stamp=${recorded:-missing} patch=${expected:-missing} ($PATCH)"
fi

# 3. The deployed gateway binary carries the flag and the env name it forwards.
for needle in switchAfterEdit GAJAEWAY_LANE_SWITCH; do
	if grep -a -q -F -- "$needle" "$BINARY" 2>/dev/null; then
		pass "$BINARY contains $needle"
	else
		fail "$BINARY does not contain $needle"
	fi
done

# 4. The running gateway daemon for this home is that binary. The broker for the
#    same home runs bin/gajaeway-gateway; only the daemon serves work lanes.
daemons=()
for proc in /proc/[0-9]*; do
	[[ -r $proc/environ && -r $proc/cmdline ]] || continue
	tr '\0' '\n' 2>/dev/null <"$proc/environ" | grep -q -x -F "GAJAEWAY_HOME=$TEST_HOME" || continue
	mapfile -d '' -t argv 2>/dev/null <"$proc/cmdline" || continue
	[[ ${#argv[@]} -ge 2 && ${argv[1]} == daemon ]] || continue
	daemons+=("${proc#/proc/}")
done
if [[ ${#daemons[@]} -eq 0 ]]; then
	fail "no gateway daemon is running with GAJAEWAY_HOME=$TEST_HOME"
else
	config_mtime=$(stat -c %Y -- "$CONFIG")
	now=$(date +%s)
	for pid in "${daemons[@]}"; do
		mapfile -d '' -t argv <"/proc/$pid/cmdline"
		exe=$(readlink "/proc/$pid/exe" 2>/dev/null)
		if [[ ${argv[0]} == "$BINARY" && $exe == "$BINARY" ]]; then
			pass "daemon pid=$pid command=${argv[0]} exe=$exe"
		else
			fail "daemon pid=$pid command=${argv[0]} exe=${exe:-unreadable}, expected $BINARY"
		fi
		# work.* is restart-required: a daemon older than config.json runs the old policy.
		started=$((now - $(ps -o etimes= -p "$pid" | tr -d ' ')))
		if ((started > config_mtime)); then
			pass "daemon pid=$pid started $(date -u -d "@$started" +%FT%TZ) after config.json mtime $(date -u -d "@$config_mtime" +%FT%TZ)"
		else
			fail "daemon pid=$pid started $(date -u -d "@$started" +%FT%TZ), before config.json mtime $(date -u -d "@$config_mtime" +%FT%TZ); work.switchAfterEdit is not live"
		fi
	done
fi

if ((failures > 0)); then
	echo "doctor: $failures check(s) failed"
	exit 1
fi
echo "doctor: ok"

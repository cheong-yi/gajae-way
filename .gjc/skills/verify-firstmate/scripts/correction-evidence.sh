#!/usr/bin/env bash
# Live speed only. Run after the secondmate submitted a correction, never as part
# of the deterministic command. Makes no model call. Read-only except the work
# status query and a read of the test gateway journal.
#
#   correction-evidence.sh --lane NAME --session SESSION_ID --prior OP_REF --since <UTC ISO time> [test-home]
#
# SESSION_ID and OP_REF are the lane's session and its settled attempt before
# the correction. --since is a time after that attempt settled and before the
# correction was asked for.
#
# Exit codes:
# It does not check who submitted the correction, the pane report, the pane's binary,
# the commit, or the local origin. The feature file records those separately.
#
#   0  same session id, a new attempt after --since that ended completed with end_turn
#      (or with no op reason once work status no longer includes the op),
#      a user message appended to that session's JSONL after --since, and no other
#      session JSONL for the lane's cwd since --since
#   1  work status names another session, or its attempt is still OP_REF or started before --since
#   2  no user message was appended to the session's JSONL after --since
#   3  a replacement session JSONL for the lane's cwd started after --since
#   4  the new attempt ended, and not completed with end_turn
#   5  the new attempt is still open. Not a result.
#   6  the session's JSONL was not found
#  64  usage
set -uo pipefail

usage() {
	echo "usage: $0 --lane NAME --session SESSION_ID --prior OP_REF --since <UTC ISO time> [test-home]" >&2
	exit 64
}

[[ $# -ge 8 && $1 == --lane && $3 == --session && $5 == --prior && $7 == --since ]] || usage
lane=$2
session=$4
prior=$6
since=$(date -u -d "$8" +%s 2>/dev/null) || usage
[[ -n $lane && -n $session && -n $prior ]] || usage
[[ $# -le 9 ]] || usage
TEST_HOME=$(realpath -m -- "${9:-${VERIFY_GAJAEWAY_TEST_HOME:-/home/cheongnet/gajaeway/test-state-firstmate-20261015}}")
SESSIONS=$TEST_HOME/broker/agent/sessions
CLI=$TEST_HOME/bin/gajaeway
SOCK=$TEST_HOME/gateway.sock
since_iso=$(date -u -d "@$since" +%FT%TZ)

echo "since=$since_iso lane=$lane session=$session prior=$prior sessions=$SESSIONS"

shopt -s nullglob
files=("$SESSIONS"/*/*_"$session".jsonl)
if ((${#files[@]} != 1)); then
	echo "FAIL session JSONL for $session: found ${#files[@]}"
	exit 6
fi
file=${files[0]}
cwd=$(head -n 1 -- "$file" | jq -r 'select(.type == "session") | .cwd')
echo "session file=$file cwd=$cwd"

replacements=()
for other in "$SESSIONS"/*/*.jsonl; do
	[[ $other == "$file" ]] && continue
	match=$(head -n 1 -- "$other" | jq -r --argjson since "$since" --arg cwd "$cwd" '
		select(.type == "session" and .cwd == $cwd)
		| select((.timestamp | sub("\\.[0-9]+"; "") | fromdateiso8601) >= $since)
		| .id' 2>/dev/null)
	[[ -n $match ]] && replacements+=("$other")
done
if ((${#replacements[@]} > 0)); then
	echo "FAIL replacement session(s) for $cwd since $since_iso:"
	printf '  %s\n' "${replacements[@]}"
else
	echo "PASS no other session JSONL for $cwd since $since_iso"
fi

appended=$(jq -c --argjson since "$since" '
	select(.type == "message" and .message.role == "user")
	| select((.timestamp | sub("\\.[0-9]+"; "") | fromdateiso8601) >= $since)
	| {timestamp, text: ([.message.content[]? | select(.type == "text") | .text] | join(" ") | .[0:160])}' -- "$file" 2>/dev/null | head -n 1)
if [[ -n $appended ]]; then
	echo "PASS appended in place $appended"
else
	echo "FAIL no user message appended to $file after $since_iso"
fi

status=$("$CLI" --socket "$SOCK" work status "$lane" 2>/dev/null) || status=""
if [[ -z $status ]]; then
	echo "FAIL work status $lane failed"
	exit 1
fi
echo "public status $status"
status_session=$(jq -r '.sessionId // empty' <<<"$status")
op=$(jq -r '.attempt.opRef // empty' <<<"$status")
started=$(jq -r '.attempt.startedAt // empty' <<<"$status")
ended=$(jq -r '.attempt.endedAt // empty' <<<"$status")
end_state=$(jq -r '.attempt.endState // empty' <<<"$status")
reason=$(jq -r '.op.outcome.reason // empty' <<<"$status")
identity=0
if [[ $status_session != "$session" ]]; then
	echo "FAIL work status session $status_session is not $session"
	identity=1
elif [[ -z $op || $op == "$prior" ]]; then
	echo "FAIL no new attempt: latest is ${op:-none}"
	identity=1
elif (($(date -u -d "$started" +%s) < since)); then
	echo "FAIL latest attempt $op started $started, before $since_iso"
	identity=1
else
	echo "PASS same session $session, new attempt $op started $started"
fi

journal=$(XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)} journalctl --user -u gajaeway-test-gateway \
	--since "@$since" --no-pager -o cat 2>/dev/null | grep -E "correction|$lane" | grep -E "correction|bind|resum|replace" | head -n 20)
[[ -n $journal ]] && printf 'journal %s\n' "${journal//$'\n'/$'\n'journal }"

if ((${#replacements[@]} > 0)); then
	exit 3
elif ((identity)); then
	exit 1
elif [[ -z $ended ]]; then
	echo "correction-evidence: pending"
	exit 5
elif [[ -z $appended ]]; then
	exit 2
elif [[ $end_state != completed || ($reason != end_turn && -n $reason) ]]; then
	echo "FAIL attempt $op ended $end_state reason=${reason:-none}"
	exit 4
fi
echo "PASS attempt $op ended $ended $end_state"
echo "correction-evidence: ok"

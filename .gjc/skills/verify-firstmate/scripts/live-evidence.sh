#!/usr/bin/env bash
# Live speed only. Run after the trigger, never as part of the deterministic
# command. Makes no model call. Read-only except the work status query.
#
#   live-evidence.sh --since <UTC ISO time> [test-home]
#
# Exit codes:
#   0  one lane_model_switch line, no warn line, and the public attempt ended after that switch
#   1  the attempt ended with no lane_model_switch line, or with more than one
#   2  a lane_model_switch warn line exists
#   3  no worker session JSONL since --since
#   4  the attempt ended, and the public end is missing or before the switch
#   5  a worker exists and the attempt is still open. Not a result.
#  64  usage
set -uo pipefail

usage() {
	echo "usage: $0 --since <UTC ISO time, e.g. 2026-11-11T07:58:33Z> [test-home]" >&2
	exit 64
}

[[ $# -ge 2 && $1 == --since ]] || usage
since=$(date -u -d "$2" +%s 2>/dev/null) || usage
TEST_HOME=$(realpath -m -- "${3:-${VERIFY_GAJAEWAY_TEST_HOME:-/home/cheongnet/gajaeway/test-state-firstmate-20261015}}")
SESSIONS=$TEST_HOME/broker/agent/sessions
LANES=$TEST_HOME/lanes
SCRATCH_ROOT=/home/cheongnet/gajaeway/verify-scratch
LOG_DIR=${VERIFY_GAJAEWAY_GJC_LOG_DIR:-$HOME/.gjc/logs}
CLI=$TEST_HOME/bin/gajaeway
SOCK=$TEST_HOME/gateway.sock

echo "since=$(date -u -d "@$since" +%FT%TZ) sessions=$SESSIONS lanes=$LANES logs=$LOG_DIR"

workers=0
missing=0
pending=0
late=0
for file in "$SESSIONS"/*/*.jsonl; do
	[[ -f $file ]] || continue
	header=$(head -n 1 -- "$file" | jq -r --argjson since "$since" '
		select(.type == "session")
		| select((.timestamp | sub("\\.[0-9]+"; "") | fromdateiso8601) >= $since)
		| .cwd' 2>/dev/null) || continue
	[[ -n $header && ( $header == "$LANES"/* || $header == "$SCRATCH_ROOT"/* ) ]] || continue
	workers=$((workers + 1))
	switches=$(jq -c 'select(.type == "model_change" and ((.reason // "") | startswith("lane_model_switch ")))
		| {timestamp, model, reason, thinkingLevel}' -- "$file" 2>/dev/null)
	count=$(grep -c . <<<"$switches")
	echo "worker session=$file cwd=$header"
	if [[ $count -ne 1 ]]; then
		base=$(basename -- "$header")
		job=${base#wt-}
		status=""
		if [[ $base == wt-* && -n $job ]]; then
			status=$("$CLI" --socket "$SOCK" work status "$job" 2>/dev/null) || status=""
		fi
		ended=$(jq -r '.attempt.endedAt // empty' <<<"${status:-{\}}")
		if [[ $count -eq 0 && -n $status && -z $ended ]]; then
			echo "PENDING no switch yet job=$job state=$(jq -r '.state // empty' <<<"$status")"
			pending=$((pending + 1))
		else
			echo "FAIL expected exactly one lane_model_switch model_change, found $count"
			[[ $count -gt 0 ]] && echo "$switches"
			missing=$((missing + 1))
		fi
		continue
	fi
	echo "PASS model_change $switches"
	base=$(basename -- "$header")
	job=${base#wt-}
	if [[ $base != wt-* || -z $job ]]; then
		echo "FAIL public end: cwd has no wt- job name: $header"
		late=$((late + 1))
		continue
	fi
	status=$("$CLI" --socket "$SOCK" work status "$job" 2>/dev/null) || status=""
	if [[ -z $status ]]; then
		echo "FAIL public end: work status $job failed"
		late=$((late + 1))
		continue
	fi
	echo "public status $status"
	switch_ts=$(jq -r '.timestamp' <<<"$switches")
	ended=$(jq -r '.attempt.endedAt // empty' <<<"$status")
	receipt=$(jq -r '.op.receiptState // empty' <<<"$status")
	reason=$(jq -r '.op.outcome.reason // empty' <<<"$status")
	opstatus=$(jq -r '.op.status // empty' <<<"$status")
	state=$(jq -r '.state // empty' <<<"$status")
	if [[ -z $ended || $receipt != present || $reason != end_turn || $opstatus != terminal_ok ]]; then
		if [[ -z $ended && $opstatus == in_flight ]]; then
			echo "PENDING switch recorded, attempt still open job=$job"
			pending=$((pending + 1))
		elif [[ -n $ended && ( $opstatus == null || -z $opstatus ) ]]; then
			end_state=$(jq -r '.attempt.endState // empty' <<<"$status")
			ended_s=$(date -u -d "$ended" +%s)
			switch_s=$(date -u -d "$switch_ts" +%s)
			if [[ $end_state == completed && ended_s -ge switch_s ]]; then
				echo "PASS attempt ended $ended after switch $switch_ts; status no longer includes the op receipt"
				continue
			fi
			echo "FAIL public attempt has not ended after the switch job=$job state=$state"
			late=$((late + 1))
		else
			echo "FAIL public attempt has not ended after the switch job=$job state=$state"
			late=$((late + 1))
		fi
		continue
	fi
	ended_s=$(date -u -d "$ended" +%s)
	switch_s=$(date -u -d "$switch_ts" +%s)
	if ((ended_s < switch_s)); then
		echo "FAIL public end $ended is before switch $switch_ts"
		late=$((late + 1))
		continue
	fi
	echo "PASS public end $ended after switch $switch_ts state=$state"
done

warns=$(
	shopt -s nullglob
	logs=("$LOG_DIR"/gjc.*.log "$LOG_DIR"/gjc.*.log.gz)
	((${#logs[@]} > 0)) || exit 0
	zgrep -h -F -e "lane_model_switch executor assignment is missing" -e "lane_model_switch failed" -- "${logs[@]}" |
		jq -R -c --argjson since "$since" '
			fromjson?
			| select(((.timestamp // "") | sub("\\.[0-9]+"; "") | sub("\\+00:00$"; "Z") | fromdateiso8601? // 0) >= $since)
			| {timestamp, pid, message}'
)
if [[ -n $warns ]]; then
	echo "FAIL lane_model_switch warn line(s); the strong model stayed:"
	echo "$warns"
fi

if [[ -n $warns ]]; then
	exit 2
elif ((workers == 0)); then
	echo "FAIL no worker session JSONL under $SESSIONS with cwd under $LANES or $SCRATCH_ROOT since --since"
	exit 3
elif ((missing > 0)); then
	exit 1
elif ((late > 0)); then
	exit 4
elif ((pending > 0)); then
	echo "live-evidence: pending workers=$workers"
	exit 5
fi
echo "live-evidence: ok workers=$workers"

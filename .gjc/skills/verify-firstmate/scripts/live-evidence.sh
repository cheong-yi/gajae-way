#!/usr/bin/env bash
# Live speed only. Run after the trigger, never as part of the deterministic
# command. Makes no model call. Read-only except the work status query.
#
#   live-evidence.sh --since <UTC ISO time> [--expect-continue] [test-home]
#
# Every worker with one switch prints one NUDGES line: the session-log index and time of the
# prewalk-plan nudge, the prewalk-continue nudge, the first successful
# todo_write result and the lane_model_switch, or none for each.
#
# Exit codes:
#   0  one lane_model_switch line, no warn line, and the public attempt ended after that switch
#   1  the attempt ended with no lane_model_switch line, or with more than one
#   2  a lane_model_switch warn line exists
#   3  no worker session JSONL since --since
#   4  the attempt ended, and the public end is missing or before the switch
#   5  a worker exists and the attempt is still open. Not a result.
#   6  the first successful edit-type result was not from the planning model after a successful
#      todo_write, a later result before the switch came from the switch model, or a result
#      after the switch came from the planning model. Results before the switch line are the
#      first batch: the switch is recorded after that turn finishes.
#   7  the switch's model_change thinkingLevel is missing or differs from the level that switch
#      applied, the level suffix on the next default configured_model_chain entry
#   8  --expect-continue only: no prewalk-continue nudge, or it is not before the first
#      successful todo_write, or that todo_write is not before the switch
#  64  usage
set -uo pipefail

usage() {
	echo "usage: $0 --since <UTC ISO time, e.g. 2026-11-11T07:58:33Z> [--expect-continue] [test-home]" >&2
	exit 64
}

[[ $# -ge 2 && $1 == --since ]] || usage
since=$(date -u -d "$2" +%s 2>/dev/null) || usage
shift 2
expect_continue=0
if [[ ${1:-} == --expect-continue ]]; then
	expect_continue=1
	shift
fi
[[ $# -le 1 ]] || usage
TEST_HOME=$(realpath -m -- "${1:-${VERIFY_GAJAEWAY_TEST_HOME:-/home/cheongnet/gajaeway/test-state-firstmate-20261015}}")
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
edits=0
thinking=0
nudges=0
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
	nudge_line=$(jq -s -r '
		. as $entries
		| def at(f): ([$entries | to_entries[] | select(.value | f)] | first
			| if . == null then null else {index: .key, timestamp: .value.timestamp} end);
		def show(x): if x == null then "none" else "\(x.index)@\(x.timestamp)" end;
		at(.type == "custom_message" and .customType == "prewalk-plan") as $plan
		| at(.type == "custom_message" and .customType == "prewalk-continue") as $continue
		| at(.type == "message" and .message.role == "toolResult" and .message.toolName == "todo_write"
			and .message.isError == false) as $todo
		| at(.type == "model_change" and ((.reason // "") | startswith("lane_model_switch "))) as $switch
		| "NUDGES plan=\(show($plan)) continue=\(show($continue)) todo_write=\(show($todo)) switch=\(show($switch))"
			+ (if $continue != null and $todo != null and $switch != null
				and $continue.index < $todo.index and $todo.index < $switch.index
				then " order=continue<todo_write<switch" else " order=incomplete" end)
	' -- "$file" 2>/dev/null)
	echo "$nudge_line"
	if ((expect_continue)) && [[ $nudge_line != *" order=continue<todo_write<switch" ]]; then
		echo "FAIL expected a prewalk-continue nudge, then a successful todo_write, then the switch"
		nudges=$((nudges + 1))
	fi
	thinking_report=$(jq -s -r '
		. as $entries
		| ([$entries | to_entries[]
			| select(.value.type == "model_change" and ((.value.reason // "") | startswith("lane_model_switch ")))]
			| first) as $switch
		| ([$entries | to_entries[]
			| select(.key > $switch.key and .value.type == "configured_model_chain" and .value.role == "default")]
			| first) as $chain
		| ($switch.value.thinkingLevel // null) as $marker
		| ((($chain.value.entries[0] // "") | capture(":(?<level>minimal|low|medium|high|xhigh|max|off)$") | .level) // null) as $applied
		| if $chain == null then "FAIL switch thinking level: no default configured_model_chain after the switch"
			elif $marker != $applied then "FAIL switch thinking level marker=\($marker) applied=\($applied)"
			else "PASS switch thinking level \($marker) matches the applied chain \($chain.value.entries[0])" end
	' -- "$file" 2>/dev/null)
	echo "$thinking_report"
	[[ $thinking_report == PASS* ]] || thinking=$((thinking + 1))
	edit_report=$(jq -s -r '
		. as $entries
		| def on_switch($assistant; $switch_model):
			($assistant != null)
			and (($assistant.model == $switch_model) or ($switch_model | startswith($assistant.model + ":")));
		([$entries | to_entries[]
			| select(.value.type == "model_change" and ((.value.reason // "") | startswith("lane_model_switch ")))]
			| first) as $switch
		| select($switch != null)
		| ($switch.key) as $switch_index
		| ($switch.value.model // "" | sub(":(minimal|low|medium|high|xhigh|max|off)$"; "")) as $switch_model
		| ([$entries | to_entries[]
			| select(.value.type == "message" and .value.message.role == "assistant")
			| {
				index: .key,
				model: "\(.value.message.provider)/\(.value.message.model)",
				calls: [.value.message.content[]? | select(.type == "toolCall") | .id],
			}]) as $assistants
		| ([$entries | to_entries[]
			| select(.value.type == "message" and .value.message.role == "toolResult")
			| select(.value.message.isError == false)
			| select(.value.message.toolName == "todo_write")
			| .key]) as $todos
		| ([$entries | to_entries[]
			| select(.value.type == "message" and .value.message.role == "toolResult")
			| select(.value.message.isError == false)
			| select(.value.message.toolName == "edit" or .value.message.toolName == "write"
				or .value.message.toolName == "apply_patch" or .value.message.toolName == "ast_edit")
			| .key as $result_index
			| .value.message as $result
			| ($assistants | map(select(.calls | index($result.toolCallId))) | first) as $assistant
			| {index: $result_index, tool: $result.toolName, toolCallId: $result.toolCallId, assistant: $assistant}]) as $edits
		| if ($edits | length) == 0 then "PASS edits none on \($switch_model); no edit-type result recorded"
			else
				($edits[0]) as $first
				| ($edits[1:]) as $later
				| ([
					(if $first.assistant == null
						then "FAIL first edit has no assistant message tool=\($first.tool) toolCallId=\($first.toolCallId)"
						elif on_switch($first.assistant; $switch_model)
						then "FAIL first edit came from the switch model tool=\($first.tool) assistant_model=\($first.assistant.model)"
						else empty end),
					(if ([$todos[] | select(. < $first.index)] | length) == 0
						then "FAIL first edit came before a successful todo_write tool=\($first.tool) result_index=\($first.index)"
						else empty end),
					($later[]
						| if .assistant == null
							then "FAIL later edit has no assistant message tool=\(.tool) toolCallId=\(.toolCallId) result_index=\(.index)"
							elif ((.index < $switch_index) or (.assistant.index < $switch_index)) and on_switch(.assistant; $switch_model)
							then "FAIL later edit came from the switch model before the lane_model_switch tool=\(.tool) result_index=\(.index) switch_index=\($switch_index)"
							elif ((.index < $switch_index) or (.assistant.index < $switch_index) | not) and (on_switch(.assistant; $switch_model) | not)
							then "FAIL later edit came from the planning model tool=\(.tool) assistant_model=\(.assistant.model) switch_model=\($switch_model)"
							else empty end)
				]) as $violations
				| if ($violations | length) > 0 then $violations[]
					else "PASS edits first on planning model \($first.assistant.model) after todo_write, \($later | map(select(.index < $switch_index)) | length) more in that batch, then \($later | map(select(.index >= $switch_index)) | length) on \($switch_model) after the switch" end
			end
	' -- "$file" 2>/dev/null)
	if [[ -n $edit_report ]]; then
		printf '%s\n' "$edit_report"
		edits=$((edits + $(grep -c '^FAIL ' <<<"$edit_report")))
	fi
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
elif ((edits > 0)); then
	exit 6
elif ((thinking > 0)); then
	exit 7
elif ((nudges > 0)); then
	exit 8
elif ((late > 0)); then
	exit 4
elif ((pending > 0)); then
	echo "live-evidence: pending workers=$workers"
	exit 5
fi
echo "live-evidence: ok workers=$workers"

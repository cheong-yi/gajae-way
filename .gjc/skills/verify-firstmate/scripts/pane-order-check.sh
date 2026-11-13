#!/usr/bin/env bash
# Local checks for the pane-order contract and the recorded pane parent.
# Exit 0 means those unit tests passed. It does not prove a live Discord parent_id.
set -uo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
checkout=$(git -C "$script_dir" rev-parse --show-toplevel)
cd -- "$checkout"

echo "pane-order-check: adapter match and gateway recording"
bun test \
	packages/adapter-discord/test/pane.test.ts \
	packages/gateway/test/firstmate-pane-wire.test.ts \
	packages/gateway/test/lane-parent-report.test.ts
wire=$?

echo "pane-order-check: closeout holds the report until the pane turn finishes"
bun test packages/gateway/test/server.test.ts -t "holds a steered native report"
closeout=$?

if ((wire != 0 || closeout != 0)); then
	echo "pane-order-check: failed wire=$wire closeout=$closeout"
	exit 1
fi
echo "pane-order-check: ok"
echo "pane-order-check: live Discord parent_id is not covered by this command"

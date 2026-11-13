# Pane parent channel

A FirstMate pane thread is opened under the matched repo channel, and the gateway records that channel as the pane's child parent. `--firstmate` still creates no lane. For this checkout the origin name is `gajae-way`, so the match is channel `1570682840037728449`, named `repo-gajae-way`, under ACTIVE REPOS. That hit is the expected live parent. Channel `1511673764574793798` is the fallback only when the slug is missing or no `repo-<slug>` channel exists. Uncovered live.

## Sub-features

- `matched-parent` opens this checkout's pane under channel `1570682840037728449`. A hit on `repo-gajae-way` is the expected live result, not a false pass.
- `fallback-parent` opens the pane under channel `1511673764574793798` only when the slug is missing or no `repo-<slug>` channel exists.
- `recorded-parent` records that channel id as `childOrigin.parentId`. A thread id without it records no pane.

## How to get to it (user POV)

- In channel `1511208447939706972`, start a new thread and mention the test bot `1557246454714736723`.
- Ask for one lane on a local repository whose origin name is `gajae-way`, and ask the lane not to start another lane.
- The first mate records the assignment and opens the pane. The pane's ordinary work start creates the lane.

## Driving it with verify-firstmate

Preconditions:

- The test gateway is already running. Do not restart it.
- This feature is uncovered. There is no passing live evidence.
- This checkout's expected live pane parent is channel `1570682840037728449`. The test gateway already has that channel. The test adapter process has not been restarted, so it does not have the new id in memory yet. Do not restart it.
- Do not add a unit test that treats channel `1570682840037728449` as a pass. The live check is the Discord thread `parent_id`.

- **Local check.** Run `.gjc/skills/verify-firstmate/scripts/pane-order-check.sh`. Exit 0 proves the adapter match and that the gateway records the channel the adapter reports. It does not prove Discord placed the thread there.
- **Start the assignment.** Post the mention described above. An operator-bot post is a stand-in for a human mention; say so in the evidence.
- **Read Discord.** Open the pane thread and read its `parent_id`.
- **Evidence.** Quote the pane thread id and its `parent_id` from Discord. The live check is that value, not a unit test. Do not use `gateway.db` or `bun test` as the live evidence.

## Gotchas

- A unit test proves the adapter returns a channel id. It does not prove Discord put the thread under that channel. The live check is the thread's `parent_id` in Discord.
- A hit on `repo-gajae-way` for origin `gajae-way` is the expected live result for this checkout. Do not call that hit a false pass, and do not invent a unit test that passes by naming channel `1570682840037728449`.
- The recorded pane child parent is the channel the adapter fetched, not the assignment thread's parent.
- Driven on `pane-parent-20261113T062237Z` by an operator-bot stand-in, before this channel was the expected match and while the test adapter still lacked the new id. Pane thread `1570688678936842280` has `parent_id` `1511208447939706972`, not `1570682840037728449`. Not a pass.

import { type OriginRef, originKey, validateOriginRef } from "@gajae-gateway/protocol";
import type { Store } from "../store";
import { FirstmateStore } from "../store/groups/work";

/** Gateway creation provenance, not a session binding or a native lane session. */
export interface FirstmatePane {
	readonly parentOrigin: OriginRef;
	readonly childOrigin: OriginRef;
	readonly taskId: string;
	readonly laneName: string;
	readonly personaOriginKey: string;
	readonly createdByGateway: string;
}

const CHILD_PREFIX = "firstmate-pane/child/";
const LANE_PREFIX = "firstmate-pane/lane/";

function parsePane(raw: string | undefined): FirstmatePane | undefined {
	if (!raw) return undefined;
	try {
		const pane = JSON.parse(raw) as FirstmatePane;
		validateOriginRef(pane.parentOrigin);
		validateOriginRef(pane.childOrigin);
		if (
			pane.parentOrigin.platform !== "discord" ||
			!["channel", "thread"].includes(pane.parentOrigin.kind) ||
			pane.childOrigin.platform !== "discord" ||
			pane.childOrigin.kind !== "thread" ||
			pane.childOrigin.threadVisibility !== "public" ||
			pane.childOrigin.parentId !==
				(pane.parentOrigin.kind === "thread" ? pane.parentOrigin.parentId : pane.parentOrigin.conversationId) ||
			pane.personaOriginKey !== originKey(pane.parentOrigin) ||
			![pane.taskId, pane.laneName, pane.createdByGateway].every(
				(value) => typeof value === "string" && value.length > 0,
			)
		)
			return undefined;
		return pane;
	} catch {
		return undefined;
	}
}

function validPane(store: Store, raw: string | undefined): FirstmatePane | undefined {
	const pane = parsePane(raw);
	if (!pane || pane.createdByGateway !== store.instanceId) return undefined;
	const child = store.core.metaGet(CHILD_PREFIX + originKey(pane.childOrigin));
	const lane = store.core.metaGet(LANE_PREFIX + pane.laneName);
	if (child !== raw || lane !== raw) return undefined;
	try {
		const assignment = new FirstmateStore(store.core).assignment(pane.laneName);
		return assignment?.taskId === pane.taskId && originKey(assignment.thread) === pane.personaOriginKey
			? pane
			: undefined;
	} catch {
		return undefined;
	}
}

export function readLanePane(store: Store, name: string): FirstmatePane | undefined {
	return validPane(store, store.core.metaGet(LANE_PREFIX + name));
}

export function readChildPane(store: Store, origin: OriginRef): FirstmatePane | undefined {
	const pane = validPane(store, store.core.metaGet(CHILD_PREFIX + originKey(origin)));
	return pane && origin.threadVisibility !== "private" && originKey(pane.childOrigin) === originKey(origin)
		? pane
		: undefined;
}

export function childPanes(store: Store): FirstmatePane[] {
	return store.core
		.metaKeysInRange(LANE_PREFIX, `${LANE_PREFIX}\uffff`)
		.flatMap((key) => validPane(store, store.core.metaGet(key)) ?? []);
}

export function paneOriginRecorded(store: Store, origin: OriginRef): boolean {
	if (store.core.metaGet(CHILD_PREFIX + originKey(origin)) !== undefined) return true;
	return store.core.metaKeysInRange(LANE_PREFIX, `${LANE_PREFIX}\uffff`).some((key) => {
		const pane = parsePane(store.core.metaGet(key));
		return pane !== undefined && originKey(pane.childOrigin) === originKey(origin);
	});
}

/** A damaged index is evidence of an unavailable pane, never authority to fall back. */
export function paneDeliveryGuard(store: Store, origin: OriginRef): true | undefined {
	if (origin.kind !== "thread") return undefined;
	if (readChildPane(store, origin)) return true;
	if (paneOriginRecorded(store, origin)) throw new Error("FirstMate child provenance unavailable");
	return undefined;
}

export function saveChildPane(store: Store, pane: FirstmatePane): void {
	const raw = JSON.stringify(pane);
	if (!parsePane(raw) || pane.createdByGateway !== store.instanceId)
		throw new Error("invalid FirstMate child provenance");
	store.transaction(() => {
		const existing = store.core.metaGet(LANE_PREFIX + pane.laneName);
		if (existing !== undefined && existing !== raw) throw new Error("FirstMate child already recorded");
		const child = store.core.metaGet(CHILD_PREFIX + originKey(pane.childOrigin));
		if (child !== undefined && child !== raw) throw new Error("FirstMate child already owned");
		store.core.metaSet(CHILD_PREFIX + originKey(pane.childOrigin), raw);
		store.core.metaSet(LANE_PREFIX + pane.laneName, raw);
	});
}

export function lanePaneRecorded(store: Store, name: string): boolean {
	return store.core.metaGet(LANE_PREFIX + name) !== undefined;
}

/** Prior identities preserve the learner's persisted source-to-receiver direction. */
export const prefixCountKey = (prefix: string, nodeId: string): string => `${prefix}|${nodeId}`;
export const positionPrefixKey = (prefix: string, position: number, nodeId: string): string => `${prefix}|${position}|${nodeId}`;
export const corridorKey = (source: string, receiver: string, position: number, nodeId: string): string => `${source}|${receiver}|${position}|${nodeId}`;
export const transitionKey = (from: string, to: string): string => `${from}|${to}`;
export const positionTransitionKey = (position: number, from: string, to: string): string => `${position}|${from}|${to}`;
export const regionalPrefixGroupKey = (region: string, prefix: string, previous: string): string => `${region}|${prefix}|${previous}`;
export const regionalPrefixKey = (region: string, prefix: string, previous: string, nodeId: string): string => `${regionalPrefixGroupKey(region, prefix, previous)}|${nodeId}`;
export const regionalTransitionKey = (region: string, from: string, to: string): string => `${region}|${from}|${to}`;
export const edgeKey = (region: string, bucket: number, from: string, to: string): string => `${region}|${bucket}|${from}|${to}`;
export const motifKey = (region: string, bucket: number, length: number, nodeIds: string): string => `${region}|${bucket}|${length}|${nodeIds}`;
export const undirectedLinkKey = (a: string, b: string): string => a < b ? `${a}:${b}` : `${b}:${a}`;

const UK_PUBLIC_NETWORKS = new Set(['ukmesh', 'northeast', 'teesside']);

export type PrivatePrefixRow = {
  node_id: string;
  network: string;
  prefix_size_bytes: number;
  prefix: string;
};

export type PacketPrivacyInput = {
  network: string;
  rxNodeId: string | null;
  srcNodeId: string | null;
  pathHashes: string[] | null;
  pathHashSizeBytes: number | null;
};

export type PacketPrivacyClassification = {
  generation: number;
  isPrivate: boolean;
  pathIsValid: boolean;
  visibilityOk: boolean;
};

function networkClass(network: string): string {
  return UK_PUBLIC_NETWORKS.has(network) ? 'ukmesh-public' : network;
}

function normalizedNodeId(value: string | null): string {
  return String(value ?? '').trim().toUpperCase();
}

export class PrivatePrefixCache {
  private generation = 0;
  private nodeKeys = new Set<string>();
  private prefixKeys = new Set<string>();

  get currentGeneration(): number {
    return this.generation;
  }

  replace(generation: number, rows: PrivatePrefixRow[]): void {
    if (!Number.isSafeInteger(generation) || generation < 1) {
      throw new Error('INVALID_PRIVATE_PREFIX_GENERATION');
    }
    const nodeKeys = new Set<string>();
    const prefixKeys = new Set<string>();
    for (const row of rows) {
      const network = networkClass(String(row.network));
      const nodeId = normalizedNodeId(row.node_id);
      const size = Number(row.prefix_size_bytes);
      const prefix = String(row.prefix).trim().toUpperCase();
      if (!/^[0-9A-F]{64}$/.test(nodeId)) continue;
      if (!Number.isInteger(size) || size < 1 || size > 3) continue;
      if (!new RegExp(`^[0-9A-F]{${size * 2}}$`).test(prefix)) continue;
      nodeKeys.add(`${network}\0${nodeId}`);
      prefixKeys.add(`${network}\0${size}\0${prefix}`);
    }
    this.nodeKeys = nodeKeys;
    this.prefixKeys = prefixKeys;
    this.generation = generation;
  }

  classify(input: PacketPrivacyInput): PacketPrivacyClassification {
    const size = input.pathHashSizeBytes;
    const hashes = input.pathHashes ?? [];
    const pathIsValid = hashes.length === 0 || (
      Number.isInteger(size)
      && size! >= 1
      && size! <= 3
      && hashes.every((hash) => (
        typeof hash === 'string'
        && hash.length === size! * 2
        && /^[0-9A-Fa-f]+$/.test(hash)
      ))
    );
    if (this.generation < 1) {
      return { generation: 0, isPrivate: true, pathIsValid, visibilityOk: false };
    }

    const network = networkClass(input.network);
    const isPrivateNode = [input.rxNodeId, input.srcNodeId]
      .map(normalizedNodeId)
      .filter(Boolean)
      .some((nodeId) => this.nodeKeys.has(`${network}\0${nodeId}`));
    const isPrivatePrefix = Number.isInteger(size)
      && hashes.some((hash) => this.prefixKeys.has(
        `${network}\0${size}\0${String(hash).trim().toUpperCase()}`,
      ));
    const isPrivate = isPrivateNode || isPrivatePrefix;
    return {
      generation: this.generation,
      isPrivate,
      pathIsValid,
      visibilityOk: pathIsValid && !isPrivate,
    };
  }
}

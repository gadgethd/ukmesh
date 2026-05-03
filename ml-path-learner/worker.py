"""
ML path learner worker.

Extracts gold-standard path observations from uniquely-resolved multibyte
packets, trains a LightGBM model to predict which node a 1-byte hash maps to,
and writes high-confidence scores back to ml_path_prefix_scores for the lazy
resolver to use as its highest-priority evidence tier.

Training data source:
  Multibyte packets (path_hash_size_bytes > 1) where every hop hash uniquely
  resolves to exactly one positioned node are treated as ground truth.  Their
  hashes are degraded to 2-char (1-byte) prefixes to simulate the hard case.

Feedback loop prevention:
  Only multibyte packet resolutions (never lazy-resolver output) are used as
  labels.  Model predictions are never fed back as training data.
"""

import io
import json
import logging
import math
import os
import random
import time
import warnings
from collections import defaultdict
from datetime import datetime, timezone

import joblib
import numpy as np
import psycopg2
import psycopg2.extras
from lightgbm import LGBMClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.frozen import FrozenEstimator

# LightGBM 4.5 calls sklearn's internal check_array with the old
# force_all_finite= kwarg; suppress until LightGBM is updated.
warnings.filterwarnings('ignore', message=".*force_all_finite.*", category=FutureWarning)
warnings.filterwarnings('ignore', message=".*force_all_finite.*", category=UserWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ml-learner] %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ['DATABASE_URL']
GOLD_INTERVAL_SECS = int(os.environ.get('GOLD_EXTRACTION_INTERVAL_MINS', '15')) * 60
TRAIN_INTERVAL_SECS = int(os.environ.get('TRAINING_INTERVAL_MINS', '30')) * 60
MIN_GOLD_ROWS = int(os.environ.get('MIN_GOLD_PATHS', '100'))
CONFIDENCE_THRESHOLD = float(os.environ.get('CONFIDENCE_THRESHOLD', '0.85'))
MIN_OBSERVATION_COUNT = int(os.environ.get('MIN_OBSERVATION_COUNT', '1'))
PROMOTION_MIN_DELTA = float(os.environ.get('PROMOTION_MIN_DELTA', '0.0'))
MAX_HOP_KM = 150.0
GOLD_BATCH = 5000
CHECKPOINT_KEY = 'gold_extraction_checkpoint'

# ── Genetic / evolutionary search ────────────────────────────────────────────

POPULATION_SIZE = int(os.environ.get('POPULATION_SIZE', '10'))
RANDOM_SEED = int(os.environ.get('RANDOM_SEED', '42'))

DEFAULT_PARAMS: dict = {
    'num_leaves': 31,
    'learning_rate': 0.05,
    'min_child_samples': 20,
    'n_estimators': 300,
    'feature_fraction': 1.0,
    'bagging_fraction': 1.0,
    'bagging_freq': 0,
}

PARAM_BOUNDS: dict = {
    'num_leaves':        (8,    127),
    'learning_rate':     (0.01, 0.20),
    'min_child_samples': (3,    50),
    'n_estimators':      (100,  600),
    'feature_fraction':  (0.5,  1.0),
    'bagging_fraction':  (0.5,  1.0),
    'bagging_freq':      (0,    5),
}

INT_PARAMS = {'num_leaves', 'min_child_samples', 'n_estimators', 'bagging_freq'}


def _clamp_param(key: str, value: float) -> int | float:
    lo, hi = PARAM_BOUNDS[key]
    if key in INT_PARAMS:
        return max(lo, min(hi, int(round(value))))
    return max(lo, min(hi, float(value)))


def random_params() -> dict:
    """Return a fully random hyperparameter set inside the allowed bounds."""
    return {
        key: random.randint(lo, hi) if key in INT_PARAMS else random.uniform(lo, hi)
        for key, (lo, hi) in PARAM_BOUNDS.items()
    }


def mutate(params: dict, strength: float = 0.35) -> dict:
    """Return a new param dict with random perturbations from params."""
    new: dict = {}
    for k, v in params.items():
        if random.random() < 0.85:
            factor = random.uniform(1.0 - strength, 1.0 + strength)
            raw = v * factor
            new[k] = _clamp_param(k, raw)
        else:
            new[k] = v
    return new


def create_population(base_params: dict, generation: int = 1) -> list[dict]:
    """Build a mixed population seeded from the champion plus wider exploration."""
    exploration = min(1.0, 0.30 + (max(0, generation - 1) * 0.08))
    strengths = [0.25, 0.45, 0.70, exploration]
    pop = [base_params.copy()]
    seen = {tuple(sorted(base_params.items()))}
    while len(pop) < POPULATION_SIZE:
        if len(pop) % 4 == 0:
            candidate = random_params()
        else:
            candidate = mutate(base_params, random.choice(strengths))
        key = tuple(sorted(candidate.items()))
        if key in seen:
            continue
        seen.add(key)
        pop.append(candidate)
    return pop


GLOBAL_NETWORK = 'global'
COMBINED_UKMESH_NETWORKS = ('teesside', 'ukmesh')
COMBINED_UKMESH_SCOPE = 'ukmesh_combined'


def network_scope_key(network: str) -> str:
    return COMBINED_UKMESH_SCOPE if network in COMBINED_UKMESH_NETWORKS else network


def network_scope_values(network_or_scope: str) -> list[str]:
    if network_or_scope == COMBINED_UKMESH_SCOPE or network_or_scope in COMBINED_UKMESH_NETWORKS:
        return list(COMBINED_UKMESH_NETWORKS)
    return [network_or_scope]


def get_champion_params(db) -> dict:
    """Return hyperparams of the current global champion, or DEFAULT_PARAMS."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT hyperparams FROM ml_model_versions
                WHERE network = %s AND is_active = TRUE
                ORDER BY promoted_at DESC LIMIT 1""",
            [GLOBAL_NETWORK],
        )
        row = cur.fetchone()
        if row and row['hyperparams']:
            p = row['hyperparams']
            return {k: p.get(k, v) for k, v in DEFAULT_PARAMS.items()}
    return DEFAULT_PARAMS.copy()


def get_current_generation(db) -> int:
    """Return the latest global generation number (0 if none)."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT COALESCE(MAX(generation), 0) AS generation
                 FROM (
                   SELECT generation
                     FROM ml_model_versions
                    WHERE network = %s
                   UNION ALL
                   SELECT generation
                     FROM ml_model_variant_runs
                    WHERE model_network = %s
                 ) generations""",
            [GLOBAL_NETWORK, GLOBAL_NETWORK],
        )
        row = cur.fetchone()
        return int(row['generation']) if row and row['generation'] is not None else 0


# ── Geometry ──────────────────────────────────────────────────────────────────

def dist_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── Database connection ───────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def get_checkpoint(db) -> str:
    with db.cursor() as cur:
        cur.execute("SELECT value FROM ml_extraction_state WHERE key = %s", [CHECKPOINT_KEY])
        row = cur.fetchone()
        return row['value'] if row else '1970-01-01T00:00:00+00:00'


def set_checkpoint(db, ts: str):
    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO ml_extraction_state (key, value, updated_at)
               VALUES (%s, %s, NOW())
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()""",
            [CHECKPOINT_KEY, ts],
        )
    db.commit()


# ── Gold extraction ───────────────────────────────────────────────────────────

def _trim_terminal_hash(path_hashes: list[str], rx_node_id: str) -> list[str]:
    """Remove the observer's own hash that meshcore appends as the last entry."""
    if not path_hashes:
        return path_hashes
    last = path_hashes[-1].upper()
    if rx_node_id.upper().startswith(last):
        return path_hashes[:-1]
    return path_hashes


def extract_gold_paths(db):
    checkpoint = get_checkpoint(db)
    log.info('Gold extraction from checkpoint %s', checkpoint)

    with db.cursor() as cur:
        cur.execute(
            """SELECT packet_hash, network, rx_node_id,
                      path_hashes, path_hash_size_bytes, time as observed_at
                 FROM packets
                WHERE path_hash_size_bytes > 1
                  AND path_hashes IS NOT NULL
                  AND cardinality(path_hashes) > 1
                  AND time > %s
                ORDER BY time ASC
                LIMIT %s""",
            [checkpoint, GOLD_BATCH],
        )
        rows = cur.fetchall()

    if not rows:
        log.info('Gold extraction: no new packets')
        return

    # ── Collect all hashes to batch-resolve ──────────────────────────────────
    # Map: (network scope, hash_upper) → [node row, ...]
    hash_to_resolve: dict[str, set[str]] = defaultdict(set)
    scope_networks: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        scope = network_scope_key(row['network'])
        scope_networks[scope].update(network_scope_values(row['network']))
        hashes = [h.upper() for h in (row['path_hashes'] or [])]
        hashes = _trim_terminal_hash(hashes, row['rx_node_id'])
        hash_size = (row['path_hash_size_bytes'] or 1) * 2  # expected hex chars
        for h in hashes:
            if len(h) == hash_size:
                hash_to_resolve[scope].add(h)

    nodes_by_net_hash: dict[tuple[str, str], list[dict]] = {}
    for scope, hashes in hash_to_resolve.items():
        if not hashes:
            continue
        hash_list = sorted(hashes)
        # Use a single query with array containment to match prefixes efficiently
        conditions = ' OR '.join([f"upper(node_id) LIKE %s" for _ in hash_list])
        with db.cursor() as cur:
            cur.execute(
                f"""SELECT node_id, network, lat, lon, elevation_m, last_seen, iata
                      FROM nodes
                     WHERE network = ANY(%s)
                       AND lat IS NOT NULL AND lon IS NOT NULL
                       AND lat != 0 AND lon != 0
                       AND ({conditions})""",
                [list(scope_networks[scope])] + [h + '%' for h in hash_list],
            )
            node_rows = cur.fetchall()

        for node in node_rows:
            nid = node['node_id'].upper()
            for h in hash_list:
                if nid.startswith(h):
                    key = (scope, h)
                    if key not in nodes_by_net_hash:
                        nodes_by_net_hash[key] = []
                    nodes_by_net_hash[key].append(dict(node))
                    break

    # ── Group observations by (packet_hash, network) ─────────────────────────
    by_packet: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        by_packet[(row['packet_hash'], row['network'])].append(dict(row))

    inserted = 0
    latest_ts = checkpoint

    for (packet_hash, network), observations in by_packet.items():
        # All observers for this packet
        all_observer_ids = [o['rx_node_id'] for o in observations]

        for obs in observations:
            hashes = [h.upper() for h in (obs['path_hashes'] or [])]
            hashes = _trim_terminal_hash(hashes, obs['rx_node_id'])
            hash_size = (obs['path_hash_size_bytes'] or 1) * 2

            # Validate hash lengths
            valid_hashes = [h for h in hashes if len(h) == hash_size]
            if len(valid_hashes) < 2:
                continue

            # Resolve each hop — require unique match
            resolved: list[tuple[str, dict]] = []  # (hash, node)
            ok = True
            for h in valid_hashes:
                candidates = nodes_by_net_hash.get((network_scope_key(network), h), [])
                if len(candidates) != 1:
                    ok = False
                    break
                resolved.append((h, candidates[0]))

            if not ok or len(resolved) < 2:
                continue

            # Validate: no impossible adjacent hops
            for i in range(len(resolved) - 1):
                a = resolved[i][1]
                b = resolved[i + 1][1]
                d = dist_km(a['lat'], a['lon'], b['lat'], b['lon'])
                if d > MAX_HOP_KM:
                    ok = False
                    break

            if not ok:
                continue

            # Get receiver region (IATA) from first observer node
            rx_region = None
            with db.cursor() as cur:
                cur.execute("SELECT iata FROM nodes WHERE node_id = %s", [obs['rx_node_id']])
                node_row = cur.fetchone()
                if node_row:
                    rx_region = node_row['iata']

            # Insert gold hop rows
            for pos, (h, node) in enumerate(resolved):
                node_id = node['node_id']
                hash_2char = node_id.upper()[:2]
                hash_4char = node_id.upper()[:4]
                hash_6char = node_id.upper()[:6]
                try:
                    with db.cursor() as cur:
                        cur.execute(
                            """INSERT INTO ml_gold_paths
                                 (packet_hash, network, observed_at, hop_position,
                                  true_node_id, hash_2char, hash_4char, hash_6char,
                                  path_hash_size_bytes, observer_ids, rx_region)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT (packet_hash, hop_position, true_node_id) DO NOTHING""",
                            [packet_hash, network, obs['observed_at'], pos,
                             node_id, hash_2char, hash_4char, hash_6char,
                             obs['path_hash_size_bytes'], all_observer_ids, rx_region],
                        )
                        inserted += cur.rowcount
                except Exception as e:
                    db.rollback()
                    log.warning('Insert error for %s pos %d: %s', packet_hash, pos, e)
                    continue

            ts = obs['observed_at']
            if isinstance(ts, datetime):
                ts_str = ts.isoformat()
            else:
                ts_str = str(ts)
            if ts_str > latest_ts:
                latest_ts = ts_str

        db.commit()

    set_checkpoint(db, latest_ts)
    log.info('Gold extraction: inserted %d new hop rows from %d packets', inserted, len(by_packet))


# ── Feature building ──────────────────────────────────────────────────────────

def build_training_data(db):
    """
    Build (X, y, meta) arrays for training.

    For each gold hop, generates one row per candidate node sharing the
    same 1-byte (2-char) hash prefix.  The true node gets label=1; all
    others get label=0.
    """
    # Load all gold hops with path length context
    with db.cursor() as cur:
        cur.execute(
            """SELECT g.id, g.packet_hash, g.network, g.observed_at,
                      g.hop_position, g.true_node_id, g.hash_2char,
                      g.path_hash_size_bytes,
                      COUNT(*) OVER (PARTITION BY g.packet_hash, g.network) as path_length
                 FROM ml_gold_paths g
                ORDER BY g.network, g.hash_2char, g.observed_at"""
        )
        gold_rows = cur.fetchall()

    if len(gold_rows) < MIN_GOLD_ROWS:
        log.info('Only %d gold rows, skipping training', len(gold_rows))
        return None, None, None

    # Load all candidate nodes per combined network scope + 1-byte prefix.
    hash_pairs: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in gold_rows:
        scope = network_scope_key(row['network'])
        hash_pairs[(scope, row['hash_2char'])].update(network_scope_values(row['network']))

    candidates_map: dict[tuple[str, str], list[dict]] = {}
    for (scope, hash_2char), networks in hash_pairs.items():
        with db.cursor() as cur:
            cur.execute(
                """SELECT node_id, network AS node_network, elevation_m, last_seen
                     FROM nodes
                    WHERE network = ANY(%s)
                      AND upper(node_id) LIKE %s
                      AND lat IS NOT NULL AND lon IS NOT NULL""",
                [list(networks), hash_2char + '%'],
            )
            candidates_map[(scope, hash_2char)] = cur.fetchall()

    now = datetime.now(timezone.utc)
    X_rows = []
    y_rows = []
    # (packet_network, packet_hash, hop_position, hash_2char, candidate_node_id,
    #  true_node_id, gold_id, path_length, candidate_network)
    meta_rows = []

    for r in gold_rows:
        rid = r['id']
        network = r['network']
        hash_2char = r['hash_2char']
        true_node_id = r['true_node_id']
        candidates = candidates_map.get((network_scope_key(network), hash_2char), [])
        if not candidates:
            continue

        collision_count = len(candidates)

        for cand in candidates:
            cand_id = cand['node_id']
            label = 1 if cand_id == true_node_id else 0

            # Days since last seen
            ls = cand['last_seen']
            if ls and hasattr(ls, 'tzinfo'):
                if ls.tzinfo is None:
                    ls = ls.replace(tzinfo=timezone.utc)
                days_since = (now - ls).total_seconds() / 86400.0
            else:
                days_since = 999.0

            feat = [
                collision_count,
                float(cand['elevation_m'] or 0),
                min(days_since, 999.0),
                1 if days_since < 7 else 0,
                int(r['hop_position']),
                int(r['path_length'] or 1),
                1,  # simulated 1-byte path hash size
            ]
            X_rows.append(feat)
            y_rows.append(label)
            meta_rows.append((
                network,
                r['packet_hash'],
                int(r['hop_position']),
                hash_2char,
                cand_id,
                true_node_id,
                rid,
                int(r['path_length'] or 1),
                cand['node_network'],
            ))

    if not X_rows:
        return None, None, None

    X = np.array(X_rows, dtype=np.float32)
    y = np.array(y_rows, dtype=np.int32)
    # gold_ids: parallel array of ml_gold_paths.id, used for grouping in evaluation
    gold_ids = np.array([m[6] for m in meta_rows], dtype=np.int64)
    return X, y, meta_rows, gold_ids


FEATURE_NAMES = [
    'collision_count', 'elevation_m', 'days_since_seen', 'is_online_recent',
    'hop_position', 'path_length', 'simulated_path_hash_size_bytes',
]


# ── Training ──────────────────────────────────────────────────────────────────

def train_variant(
    X_train: np.ndarray, y_train: np.ndarray,
    X_val: np.ndarray, y_val: np.ndarray,
    params: dict,
) -> object | None:
    """Train one LightGBM variant with given hyperparams and return calibrated model."""
    if len(set(y_train)) < 2 or len(set(y_val)) < 2:
        return None

    rng = np.random.default_rng(RANDOM_SEED)
    train_idx = np.arange(len(y_train))
    rng.shuffle(train_idx)
    split = int(len(train_idx) * 0.8)
    split = max(1, min(len(train_idx) - 1, split))
    fit_idx = train_idx[:split]
    cal_idx = train_idx[split:]

    X_fit, y_fit = X_train[fit_idx], y_train[fit_idx]
    X_cal, y_cal = X_train[cal_idx], y_train[cal_idx]
    if len(set(y_fit.tolist())) < 2:
        X_fit, y_fit = X_train, y_train

    base = LGBMClassifier(
        objective='binary',
        n_jobs=2,
        is_unbalance=True,
        verbose=-1,
        **{k: v for k, v in params.items() if k != 'bagging_freq' or v > 0},
    )
    try:
        eval_set = [(X_cal, y_cal)] if len(set(y_cal.tolist())) >= 2 else None
        base.fit(X_fit, y_fit, eval_set=eval_set, callbacks=[])
    except Exception as e:
        log.warning('Variant training error: %s', e)
        return None

    if len(set(y_cal.tolist())) < 2:
        return base

    model = CalibratedClassifierCV(FrozenEstimator(base), method='isotonic')
    model.fit(X_cal, y_cal)
    return model


def train_final_variant(X: np.ndarray, y: np.ndarray, params: dict) -> object | None:
    """Train a final LightGBM model on all gold rows without calibration splits."""
    if len(set(y.tolist())) < 2:
        return None

    model = LGBMClassifier(
        objective='binary',
        n_jobs=2,
        is_unbalance=True,
        verbose=-1,
        **{k: v for k, v in params.items() if k != 'bagging_freq' or v > 0},
    )
    try:
        model.fit(X, y)
        return model
    except Exception as e:
        log.warning('Final variant training error: %s', e)
        return None


def collect_path_predictions(model, X: np.ndarray, y: np.ndarray, meta_rows: list) -> tuple[list[dict], dict]:
    """
    Run the model over candidate rows and aggregate predictions back into
    packet paths.

    A candidate row is still the model's unit of inference, but a packet path is
    the unit of evaluation.  A packet is only complete when every expected hop
    is present and the top-ranked candidate for every hop is the true node.
    """
    probs = model.predict_proba(X)[:, 1]

    hop_groups: dict[int, list[int]] = defaultdict(list)
    packets: dict[tuple[str, str], dict] = {}

    for i, meta in enumerate(meta_rows):
        net, packet_hash, hop_pos, h2, cid, true_id, gid, path_len, _cand_net = meta
        hop_groups[int(gid)].append(i)
        packet_key = (net, packet_hash)
        if packet_key not in packets:
            packets[packet_key] = {
                'network': net,
                'packet_hash': packet_hash,
                'expected_hops': int(path_len),
                'predicted_hops': {},
            }
        else:
            packets[packet_key]['expected_hops'] = max(
                int(packets[packet_key]['expected_hops']),
                int(path_len),
            )

    predictions: list[dict] = []

    for gid, idxs in hop_groups.items():
        if not idxs:
            continue
        idx_arr = np.array(idxs, dtype=np.int64)
        labels = y[idx_arr]
        if labels.sum() != 1:
            continue

        group_probs = probs[idx_arr]
        best_idx = int(idx_arr[int(np.argmax(group_probs))])
        top3_local = np.argsort(group_probs)[::-1][:3]
        top3_correct = bool(labels[top3_local].sum() > 0)

        net, packet_hash, hop_pos, h2, cid, true_id, _gid, path_len, _cand_net = meta_rows[best_idx]
        packet_key = (net, packet_hash)
        correct = bool(y[best_idx] == 1)

        packets[packet_key]['predicted_hops'][int(hop_pos)] = correct
        predictions.append({
            'network': net,
            'packet_hash': packet_hash,
            'packet_key': packet_key,
            'hop_position': int(hop_pos),
            'hash_2char': h2,
            'node_id': cid,
            'true_node_id': true_id,
            'gold_id': gid,
            'correct': correct,
            'top3_correct': top3_correct,
            'probability': float(probs[best_idx]),
        })

    for packet in packets.values():
        expected = max(1, int(packet['expected_hops']))
        predicted_hops: dict[int, bool] = packet['predicted_hops']
        correct_hops = sum(1 for pos in range(expected) if predicted_hops.get(pos, False))
        predicted_count = len(predicted_hops)
        packet['correct_hops'] = correct_hops
        packet['predicted_hops_count'] = predicted_count
        packet['complete'] = predicted_count >= expected and correct_hops == expected
        packet['completion'] = correct_hops / expected

    return predictions, packets


def evaluate_path_metrics(model, X: np.ndarray, y: np.ndarray, meta_rows: list) -> dict:
    return evaluate_path_details(model, X, y, meta_rows)[0]


def evaluate_path_details(model, X: np.ndarray, y: np.ndarray, meta_rows: list) -> tuple[dict, list[dict], dict]:
    predictions, packets = collect_path_predictions(model, X, y, meta_rows)
    hop_total = len(predictions)
    hop_correct = sum(1 for p in predictions if p['correct'])
    top3_correct = sum(1 for p in predictions if p['top3_correct'])

    packet_total = len(packets)
    complete_paths = sum(1 for p in packets.values() if p['complete'])
    total_expected_hops = sum(int(p['expected_hops']) for p in packets.values())
    total_correct_path_hops = sum(int(p['correct_hops']) for p in packets.values())

    metrics = {
        'hop_total': hop_total,
        'hop_correct': hop_correct,
        'hop_accuracy': hop_correct / hop_total if hop_total else 0.0,
        'hop_top3_accuracy': top3_correct / hop_total if hop_total else 0.0,
        'packet_total': packet_total,
        'complete_paths': complete_paths,
        'complete_path_accuracy': complete_paths / packet_total if packet_total else 0.0,
        'mean_path_completion': (
            total_correct_path_hops / total_expected_hops
            if total_expected_hops else 0.0
        ),
    }
    return metrics, predictions, packets


def persist_variant_evaluation(
    db,
    training_run_id: str,
    generation: int,
    variant_rank: int,
    params: dict,
    all_metrics: dict,
    val_metrics: dict,
    packets: dict,
):
    packet_rows = [
        (
            training_run_id,
            GLOBAL_NETWORK,
            generation,
            variant_rank,
            packet['network'],
            packet['packet_hash'],
            int(packet['expected_hops']),
            int(packet['predicted_hops_count']),
            int(packet['correct_hops']),
            bool(packet['complete']),
            float(packet['completion']),
        )
        for packet in packets.values()
    ]

    with db.cursor() as cur:
        cur.execute(
            """INSERT INTO ml_model_variant_runs
                 (training_run_id, model_network, generation, variant_rank,
                  population_size, hyperparams, evaluated_packets,
                  evaluated_hops, hop_accuracy, hop_top3_accuracy,
                  complete_path_accuracy, mean_path_completion,
                  val_evaluated_packets, val_evaluated_hops,
                  val_hop_accuracy, val_hop_top3_accuracy,
                  val_complete_path_accuracy, val_mean_path_completion,
                  created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (training_run_id, variant_rank) DO UPDATE SET
                  hyperparams = EXCLUDED.hyperparams,
                  evaluated_packets = EXCLUDED.evaluated_packets,
                  evaluated_hops = EXCLUDED.evaluated_hops,
                  hop_accuracy = EXCLUDED.hop_accuracy,
                  hop_top3_accuracy = EXCLUDED.hop_top3_accuracy,
                  complete_path_accuracy = EXCLUDED.complete_path_accuracy,
                  mean_path_completion = EXCLUDED.mean_path_completion,
                  val_evaluated_packets = EXCLUDED.val_evaluated_packets,
                  val_evaluated_hops = EXCLUDED.val_evaluated_hops,
                  val_hop_accuracy = EXCLUDED.val_hop_accuracy,
                  val_hop_top3_accuracy = EXCLUDED.val_hop_top3_accuracy,
                  val_complete_path_accuracy = EXCLUDED.val_complete_path_accuracy,
                  val_mean_path_completion = EXCLUDED.val_mean_path_completion""",
            [
                training_run_id,
                GLOBAL_NETWORK,
                generation,
                variant_rank,
                POPULATION_SIZE,
                json.dumps(params),
                all_metrics['packet_total'],
                all_metrics['hop_total'],
                all_metrics['hop_accuracy'],
                all_metrics['hop_top3_accuracy'],
                all_metrics['complete_path_accuracy'],
                all_metrics['mean_path_completion'],
                val_metrics['packet_total'],
                val_metrics['hop_total'],
                val_metrics['hop_accuracy'],
                val_metrics['hop_top3_accuracy'],
                val_metrics['complete_path_accuracy'],
                val_metrics['mean_path_completion'],
            ],
        )

        if packet_rows:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO ml_model_variant_packet_results
                     (training_run_id, model_network, generation, variant_rank,
                      packet_network, packet_hash, expected_hops,
                      predicted_hops, correct_hops, complete_path,
                      path_completion)
                   VALUES %s
                   ON CONFLICT (training_run_id, variant_rank, packet_network, packet_hash)
                   DO UPDATE SET
                      expected_hops = EXCLUDED.expected_hops,
                      predicted_hops = EXCLUDED.predicted_hops,
                      correct_hops = EXCLUDED.correct_hops,
                      complete_path = EXCLUDED.complete_path,
                      path_completion = EXCLUDED.path_completion""",
                packet_rows,
                page_size=1000,
            )
    db.commit()


def split_by_packet(meta_rows: list, train_fraction: float = 0.8) -> tuple[np.ndarray, np.ndarray]:
    """Split candidate rows by packet so a full path is entirely train or val."""
    packet_keys = sorted({(m[0], m[1]) for m in meta_rows})
    if len(packet_keys) < 2:
        empty = np.zeros(len(meta_rows), dtype=bool)
        return ~empty, empty

    rng = random.Random(RANDOM_SEED)
    rng.shuffle(packet_keys)
    split = int(len(packet_keys) * train_fraction)
    split = max(1, min(len(packet_keys) - 1, split))

    train_packets = set(packet_keys[:split])
    train_mask = np.array([(m[0], m[1]) in train_packets for m in meta_rows], dtype=bool)
    val_mask = ~train_mask
    return train_mask, val_mask


# ── Promotion ─────────────────────────────────────────────────────────────────

def get_current_best_accuracy(db) -> float:
    with db.cursor() as cur:
        cur.execute(
            """SELECT complete_path_accuracy AS champion_score
                 FROM ml_model_versions
                WHERE network = %s AND is_active = TRUE ORDER BY promoted_at DESC LIMIT 1""",
            [GLOBAL_NETWORK],
        )
        row = cur.fetchone()
        return float(row['champion_score']) if row and row['champion_score'] is not None else 0.0


def evaluate_current_champion(db, X: np.ndarray, y: np.ndarray, meta_rows: list) -> dict | None:
    """Replay the active champion on the current gold corpus."""
    with db.cursor() as cur:
        cur.execute(
            """SELECT version, generation, variant_rank, model_artifact
                 FROM ml_model_versions
                WHERE network = %s AND is_active = TRUE
                ORDER BY promoted_at DESC LIMIT 1""",
            [GLOBAL_NETWORK],
        )
        row = cur.fetchone()

    if not row or not row['model_artifact']:
        return None

    try:
        model = joblib.load(io.BytesIO(bytes(row['model_artifact'])))
        metrics = evaluate_path_metrics(model, X, y, meta_rows)
        metrics['version'] = row['version']
        metrics['generation'] = int(row['generation'])
        metrics['variant_rank'] = int(row['variant_rank'])
        return metrics
    except Exception as e:
        log.warning('Current champion replay failed: %s', e, exc_info=True)
        return None


def promotion_score(metrics: dict) -> tuple[float, float, float]:
    return (
        float(metrics['complete_path_accuracy']),
        float(metrics['hop_accuracy']),
        float(metrics['mean_path_completion']),
    )


def score_beats(candidate: tuple[float, float, float],
                incumbent: tuple[float, float, float],
                min_delta: float = 0.0) -> bool:
    if candidate[0] > incumbent[0] + min_delta:
        return True
    if candidate[0] + min_delta < incumbent[0]:
        return False
    return candidate[1:] > incumbent[1:]


def promote_model(db, model, val_metrics: dict, all_metrics: dict, gold_count: int,
                  X: np.ndarray, y: np.ndarray, meta_rows: list,
                  gold_ids: np.ndarray,
                  hyperparams: dict | None = None,
                  generation: int = 1, variant_rank: int = 1):
    version = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S') + '_global'

    # Serialize model
    buf = io.BytesIO()
    joblib.dump(model, buf)
    artifact = buf.getvalue()

    with db.cursor() as cur:
        cur.execute("UPDATE ml_model_versions SET is_active = FALSE WHERE is_active = TRUE")
        cur.execute(
            """INSERT INTO ml_model_versions
                 (version, network, trained_at, gold_paths_used, top1_accuracy,
                  top3_accuracy, is_active, promoted_at, model_artifact,
                  hyperparams, generation, variant_rank, population_size,
                  evaluated_packets, evaluated_hops, complete_path_accuracy,
                  mean_path_completion)
               VALUES (%s, %s, NOW(), %s, %s, %s, TRUE, NOW(), %s, %s, %s, %s, %s,
                       %s, %s, %s, %s)""",
            [
                version,
                GLOBAL_NETWORK,
                gold_count,
                all_metrics['hop_accuracy'],
                all_metrics['hop_top3_accuracy'],
                psycopg2.Binary(artifact),
                json.dumps(hyperparams or {}),
                generation,
                variant_rank,
                POPULATION_SIZE,
                all_metrics['packet_total'],
                all_metrics['hop_total'],
                all_metrics['complete_path_accuracy'],
                all_metrics['mean_path_completion'],
            ],
        )

    probs = model.predict_proba(X)[:, 1]
    predictions, packets = collect_path_predictions(model, X, y, meta_rows)
    selected_predictions = {
        (pred['gold_id'], pred['node_id']): pred
        for pred in predictions
    }
    score_rows: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {
            'observed': 0,
            'correct': 0,
            'prob_sum': 0.0,
            'selected': 0,
            'selected_correct': 0,
            'packets': set(),
            'complete_packets': set(),
        }
    )
    for i, meta in enumerate(meta_rows):
        packet_net, packet_hash, _hop_pos, h2, cid, _true_id, gid, _path_len, cand_net = meta
        score_net = cand_net or packet_net
        packet_key = (packet_net, packet_hash)
        key = (score_net, h2, cid)
        row = score_rows[key]
        row['observed'] += 1
        row['prob_sum'] += float(probs[i])
        if y[i] == 1:
            row['correct'] += 1
        row['packets'].add(packet_key)

        selected = selected_predictions.get((int(gid), cid))
        if selected:
            row['selected'] += 1
            if selected['correct']:
                row['selected_correct'] += 1
            if packets[packet_key]['complete']:
                row['complete_packets'].add(packet_key)

    scores_written = 0
    with db.cursor() as cur:
        cur.execute("DELETE FROM ml_path_prefix_scores WHERE model_version != %s", [version])
        for (net, h2, node_id), counts in score_rows.items():
            obs = int(counts['observed'])
            correct = int(counts['correct'])
            avg_model_prob = float(counts['prob_sum']) / obs if obs else 0.0
            # Persist a conservative score for every candidate mapping the
            # model considered.  The score cannot exceed the empirical rate at
            # which this node was actually correct for the 1-byte prefix.
            empirical_correct_rate = (correct + 1) / (obs + 2)
            score = min(avg_model_prob, empirical_correct_rate)
            if obs < MIN_OBSERVATION_COUNT or score < CONFIDENCE_THRESHOLD:
                continue
            cur.execute(
                """INSERT INTO ml_path_prefix_scores
                     (network, hash_2char, node_id, score, observation_count,
                      correct_count, packet_count, complete_path_count,
                      model_version, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (network, hash_2char, node_id) DO UPDATE
                     SET score = EXCLUDED.score,
                         observation_count = EXCLUDED.observation_count,
                         correct_count = EXCLUDED.correct_count,
                         packet_count = EXCLUDED.packet_count,
                         complete_path_count = EXCLUDED.complete_path_count,
                         model_version = EXCLUDED.model_version,
                         updated_at = NOW()""",
                [
                    net,
                    h2,
                    node_id,
                    score,
                    obs,
                    correct,
                    len(counts['packets']),
                    len(counts['complete_packets']),
                    version,
                ],
            )
            scores_written += 1

    db.commit()
    log.info(
        'Champion promoted network=%s generation=%d variant=%d/%d val_hop=%.3f val_top3=%.3f val_full_path=%.3f val_path_completion=%.3f all_hop=%.3f all_full_path=%.3f scores_written=%d confidence_threshold=%.2f params=%s',
        GLOBAL_NETWORK, generation, variant_rank, POPULATION_SIZE,
        val_metrics['hop_accuracy'], val_metrics['hop_top3_accuracy'],
        val_metrics['complete_path_accuracy'], val_metrics['mean_path_completion'],
        all_metrics['hop_accuracy'], all_metrics['complete_path_accuracy'],
        scores_written, CONFIDENCE_THRESHOLD, json.dumps(hyperparams or {}),
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_training_cycle(db):
    log.info('Starting training cycle')
    result = build_training_data(db)
    if result[0] is None:
        log.info('Insufficient training data, skipping')
        return
    X, y, meta_rows, gold_ids = result

    if len(y) < MIN_GOLD_ROWS:
        log.info('Only %d rows, skipping', len(y))
        return

    # Train one global model across all networks combined.
    # Split by packet so whole paths never straddle train/val.
    n = len(y)
    train_mask, val_mask = split_by_packet(meta_rows)
    X_train, X_val = X[train_mask], X[val_mask]
    y_train, y_val = y[train_mask], y[val_mask]
    gids_train, gids_val = gold_ids[train_mask], gold_ids[val_mask]
    meta_train = [m for m, keep in zip(meta_rows, train_mask) if keep]
    meta_val = [m for m, keep in zip(meta_rows, val_mask) if keep]

    if len(y_train) == 0 or len(y_val) == 0:
        log.warning('Train/validation split produced an empty side, skipping')
        return

    if len(set(y_train.tolist())) < 2 or len(set(y_val.tolist())) < 2:
        log.warning('Class diversity too low globally, skipping')
        return

    groups_val: dict[int, list[int]] = defaultdict(list)
    for i, gid in enumerate(gids_val):
        groups_val[int(gid)].append(i)
    ambiguous_count = sum(1 for idxs in groups_val.values() if len(idxs) > 1)
    train_packets = {(m[0], m[1]) for m in meta_train}
    val_packets = {(m[0], m[1]) for m in meta_val}
    all_packets = {(m[0], m[1]) for m in meta_rows}

    generation = get_current_generation(db) + 1
    training_run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S') + f'_gen{generation}'
    champion_params = get_champion_params(db)
    population = create_population(champion_params, generation)
    stored_current_best = get_current_best_accuracy(db)
    champion_current_metrics = evaluate_current_champion(db, X, y, meta_rows)
    champion_val_metrics = evaluate_current_champion(db, X_val, y_val, meta_val)
    current_all_best = (
        champion_current_metrics['complete_path_accuracy']
        if champion_current_metrics
        else stored_current_best
    )
    current_val_best = (
        champion_val_metrics['complete_path_accuracy']
        if champion_val_metrics
        else 0.0
    )

    networks_repr = ', '.join(sorted({m[0] for m in meta_rows}))
    log.info(
        'Generation %d network=%s population=%d rows=%d train_rows=%d val_rows=%d packets=%d train_packets=%d val_packets=%d gold_hops=%d train_hops=%d val_hops=%d positives=%d ambiguous_hops=%d champion_val_full_path=%.3f champion_all_full_path=%.3f stored_best_full_path=%.3f networks=[%s]',
        generation, GLOBAL_NETWORK, POPULATION_SIZE, n, len(y_train), len(y_val),
        len(all_packets), len(train_packets), len(val_packets),
        len(set(gold_ids.tolist())), len(set(gids_train.tolist())), len(set(gids_val.tolist())),
        int(sum(y)), ambiguous_count, current_val_best, current_all_best,
        stored_current_best, networks_repr,
    )
    if champion_current_metrics:
        log.info(
            'Active champion replay on current corpus version=%s generation=%d variant=%d packets=%d hop=%.3f full_path=%.3f path_completion=%.3f',
            champion_current_metrics['version'],
            champion_current_metrics['generation'],
            champion_current_metrics['variant_rank'],
            champion_current_metrics['packet_total'],
            champion_current_metrics['hop_accuracy'],
            champion_current_metrics['complete_path_accuracy'],
            champion_current_metrics['mean_path_completion'],
        )
    if champion_val_metrics:
        log.info(
            'Active champion replay on current validation split version=%s generation=%d variant=%d packets=%d hop=%.3f full_path=%.3f path_completion=%.3f',
            champion_val_metrics['version'],
            champion_val_metrics['generation'],
            champion_val_metrics['variant_rank'],
            champion_val_metrics['packet_total'],
            champion_val_metrics['hop_accuracy'],
            champion_val_metrics['complete_path_accuracy'],
            champion_val_metrics['mean_path_completion'],
        )

    best_model = None
    best_val_metrics: dict | None = None
    best_all_metrics: dict | None = None
    best_params: dict = champion_params
    best_rank = 0
    best_score = (-1.0, -1.0, -1.0, -1.0)

    for rank, params in enumerate(population, start=1):
        model = train_variant(X_train, y_train, X_val, y_val, params)
        if model is None:
            continue

        val_metrics = evaluate_path_metrics(model, X_val, y_val, meta_val)
        all_metrics, _all_predictions, all_packets_detail = evaluate_path_details(model, X, y, meta_rows)
        persist_variant_evaluation(
            db, training_run_id, generation, rank, params,
            all_metrics, val_metrics, all_packets_detail,
        )

        log.info(
            'Generation %d variant %d/%d network=%s val_hop=%d/%d %.3f val_top3=%.3f val_full_path=%d/%d %.3f val_path_completion=%.3f all_hop=%d/%d %.3f all_full_path=%d/%d %.3f params=%s',
            generation, rank, POPULATION_SIZE, GLOBAL_NETWORK,
            val_metrics['hop_correct'], val_metrics['hop_total'], val_metrics['hop_accuracy'],
            val_metrics['hop_top3_accuracy'],
            val_metrics['complete_paths'], val_metrics['packet_total'],
            val_metrics['complete_path_accuracy'], val_metrics['mean_path_completion'],
            all_metrics['hop_correct'], all_metrics['hop_total'], all_metrics['hop_accuracy'],
            all_metrics['complete_paths'], all_metrics['packet_total'],
            all_metrics['complete_path_accuracy'], json.dumps(params),
        )

        selection_score = (
            val_metrics['complete_path_accuracy'],
            val_metrics['hop_accuracy'],
            val_metrics['mean_path_completion'],
            all_metrics['complete_path_accuracy'],
        )
        if selection_score > best_score:
            best_score = selection_score
            best_model = model
            best_val_metrics = val_metrics
            best_all_metrics = all_metrics
            best_params = params
            best_rank = rank

    if best_model is None or best_val_metrics is None or best_all_metrics is None:
        log.warning('All variants failed for global model')
        return

    log.info(
        'Generation %d winner network=%s variant=%d/%d val_hop=%.3f val_full_path=%.3f val_path_completion=%.3f all_hop=%.3f all_full_path=%.3f (champion val full_path=%.3f all full_path=%.3f)',
        generation, GLOBAL_NETWORK, best_rank, POPULATION_SIZE,
        best_val_metrics['hop_accuracy'], best_val_metrics['complete_path_accuracy'],
        best_val_metrics['mean_path_completion'], best_all_metrics['hop_accuracy'],
        best_all_metrics['complete_path_accuracy'], current_val_best, current_all_best,
    )

    final_model = train_final_variant(X, y, best_params)
    if final_model is None:
        log.warning('Final all-gold training failed for generation=%d variant=%d', generation, best_rank)
        final_model = best_model

    final_all_metrics = evaluate_path_metrics(final_model, X, y, meta_rows)
    if (
        final_all_metrics['complete_path_accuracy'],
        final_all_metrics['hop_accuracy'],
        final_all_metrics['mean_path_completion'],
    ) < (
        best_all_metrics['complete_path_accuracy'],
        best_all_metrics['hop_accuracy'],
        best_all_metrics['mean_path_completion'],
    ):
        log.warning(
            'Final all-gold model underperformed selected variant for generation=%d variant=%d (final full=%.3f hop=%.3f vs selected full=%.3f hop=%.3f); promoting selected variant model',
            generation, best_rank,
            final_all_metrics['complete_path_accuracy'], final_all_metrics['hop_accuracy'],
            best_all_metrics['complete_path_accuracy'], best_all_metrics['hop_accuracy'],
        )
        final_model = best_model
        final_all_metrics = best_all_metrics

    log.info(
        'Generation %d final all-gold model network=%s variant=%d/%d gold_replay_hop=%.3f gold_replay_full_path=%.3f gold_replay_path_completion=%.3f heldout_full_path_guardrail=%.3f',
        generation, GLOBAL_NETWORK, best_rank, POPULATION_SIZE,
        final_all_metrics['hop_accuracy'], final_all_metrics['complete_path_accuracy'],
        final_all_metrics['mean_path_completion'], best_val_metrics['complete_path_accuracy'],
    )

    candidate_guardrail_score = promotion_score(best_val_metrics)
    champion_guardrail_score = (
        promotion_score(champion_val_metrics)
        if champion_val_metrics
        else (0.0, 0.0, 0.0)
    )

    if champion_val_metrics is None or score_beats(
        candidate_guardrail_score,
        champion_guardrail_score,
        PROMOTION_MIN_DELTA,
    ):
        promote_model(
            db, final_model, best_val_metrics, final_all_metrics, int(sum(y)),
            X, y, meta_rows, gold_ids,
            hyperparams=best_params, generation=generation, variant_rank=best_rank,
        )
    else:
        log.info(
            'Generation %d network=%s no held-out improvement (candidate val_full=%.3f val_hop=%.3f vs champion val_full=%.3f val_hop=%.3f; candidate all_full=%.3f champion all_full=%.3f), discarding',
            generation, GLOBAL_NETWORK,
            best_val_metrics['complete_path_accuracy'], best_val_metrics['hop_accuracy'],
            champion_guardrail_score[0], champion_guardrail_score[1],
            final_all_metrics['complete_path_accuracy'], current_all_best,
        )


def main():
    log.info('ML path learner starting')
    db = None
    last_train = 0.0

    while True:
        try:
            if db is None or db.closed:
                db = get_db()
                log.info('Connected to database')

            extract_gold_paths(db)

            now = time.time()
            if now - last_train >= TRAIN_INTERVAL_SECS:
                run_training_cycle(db)
                last_train = now

        except KeyboardInterrupt:
            log.info('Shutting down')
            break
        except Exception as e:
            log.error('Error in main loop: %s', e, exc_info=True)
            try:
                if db and not db.closed:
                    db.rollback()
            except Exception:
                pass
            try:
                if db and not db.closed:
                    db.close()
            except Exception:
                pass
            db = None
            time.sleep(30)
            continue

        time.sleep(GOLD_INTERVAL_SECS)


if __name__ == '__main__':
    main()

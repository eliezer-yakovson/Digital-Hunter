import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Target bank — known entities with initial positions and metadata
# ---------------------------------------------------------------------------
TARGET_BANK: list[dict[str, Any]] = [
    {"entity_id": "TGT-001", "name": "Convoy Alpha",      "type": "mobile_vehicle", "lat": 31.52, "lon": 34.45, "priority_level": 1, "status": "active"},
    {"entity_id": "TGT-002", "name": "Depot Bravo",       "type": "infrastructure", "lat": 31.78, "lon": 34.63, "priority_level": 2, "status": "active"},
    {"entity_id": "TGT-003", "name": "Squad Charlie",     "type": "human_squad",    "lat": 32.05, "lon": 34.78, "priority_level": 1, "status": "active"},
    {"entity_id": "TGT-004", "name": "Launcher Delta",    "type": "launcher",       "lat": 31.90, "lon": 35.20, "priority_level": 1, "status": "active"},
    {"entity_id": "TGT-005", "name": "Transport Echo",    "type": "mobile_vehicle", "lat": 32.30, "lon": 35.50, "priority_level": 3, "status": "active"},
    {"entity_id": "TGT-006", "name": "Bunker Foxtrot",    "type": "infrastructure", "lat": 31.65, "lon": 34.35, "priority_level": 2, "status": "active"},
    {"entity_id": "TGT-007", "name": "Patrol Golf",       "type": "human_squad",    "lat": 32.45, "lon": 35.10, "priority_level": 4, "status": "active"},
    {"entity_id": "TGT-008", "name": "Launcher Hotel",    "type": "launcher",       "lat": 31.40, "lon": 34.90, "priority_level": 1, "status": "active"},
    {"entity_id": "TGT-009", "name": "Rover India",       "type": "mobile_vehicle", "lat": 32.10, "lon": 35.80, "priority_level": 5, "status": "active"},
    {"entity_id": "TGT-010", "name": "Outpost Juliet",    "type": "infrastructure", "lat": 31.85, "lon": 34.55, "priority_level": 3, "status": "active"},
    {"entity_id": "TGT-011", "name": "Squad Kilo",        "type": "human_squad",    "lat": 32.60, "lon": 35.30, "priority_level": 2, "status": "active"},
    {"entity_id": "TGT-012", "name": "Launcher Lima",     "type": "launcher",       "lat": 31.55, "lon": 35.65, "priority_level": 1, "status": "active"},
    {"entity_id": "TGT-013", "name": "Jeep Mike",         "type": "mobile_vehicle", "lat": 32.25, "lon": 34.80, "priority_level": 4, "status": "active"},
    {"entity_id": "TGT-014", "name": "Compound November", "type": "infrastructure", "lat": 31.70, "lon": 35.40, "priority_level": 3, "status": "active"},
    {"entity_id": "TGT-015", "name": "Squad Oscar",       "type": "human_squad",    "lat": 32.00, "lon": 35.00, "priority_level": 2, "status": "active"},
]

SIGNAL_TYPES: list[str] = ["SIGINT", "VISINT", "HUMINT"]
WEAPON_TYPES: list[str] = [
    "AGM-114 Hellfire",
    "GBU-39 SDB",
    "Delilah Missile",
    "SPICE-250",
    "Popeye AGM",
    "Griffin LGM",
]
DAMAGE_RESULTS: list[str] = ["destroyed", "damaged", "no_damage"]

# ---------------------------------------------------------------------------
# Simulator state — tracks what has happened so far
# ---------------------------------------------------------------------------
entity_last_position: dict[str, dict[str, float]] = {}
reported_entity_ids: set[str] = set()
produced_attacks: dict[str, str] = {}  # attack_id -> entity_id
destroyed_entities: set[str] = set()


def _connect_producer(bootstrap_servers: str, max_retries: int = 10) -> KafkaProducer:
    """Try connecting to Kafka with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode("utf-8"),
            )
            logger.info("Connected to Kafka on attempt %d", attempt)
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready (attempt %d/%d), retrying in 3s...", attempt, max_retries)
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after %d attempts" % max_retries)


# ---------------------------------------------------------------------------
# Message generators
# ---------------------------------------------------------------------------

def _jitter(base: float, range_km: float = 0.02) -> float:
    """Add small random offset to a coordinate to simulate movement."""
    return base + random.uniform(-range_km, range_km)


def generate_intel_message() -> dict[str, Any]:
    """Generate an intelligence signal message."""
    target = random.choice(TARGET_BANK)
    entity_id: str = target["entity_id"]

    last_pos = entity_last_position.get(entity_id, {"lat": target["lat"], "lon": target["lon"]})
    new_lat = _jitter(last_pos["lat"])
    new_lon = _jitter(last_pos["lon"])

    entity_last_position[entity_id] = {"lat": new_lat, "lon": new_lon}
    reported_entity_ids.add(entity_id)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "signal_id": str(uuid.uuid4()),
        "entity_id": entity_id,
        "reported_lat": round(new_lat, 6),
        "reported_lon": round(new_lon, 6),
        "signal_type": random.choice(SIGNAL_TYPES),
        "priority_level": target["priority_level"],
    }


def generate_attack_message() -> dict[str, Any]:
    """Generate an air force attack message."""
    target = random.choice(TARGET_BANK)
    attack_id = str(uuid.uuid4())
    produced_attacks[attack_id] = target["entity_id"]

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attack_id": attack_id,
        "entity_id": target["entity_id"],
        "weapon_type": random.choice(WEAPON_TYPES),
    }


def generate_damage_message() -> dict[str, Any]:
    """Generate a damage assessment message for a previous attack."""
    if not produced_attacks:
        return generate_attack_message()  # fallback: produce an attack instead

    attack_id = random.choice(list(produced_attacks.keys()))
    entity_id = produced_attacks[attack_id]
    result = random.choice(DAMAGE_RESULTS)

    if result == "destroyed":
        destroyed_entities.add(entity_id)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attack_id": attack_id,
        "entity_id": entity_id,
        "result": result,
    }


# ---------------------------------------------------------------------------
# Error injection
# ---------------------------------------------------------------------------

def inject_broken_json() -> bytes:
    """Return malformed JSON bytes."""
    variants = [
        b'{"timestamp": "2025-01-01T00:00:00Z", "signal_id": "abc", "entity_id"',
        b'not json at all!!!',
        b'{"half": "message"',
        b'{{{bad json}}}',
        b'',
    ]
    chosen = random.choice(variants)
    logger.warning("INJECTING broken JSON: %s", chosen[:60])
    return chosen


def inject_missing_fields(topic: str) -> dict[str, Any]:
    """Return a message with critical fields missing."""
    timestamp = datetime.now(timezone.utc).isoformat()

    if topic == "intel":
        # Missing entity_id or signal_id
        msg = random.choice([
            {"timestamp": timestamp, "signal_id": str(uuid.uuid4()), "reported_lat": 31.5, "reported_lon": 34.5, "signal_type": "SIGINT", "priority_level": 2},
            {"timestamp": timestamp, "entity_id": "TGT-001", "reported_lat": 31.5, "reported_lon": 34.5, "signal_type": "SIGINT", "priority_level": 2},
            {"timestamp": timestamp, "signal_id": str(uuid.uuid4()), "entity_id": "TGT-001", "signal_type": "SIGINT", "priority_level": 2},
        ])
    elif topic == "attack":
        msg = random.choice([
            {"timestamp": timestamp, "entity_id": "TGT-001", "weapon_type": "GBU-39 SDB"},
            {"timestamp": timestamp, "attack_id": str(uuid.uuid4()), "weapon_type": "Delilah Missile"},
        ])
    else:
        msg = random.choice([
            {"timestamp": timestamp, "entity_id": "TGT-001", "result": "destroyed"},
            {"timestamp": timestamp, "attack_id": str(uuid.uuid4()), "result": "damaged"},
        ])

    logger.warning("INJECTING missing-fields message on topic '%s': %s", topic, msg)
    return msg


def inject_attack_unknown_entity() -> dict[str, Any]:
    """Attack message referencing an entity that was never reported in intel."""
    unknown_entity_id = f"TGT-UNKNOWN-{random.randint(100, 999)}"
    attack_id = str(uuid.uuid4())
    produced_attacks[attack_id] = unknown_entity_id

    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attack_id": attack_id,
        "entity_id": unknown_entity_id,
        "weapon_type": random.choice(WEAPON_TYPES),
    }
    logger.warning("INJECTING attack on unknown entity: %s", unknown_entity_id)
    return msg


def inject_damage_unknown_attack() -> dict[str, Any]:
    """Damage assessment for an attack_id that was never produced."""
    fake_attack_id = f"ATTACK-FAKE-{random.randint(100, 999)}"
    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "attack_id": fake_attack_id,
        "entity_id": random.choice(TARGET_BANK)["entity_id"],
        "result": random.choice(DAMAGE_RESULTS),
    }
    logger.warning("INJECTING damage for non-existent attack: %s", fake_attack_id)
    return msg


def inject_intel_destroyed_entity() -> dict[str, Any] | None:
    """Intel signal for an entity that has already been destroyed."""
    if not destroyed_entities:
        return None

    entity_id = random.choice(list(destroyed_entities))
    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "signal_id": str(uuid.uuid4()),
        "entity_id": entity_id,
        "reported_lat": round(random.uniform(31.0, 33.0), 6),
        "reported_lon": round(random.uniform(34.0, 36.0), 6),
        "signal_type": random.choice(SIGNAL_TYPES),
        "priority_level": random.randint(1, 5),
    }
    logger.warning("INJECTING intel for destroyed entity: %s", entity_id)
    return msg


def inject_intel_unknown_near_priority() -> dict[str, Any]:
    """Intel signal for an unknown entity near a priority-1 target (simulates a meeting)."""
    priority_targets = [t for t in TARGET_BANK if t["priority_level"] == 1]
    target = random.choice(priority_targets)

    # Place the unknown entity within ~0.005 degrees (~0.5 km) of the target
    last_pos = entity_last_position.get(target["entity_id"],
                                        {"lat": target["lat"], "lon": target["lon"]})
    unknown_entity_id = f"TGT-UNKNOWN-{random.randint(100, 999)}"

    msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "signal_id": str(uuid.uuid4()),
        "entity_id": unknown_entity_id,
        "reported_lat": round(last_pos["lat"] + random.uniform(-0.005, 0.005), 6),
        "reported_lon": round(last_pos["lon"] + random.uniform(-0.005, 0.005), 6),
        "signal_type": random.choice(SIGNAL_TYPES),
        "priority_level": random.randint(1, 5),
    }
    logger.warning("INJECTING intel for unknown entity %s near priority target %s",
                   unknown_entity_id, target["entity_id"])
    return msg


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

ERROR_RATE = 0.10  # 10% of messages are errors
LOGICAL_ERROR_RATE = 0.15  # 15% of messages are logical errors

TOPIC_WEIGHTS = {
    "intel": 0.60,
    "attack": 0.25,
    "damage": 0.15,
}


def _pick_topic() -> str:
    roll = random.random()
    if roll < TOPIC_WEIGHTS["intel"]:
        return "intel"
    elif roll < TOPIC_WEIGHTS["intel"] + TOPIC_WEIGHTS["attack"]:
        return "attack"
    return "damage"


def run_simulator(bootstrap_servers: str = "localhost:9092", delay: float = 1.0) -> None:
    """Run the simulator in a continuous loop."""
    producer = _connect_producer(bootstrap_servers)
    message_count = 0

    logger.info("Simulator started — producing messages every %.1fs", delay)
    logger.info("Target bank has %d entities", len(TARGET_BANK))
    logger.info("Error rate: %.0f%% structural, %.0f%% logical", ERROR_RATE * 100, LOGICAL_ERROR_RATE * 100)

    try:
        while True:
            topic = _pick_topic()
            roll = random.random()

            # --- Structural error: broken JSON ---
            if roll < ERROR_RATE:
                raw_bytes = inject_broken_json()
                producer.send(topic, value=raw_bytes)
                message_count += 1
                time.sleep(delay)
                continue

            # --- Logical errors ---
            if roll < ERROR_RATE + LOGICAL_ERROR_RATE:
                error_msg = _generate_logical_error(topic)
                if error_msg is not None:
                    producer.send(topic, value=error_msg)
                    message_count += 1
                    time.sleep(delay)
                    continue

            # --- Normal message ---
            if topic == "intel":
                msg = generate_intel_message()
            elif topic == "attack":
                msg = generate_attack_message()
            else:
                msg = generate_damage_message()

            producer.send(topic, value=msg)
            logger.info("Sent to '%s': %s", topic, _summarize(msg))
            message_count += 1
            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Shutting down. Total messages produced: %d", message_count)
    finally:
        producer.flush()
        producer.close()


def _generate_logical_error(topic: str) -> dict[str, Any] | None:
    """Pick a logical error appropriate for the topic."""
    if topic == "intel":
        options = [
            lambda: inject_missing_fields("intel"),
            inject_intel_destroyed_entity,
            inject_intel_unknown_near_priority,
        ]
    elif topic == "attack":
        options = [
            inject_attack_unknown_entity,
            lambda: inject_missing_fields("attack"),
        ]
    else:
        options = [
            inject_damage_unknown_attack,
            lambda: inject_missing_fields("damage"),
        ]
    return random.choice(options)()


def _summarize(msg: dict[str, Any]) -> str:
    """Short summary of a message for logging."""
    entity = msg.get("entity_id", "?")
    if "signal_id" in msg:
        return f"intel entity={entity} type={msg.get('signal_type')}"
    if "attack_id" in msg and "result" not in msg:
        return f"attack entity={entity} weapon={msg.get('weapon_type')}"
    if "result" in msg:
        return f"damage entity={entity} result={msg.get('result')}"
    return str(msg)[:80]


if __name__ == "__main__":
    import os
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    run_simulator(bootstrap_servers=bootstrap)

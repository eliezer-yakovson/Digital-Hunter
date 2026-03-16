import os
import json
from confluent_kafka import Consumer, Producer
from pymongo import MongoClient
from pydantic import BaseModel, ValidationError

from haversine import haversine_km
from logger import log_event


consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'group.id': 'intel_processor',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['intel'])


producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
})


client = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017/'))
db = client['digital_hunter']


class IntelSignal(BaseModel):
    signal_id: str
    entity_id: str
    reported_lat: float
    reported_lon: float
    timestamp: str
    signal_type: str
    priority_level: int = 99


def validate_message(raw_data, db):
    try:
        signal = IntelSignal(**raw_data)
    except ValidationError as e:
        missing_field = e.errors()
        log_event("ERROR", {"error": str(missing_field)})
        raise ValueError(missing_field)

    target_in_db = db["targets"].find_one({"entity_id": signal.entity_id})

    if target_in_db and target_in_db.get("status") == "destroyed":
        log_event("WARNING", f"Received signal for destroyed target", {"entity_id": signal.entity_id})
        raise ValueError(f"{signal.entity_id} is already destroyed")

    return signal, target_in_db


def define_priority(signal, target_in_db):
    if target_in_db:
        priority = target_in_db.get("priority_level", signal.priority_level)
    else:
        priority = 99

    return priority


def calculate_distance_and_save(signal, target_in_db, priority, db):
    distance = 0.0

    if target_in_db and "last_lat" in target_in_db:
        distance = haversine_km(
            target_in_db["last_lat"],
            target_in_db["last_lon"],
            signal.reported_lat,
            signal.reported_lon,
        )
    signal_data = signal.model_dump()
    signal_data["distance_km"] = distance
    signal_data["priority_level"] = priority

    db["intel_signals"].insert_one(signal_data)

    db["targets"].update_one(
        {"entity_id": signal.entity_id},
        {
            "$set": {
                "last_lat": signal.reported_lat,
                "last_lon": signal.reported_lon,
                "priority_level": priority,
            }
        },
        upsert=True
    )
    return distance

log_event("INFO", "Intel processor started")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    raw_value = msg.value().decode('utf-8')
    try:
        raw_data = json.loads(raw_value)
        signal, target_in_db = validate_message(raw_data, db)
        priority = define_priority(signal, target_in_db)
        distance = calculate_distance_and_save(signal, target_in_db, priority, db)
    except Exception as e:
        dlq_message = {'error_reason': str(e)}
        producer.produce('intel_signals_dlq', json.dumps(dlq_message).encode('utf-8'))
        producer.flush()
        log_event("ERROR", "Message processing failed - routing to DLQ", {"error": str(e)})
            

    



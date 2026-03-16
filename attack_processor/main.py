import os
from confluent_kafka import Consumer
from pymongo import MongoClient


consumer = Consumer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': 'attack_processor',
    'auto.offset.reset': 'earliest',
})
db = MongoClient(os.getenv('MONGO_URI', 'mongodb://mongodb:27017/'))['digital_hunter']
attacks = db['attacks']
targets = db['targets']

consumer.subscribe(['attack'])

REQUIRED = ['attack_id', 'entity_id', 'weapon_type', 'timestamp']

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    try:
        payload = json.loads(msg.value().decode('utf-8'))
        missing = [f for f in REQUIRED if f not in payload]
        if missing:
            raise ValueError(f'Missing fields: {missing}')
        eid = payload['entity_id']
        attacks.insert_one(payload)
        targets.update_one(
            {'entity_id': eid},
            {'$set': {'status': 'attacked', 'last_attack_id': payload['attack_id']}},
            upsert=True,
        )
    except Exception as e:
        print(e)
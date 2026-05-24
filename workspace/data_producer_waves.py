import json
import time
import random
import uuid
import math
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'user_events_bursty'

print(f"Streaming into topic '{TOPIC_NAME}' initiated (Time-Based Bursty Mode)")

CYCLE_MINUTES = 24
cycle_duration_sec = CYCLE_MINUTES * 60

MAX_MSG_PER_SEC = 1000 
MIN_MSG_PER_SEC = 20   

min_sleep = 1.0 / MAX_MSG_PER_SEC
max_sleep = 1.0 / MIN_MSG_PER_SEC

base_sleep = (max_sleep + min_sleep) / 2.0
amplitude = (max_sleep - min_sleep) / 2.0

start_time = time.time()
counter = 0

try:
    while True:
        event = {
            "user_id": random.randint(1, 10000),
            "shop": random.choice(["rozetka", "dniprom", "foxtrot", "citrus", "allo"]),
            "event_type": random.choice(["page_view", "click", "add_to_cart", "purchase", "change_lang", "register_success"]),
            "device_type": random.choice(["ios", "android", "desktop", "mobile_web"]),
            "price": round(random.uniform(5.0, 150.0), 2) if random.random() > 0.8 else 0.0,
            "session_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        producer.send(TOPIC_NAME, value=event)
        print(f"Sent: {event}")

        elapsed_time = time.time() - start_time
        
        current_sleep = base_sleep - (amplitude * math.sin((2 * math.pi / cycle_duration_sec) * elapsed_time))
        
        current_sleep = max(0.0001, current_sleep) 
        
        time.sleep(current_sleep)
        counter += 1
        
        if counter % 500 == 0:
            msgs_per_sec = int(1.0 / current_sleep)
            elapsed_minutes = elapsed_time / 60.0
            print(f"Elapsed: {elapsed_minutes:.1f}m | Sent: {counter} | Speed: ~{msgs_per_sec} msg/sec")

except KeyboardInterrupt:
    print("\nStreaming stopped")
finally:
    producer.flush()
    producer.close()
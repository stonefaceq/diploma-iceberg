import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'user_events'

print(f"Streaming into topic '{TOPIC_NAME}' initiated")

try:
    while True:
        event = {
            "user_id": random.randint(1, 10000),
            "event_type": random.choice(["page_view", "click", "add_to_cart", "purchase"]),
            "price": round(random.uniform(5.0, 150.0), 2) if random.random() > 0.8 else 0.0,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        producer.send(TOPIC_NAME, value=event)
        print(f"Sent: {event}")
        time.sleep(0.001) # 1000 events per sec

except KeyboardInterrupt:
    print("\Streaming stopped")
finally:
    producer.flush()
    producer.close()
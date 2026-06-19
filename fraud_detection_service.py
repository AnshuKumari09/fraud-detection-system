# KafkaProducer  → data bhejta hai
# KafkaConsumer → data leta hai
# FastAPI (Transaction Service)
#         ↓
# KafkaProducer
#         ↓
# Kafka Topic: transactions
#----------
# Kafka Topic: transactions
#         ↓
# Fraud Detection Service (Consumer)
#         ↓
# Redis / Rules check
#         ↓
# fraud_alerts topic

import json
import time
import redis
from kafka import KafkaConsumer, KafkaProducer

# Redis
r = redis.Redis(
    host='localhost',
    port=6379,
    password='scanremember',
    decode_responses=True
)

# FastAPI (Producer)
#       ↓
# Kafka topic: transactions
#       ↓
# Fraud Consumer (THIS CODE)
#       ↓
# Checks rules:
#     - high amount?
#     - too frequent?
#     - suspicious location?
#       ↓
# If fraud:
#       ↓
# Kafka Producer
#       ↓
# fraud_alerts topic
#       ↓
# Alert Service

# Kafka Consumer — transactions topic se padhega
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9094',
    group_id='fraud-detection-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'  # pehle se stored messages bhi padho
)

# Kafka Producer — fraud_alerts topic mein likhega
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_fraud_alert(transaction, reason):
    alert = {
        "user_id": transaction["user_id"],
        "transaction_id": transaction["transaction_id"],
        "amount": transaction["amount"],
        "location": transaction["location"],
        "reason": reason,
        "alert_time": int(time.time() * 1000)
    }
    producer.send('fraud_alerts', value=alert)
    producer.flush()
    print(f"🚨 FRAUD ALERT: {reason} | User: {transaction['user_id']}")

#Rule 1 (Large Amount)
def check_large_amount(transaction):
    if transaction["amount"] > 10000:
        send_fraud_alert(transaction, "Large Transaction Detected")

#Rule 2 (Rapid Transactions)
def check_rapid_transactions(transaction):
    user_id = transaction["user_id"]
    timestamp = transaction["timestamp"]

    # Redis key → "user:u1:timestamps"
    ts_key = f"user:{user_id}:timestamps"

    # Naya timestamp daalo list mein (left side se)
    r.lpush(ts_key, timestamp)

    # Sirf last 5 timestamps rakhho
    r.ltrim(ts_key, 0, 4)

    # Saare timestamps uthao
    timestamps = r.lrange(ts_key, 0, -1)

    # 5 transactions hue hain tabhi check karo
    if len(timestamps) >= 5:
        latest = int(timestamps[0])   # sabse naya
        oldest = int(timestamps[-1])  # sabse purana

        # Difference 30 seconds se kam?
        diff = latest - oldest
        if diff < 30000:  # 30000 milliseconds = 30 seconds
            send_fraud_alert(transaction, "Multiple Rapid Transactions Detected")



#Rule 3 (Location Change)
def check_location_change(transaction):
    user_id = transaction["user_id"]
    current_location = transaction["location"]

    # Redis key → "user:u1:location"
    loc_key = f"user:{user_id}:location"

    # Pehle ki location uthao
    last_location = r.get(loc_key)

    if last_location and last_location != current_location:
        # Location badli hai → Alert!
        send_fraud_alert(transaction, f"Unusual Location Change: {last_location} → {current_location}")

    # Current location save karo next time ke liye
    r.set(loc_key, current_location)


print("🟢 Fraud Detection Service Started...")
 #Ye for loop hamesha chalta rehta hai — jab bhi naya message aaye Kafka se, turant process karo.
for message in consumer:
    transaction = message.value
    print(f"📥 Transaction mili: {transaction['transaction_id']} | User: {transaction['user_id']} | Amount: {transaction['amount']}")

    # Teeno rules check karo
    check_large_amount(transaction)
    check_rapid_transactions(transaction)
    check_location_change(transaction)
import json
from kafka import KafkaConsumer

# Consumer — fraud_alerts topic se padhega
consumer = KafkaConsumer(
    'fraud_alerts',
    bootstrap_servers='localhost:9094',
    group_id='alert-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("🟢 Alert Service Started — Waiting for Fraud Alerts...")

for message in consumer:
    alert = message.value
    print("=" * 50)
    print(f"🚨 FRAUD ALERT RECEIVED!")
    print(f"👤 User ID     : {alert['user_id']}")
    print(f"💰 Amount      : {alert['amount']}")
    print(f"📍 Location    : {alert['location']}")
    print(f"⚠️  Reason      : {alert['reason']}")
    print(f"🔑 Txn ID      : {alert['transaction_id']}")
    print("=" * 50)
    
    # Real world mein yahan Email/SMS code aayega
    # send_email(alert)
    # send_sms(alert)
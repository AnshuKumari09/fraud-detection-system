from fastapi import FastAPI #api bnane ke liye
import json #string me convert krne ke liye
import uuid # unique id to each transaction
import time #timestamps
import redis #connect with redis
from kafka import KafkaProducer #Kafka mein message bhejne ke liye
from pydantic import BaseModel

app=FastAPI()

# redis connection
r=redis.Redis(
    host='localhost',
    port=6379,
    password='scanremember',
    decode_responses=True #redis by default bytes return krta h ,, ye lagane se normal string milega
)

# kafka producer connection #Kafka sirf bytes store karta hai.
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)
# Input:
# v = {
#     "user_id": "rahul123",
#     "amount": 5000
# }
# json.dumps(v)
# Dictionary → JSON String ->'{"user_id": "rahul123", "amount": 5000}'
# .encode("utf-8") String → Bytes

class transaction(BaseModel):
    user_id:str
    amount:float
    currency:str
    location:str

@app.post("/api/v1/transaction")
def create_transaction(txn:transaction):
    transaction_id = str(uuid.uuid4())
    transaction_record={
        "transaction_id":transaction_id,
        "user_id":txn.user_id,
        "amount":txn.amount,
        "currency":txn.currency,
        "location":txn.location,
        "timestamp":int(time.time()*100)

    }
    # Redis key banao → "user:u1:transactions"
    redis_key = f"user:{txn.user_id}:transactions"

    # List mein left side se push karo (latest pehle)
    r.lpush(redis_key, json.dumps(transaction_record))

    # Sirf last 10 rakhho, baaki delete
    r.ltrim(redis_key, 0, 9)
    # Kafka topic mein bhejo
    producer.send(
        'transactions',              # topic name
        key=txn.user_id,            # same user → same partition
        value=transaction_record
    )
    producer.flush()  # ensure ho jaye ki message gaya
    return {
        "status": "success",
        "transaction_id": transaction_id
    }

# Ye topic ka naam hai. Topic ko newspaper category samjho.

# Sports
# Politics
# Finance

# Kafka me:

# transactions
# fraud_alerts
# payments

# transactions topic -> Topic ke andar partitions hote hain.

# Partition 0
# Partition 1
# Partition 2
# Partition 3

# FastAPI
#    |
#    | producer.send()
#    |
#    v
# Kafka Broker
#    |
#    v
# transactions Topic
#    |
#    v
# Partition Selected
# (hash(user_id))
#    |
#    v
# Store Message

# Laptop
#    |
# localhost
#    |
#    +---- Port 3000 -> FastAPI
#    +---- Port 6379 -> Redis
#    +---- Port 8080 -> Kafka UI
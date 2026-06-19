# Real-Time Fraud Detection System 🚨

## Tech Stack
- Apache Kafka — Event Streaming
- Redis — Transaction History Storage
- FastAPI — Transaction Service
- Python — Fraud Detection & Alert Service

## Architecture
Client → Transaction Service → Kafka → Fraud Detection → Kafka → Alert Service

## Fraud Detection Rules
1. Large Transaction (> ₹10,000)
2. Multiple Rapid Transactions (5 transactions < 30 seconds)
3. Unusual Location Change

## Setup
```bash
# Start Kafka + Redis
docker-compose up -d

# Run Services (3 terminals)
uvicorn transaction_service:app --reload --port 3000
python fraud_detection_service.py
python alert_service.py
```

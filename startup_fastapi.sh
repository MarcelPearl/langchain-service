#!/bin/bash
# FastAPI Node Executor Startup Script

set -e

echo "🚀 Starting FastAPI Workflow Node Executor..."

# Environment setup
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Default environment variables if not set
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Create logs directory
mkdir -p logs




# Install requirements
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Download required models (if not present)
python <<EOF
from transformers import GPT2LMHeadModel, GPT2Tokenizer
from huggingface_hub import login
import logging

HF_TOKEN = "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"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    logger.info('🔐 Logging into Hugging Face Hub...')
    login(token=HF_TOKEN)

    logger.info('📦 Downloading GPT2 tokenizer...')
    tokenizer = GPT2Tokenizer.from_pretrained('gpt2')

    logger.info('🧠 Downloading GPT2 model...')
    model = GPT2LMHeadModel.from_pretrained('gpt2')

    logger.info('✅ Models ready')
except Exception as e:
    logger.error(f'❌ Failed to download models: {e}')
EOF


# Health check for dependencies
echo "🏥 Checking dependencies..."
python -c "
import sys
import redis
from kafka import KafkaProducer

# Check Redis
try:
    r = redis.from_url('${REDIS_URL}')
    r.ping()
    print('✅ Redis connection successful')
except Exception as e:
    print(f'❌ Redis connection failed: {e}')
    sys.exit(1)

# Check Kafka
try:
    producer = KafkaProducer(bootstrap_servers='${KAFKA_BOOTSTRAP_SERVERS}')
    producer.close()
    print('✅ Kafka connection successful')
except Exception as e:
    print(f'❌ Kafka connection failed: {e}')
    sys.exit(1)
"

# Start the FastAPI application
echo "🚀 Starting FastAPI application..."
echo "📊 Configuration:"
echo "  - Kafka: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  - Redis: ${REDIS_URL}"
echo "  - Log Level: ${LOG_LEVEL}"
echo ""

# Run with uvicorn for production
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 1 \
    --log-level "${LOG_LEVEL,,}" \
    --access-log \
    --log-config logging.json
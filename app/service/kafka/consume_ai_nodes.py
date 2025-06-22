import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.schema.nodeexecutionmessage import NodeExecutionMessage
from app.services.ai.ai_node_executor import execute_ai_node
from app.core.config import settings

logger = logging.getLogger(__name__)

async def consume_ai_nodes():
    """Consumer for AI node execution messages from fastapi-nodes topic"""
    consumer = AIOKafkaConsumer(
        'fastapi-nodes',  # Topic that Spring Boot routes AI nodes to
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id='fastapi-ai-executor',  # Different from Spring Boot group
        enable_auto_commit=False,
        auto_offset_reset=settings.kafka.consumer_auto_offset_reset,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    await consumer.start()
    try:
        logger.info("✅ AI Node Consumer started")
        async for msg in consumer:
            try:
                data = NodeExecutionMessage(**msg.value)
                logger.info(f"🤖 Received AI node {data.nodeId} type {data.nodeType}")
                
                await execute_ai_node(data)
                
                await consumer.commit()
            except Exception as e:
                logger.exception(f"❌ Error handling AI node execution: {e}")
                await consumer.commit()  # avoid retry loop
    finally:
        await consumer.stop()
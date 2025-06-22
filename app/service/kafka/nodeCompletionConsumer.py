# app/services/kafka/node_completion_consumer.py

import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from app.schema.nodecompletionmessage import NodeCompletionMessage
from app.workflow.workflow_coordinator import handle_node_completion
from app.core.config import settings

logger = logging.getLogger(__name__)

async def consume_node_completion():
    consumer = AIOKafkaConsumer(
        'node-completion',
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka.consumer_group_id,
        enable_auto_commit=False,
        auto_offset_reset=settings.kafka.consumer_auto_offset_reset,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    )

    await consumer.start()
    try:
        logger.info("✅ NodeCompletionConsumer started")
        async for msg in consumer:
            try:
                data = NodeCompletionMessage(**msg.value)
                logger.info(f"📥 Received completion for node {data.nodeId} with status {data.status}")
                
                await handle_node_completion(data)

                await consumer.commit()
            except Exception as e:
                logger.exception(f"❌ Error handling node-completion message: {e}")
                await consumer.commit()  # avoid retry loop
    finally:
        await consumer.stop()

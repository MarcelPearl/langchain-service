import json
import logging
import asyncio
from typing import Callable
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from app.core.config import settings
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage

logger = logging.getLogger(__name__)

class KafkaService:
    """Kafka service matching Spring Boot configuration and patterns"""
    
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.running = False
    
    def _create_consumer(self):
        """Create Kafka consumer with exact Spring Boot configuration"""
        return KafkaConsumer(
            settings.kafka_node_execution_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            auto_offset_reset=settings.kafka_auto_offset_reset,
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,  # Match Spring Boot
            session_timeout_ms=30000,     # Match Spring Boot
            heartbeat_interval_ms=3000,   # Match Spring Boot
            max_poll_records=500,         # Match Spring Boot
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=1000
        )
    
    def _create_producer(self):
        """Create Kafka producer with exact Spring Boot configuration"""
        return KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks=1,      # Match Spring Boot
            retries=3,   # Match Spring Boot
            batch_size=16384,      # Match Spring Boot
            linger_ms=5,          # Match Spring Boot
            buffer_memory=33554432 # Match Spring Boot
        )
    
    async def start_consumer(self, node_executor_callback: Callable):
        """Start consuming messages from Kafka"""
        self.running = True
        logger.info(f"🔗 Starting Kafka consumer for topic: {settings.kafka_node_execution_topic}")
        logger.info(f"🔗 Consumer group: {settings.kafka_group_id}")
        
        try:
            self.consumer = self._create_consumer()
            self.producer = self._create_producer()
            
            logger.info("✅ Kafka consumer started, waiting for messages...")
            
            while self.running:
                try:
                    # Poll for messages with timeout
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                logger.info(f"📨 Raw message received from {topic_partition}: {message.value}")
                                
                                # Parse the message using the exact Spring Boot format
                                execution_message = NodeExecutionMessage.model_validate(message.value)
                                
                                logger.info(
                                    f"📨 Parsed node execution: {execution_message.nodeId} "
                                    f"of type: {execution_message.nodeType} "
                                    f"for execution: {execution_message.executionId}"
                                )
                                
                                # Execute the node asynchronously
                                await node_executor_callback(execution_message)
                                
                            except Exception as e:
                                logger.error(f"❌ Error processing message: {e}")
                                logger.error(f"❌ Message content: {message.value}")
                                # Send failure message
                                await self._send_failure_completion(message.value, str(e))
                    
                    # Small sleep to prevent busy waiting
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"❌ Error in consumer loop: {e}")
                    await asyncio.sleep(5)  # Wait before retrying
                    
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka consumer: {e}")
            raise
        finally:
            logger.info("🛑 Kafka consumer stopped")
    
    async def publish_completion(self, completion_message: NodeCompletionMessage):
        """Publish node completion message matching Spring Boot format"""
        try:
            if not self.producer:
                self.producer = self._create_producer()
            
            # Convert to dict for JSON serialization
            message_dict = completion_message.model_dump(by_alias=True)
            
            future = self.producer.send(
                settings.kafka_node_completion_topic,
                key=completion_message.nodeId,
                value=message_dict
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"✅ Published completion for node: {completion_message.nodeId} "
                f"with status: {completion_message.status} "
                f"to partition: {record_metadata.partition}"
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to publish completion message: {e}")
            raise
    
    async def _send_failure_completion(self, original_message: dict, error: str):
        """Send failure completion message"""
        try:
            from datetime import datetime
            
            completion = NodeCompletionMessage(
                executionId=original_message.get('executionId') or original_message.get('execution_id'),
                workflowId=original_message.get('workflowId') or original_message.get('workflow_id'),
                nodeId=original_message.get('nodeId') or original_message.get('node_id'),
                nodeType=original_message.get('nodeType') or original_message.get('node_type'),
                status="FAILED",
                output={"error": error},
                error=error,
                timestamp=datetime.now().isoformat(),
                processingTime=0
            )
            await self.publish_completion(completion)
        except Exception as e:
            logger.error(f"❌ Failed to send failure completion: {e}")
    
    async def close(self):
        """Close Kafka connections"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        logger.info("✅ Kafka connections closed")
        
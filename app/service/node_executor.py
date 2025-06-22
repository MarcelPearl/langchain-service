import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any

from pydantic import validator
from app.handlers.summarizationhandler import SummarizationHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.service.redis import RedisService
from app.handlers.textgeneration import TextGenerationHandler
from app.handlers.kmeanshandler import KMeansHandler
from app.handlers.pythontaskhandler import PythonTaskHandler

logger = logging.getLogger(__name__)

class NodeExecutorService:
    """Node executor service matching Spring Boot StaticNodeExecutor pattern"""
    
    def __init__(self, redis_service: RedisService, kafka_service):
        self.redis_service = redis_service
        self.kafka_service = kafka_service
        
        # Initialize ALL node handlers - exact match to Spring Boot pattern
        self.handlers = {
            "text-generation": TextGenerationHandler(redis_service),
            "k-means": KMeansHandler(redis_service),
            "python-task": PythonTaskHandler(redis_service),
            "question-answer": TextGenerationHandler(redis_service),  
            "clusterization": KMeansHandler(redis_service),  
            "ai_decision": TextGenerationHandler(redis_service),
            "ai_classification": TextGenerationHandler(redis_service),
            "summarization": SummarizationHandler(redis_service),
            "llm_prompt": TextGenerationHandler(redis_service),
            "ai_transform": PythonTaskHandler(redis_service),
            "sentiment_analysis": TextGenerationHandler(redis_service),
            "data_analysis": PythonTaskHandler(redis_service),
            "ml_prediction": KMeansHandler(redis_service),
        }
        
        logger.info(f"🔧 Initialized node executor with handlers: {list(self.handlers.keys())}")
    
    async def execute_node(self, message: NodeExecutionMessage):
        """Execute a node - exact match to Spring Boot StaticNodeExecutor.executeNode"""
        start_time = time.time()
        node_id = message.nodeId
        node_type = message.nodeType.lower()
        
        logger.info(f"🔄 Executing node: {node_id} of type: {node_type} for execution: {message.executionId}")
        
        try:
            # Store execution context in Redis (matching Spring Boot pattern)
            await self._store_execution_context(message)
            
            # Find handler (matching Spring Boot findHandler pattern)
            handler = self._find_handler(node_type)
            if not handler:
                error = f"No handler found for node type: {node_type}"
                logger.error(error)
                await self._publish_failure_event(message, error, start_time)
                return
            
            # Execute the handler
            output = await handler.execute(message)
            
            processing_time = int((time.time() - start_time) * 1000)
            logger.info(f"✅ Node execution completed: {node_id} in {processing_time}ms")
            
            # Note: Handler publishes its own completion event (matching Spring Boot pattern)
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Node execution failed: {node_id} after {processing_time}ms", exc_info=True)
            await self._publish_failure_event(message, str(e), start_time)
    
    def _find_handler(self, node_type: str):
        """Find handler for node type - matching Spring Boot pattern"""
        handler = self.handlers.get(node_type.lower())
        if not handler:
            # Try with underscores replaced by hyphens
            handler = self.handlers.get(node_type.lower().replace('_', '-'))
        return handler
    
    async def _store_execution_context(self, message: NodeExecutionMessage):
        """Store execution context in Redis - matching Spring Boot pattern"""
        try:
            context_key = f"execution:{message.executionId}:node:{message.nodeId}"
            context_data = {
                "node_type": message.nodeType,
                "started_at": datetime.now().isoformat(),
                "status": "RUNNING"
            }
            await self.redis_service.set(context_key, context_data, ex=3600)  # 1 hour TTL
        except Exception as e:
            logger.warning(f"Failed to store execution context: {e}")
    
    async def _publish_failure_event(self, message: NodeExecutionMessage, error: str, start_time: float):
        """Publish failure event - exact match to Spring Boot pattern"""
        try:
            processing_time = int((time.time() - start_time) * 1000)
            
            failure_message = NodeCompletionMessage(
                executionId=message.executionId,
                workflowId=message.workflowId,
                nodeId=message.nodeId,
                nodeType=message.nodeType,
                status="FAILED",
                output={
                    "error": error,
                    "failed_at": datetime.now().isoformat(),
                    "node_type": message.nodeType
                },
                error=error,
               timestamp=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                processingTime=processing_time,
                service="fastapi"  # Mark as FastAPI service
            )
            
            await self.kafka_service.publish_completion(failure_message)
            logger.info(f"Published failure event for node: {message.nodeId}")
            
        except Exception as publish_error:
            logger.error(f"Failed to publish failure event for node: {message.nodeId}", exc_info=True)
            
@validator('timestamp', pre=True, always=True)
def ensure_string_timestamp(cls, v):
    if isinstance(v, datetime):
        # ✅ Format with milliseconds only, to match Java Instant
        return v.isoformat(timespec='milliseconds') + 'Z'
    return str(v)
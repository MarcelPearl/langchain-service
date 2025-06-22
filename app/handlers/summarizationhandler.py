import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any
from pydantic import Field
from transformers import pipeline
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings

logger = logging.getLogger(__name__)

class SummarizationHandler(BaseNodeHandler):
    def __init__(self, redis_service):
        super().__init__(redis_service)
        logger.info("📥 Loading summarization pipeline...")
        self.summarizer = pipeline("summarization", model=settings.summarization_model)
        logger.info("✅ Summarization model loaded")

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"📝 Executing summarization node: {message.nodeId}")

        try:
            node_data = message.nodeData
            context = message.context or {}

            text = self.substitute_template_variables(node_data.get("text", ""), context)
            max_length = node_data.get("max_length", 130)
            min_length = node_data.get("min_length", 30)

            logger.info(f"📄 Summarizing text: {text[:50]}...")

            summary = self.summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)[0]

            output = {
                "summary": summary['summary_text'],
                "original_text": text,
                "node_type": "summarization",
                "node_executed_at": datetime.now().isoformat(),
                "model_used": settings.summarization_model
            }

            if context:
                output.update(context)

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Summarization completed in {processing_time}ms")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Summarization failed: {message.nodeId}", exc_info=True)

            error_output = {
                "error": str(e),
                "summary": None,
                "failed_at": datetime.now().isoformat(),
                "node_type": "summarization"
            }
            if context:
                error_output.update(context)

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _publish_completion_event(self, message, output, status, processing_time):
        try:
            from app.main import app
            completion_message = NodeCompletionMessage(
                executionId=message.executionId,
                workflowId=message.workflowId,
                nodeId=message.nodeId,
                nodeType=message.nodeType,
                status=status,
                output=output,
                error=output.get("error") if status == "FAILED" else None,
                timestamp=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                processingTime=processing_time
            )
            if hasattr(app.state, 'kafka_service'):
                await app.state.kafka_service.publish_completion(completion_message)
                logger.info(f"Published summarization completion: {message.nodeId}")
        except Exception as e:
            logger.error(f"Failed to publish completion event: {e}")

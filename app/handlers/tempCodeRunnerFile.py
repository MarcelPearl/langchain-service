import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings

logger = logging.getLogger(__name__)

class TextGenerationHandler(BaseNodeHandler):
    """Text generation handler using Hugging Face causal language models"""

    def __init__(self, redis_service):
        super().__init__(redis_service)
        self.model = None
        self.tokenizer = None
        self._load_model()

    def _load_model(self):
        """Load a more capable Hugging Face model"""
        try:
            logger.info(f"📥 Loading text generation model: 	openchat/openchat-3.5-1210")
            self.tokenizer = AutoTokenizer.from_pretrained("openchat/openchat-3.5-1210",
                                                            use_auth_token=True)
            self.model = AutoModelForCausalLM.from_pretrained("openchat/openchat-3.5-1210",
                                                               use_auth_token=True)

            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            logger.info("✅ Model loaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to load model: {e}", exc_info=True)
            raise

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Execute the node for text generation"""
        start_time = time.time()
        logger.info(f"🚀 Executing text-generation node: {message.nodeId}")

        try:
            node_data = message.nodeData
            context = message.context if message.context else {}

            logger.info(f"🔍 Context variables: {list(context.keys())}")

            raw_prompt = node_data.get('prompt', 'Hello, how are you?')
            prompt = self.substitute_template_variables(raw_prompt, context)

            max_tokens = node_data.get('max_tokens', settings.max_tokens)
            temperature = node_data.get('temperature', settings.temperature)

            logger.info(f"🤖 Generating text for prompt: {prompt[:50]}...")

            input_ids = self.tokenizer.encode(prompt, return_tensors='pt')

            with torch.no_grad():
                outputs = self.model.generate(
                    input_ids,
                    max_length=input_ids.shape[1] + max_tokens,
                    temperature=temperature,
                    top_k=50,
                    top_p=0.95,
                    do_sample=True,
                    pad_token_id=self.tokenizer.pad_token_id,
                    num_return_sequences=1
                )

            generated_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            generated_only = generated_text[len(prompt):].strip()

            output = {**context, **{
                "generated_text": generated_only,
                "full_text": generated_text,
                "original_prompt": prompt,
                "node_type": "text-generation",
                "node_executed_at": datetime.now().isoformat(),
                "model_used": settings.huggingface_model,
                "parameters": {
                    "max_tokens": max_tokens,
                    "temperature": temperature
                }
            }}

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Text generation completed in {processing_time}ms — {len(generated_only)} chars")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Text generation failed for node {message.nodeId}", exc_info=True)

            error_output = {**(message.context or {}), **{
                "error": str(e),
                "generated_text": None,
                "original_prompt": node_data.get('prompt', ''),
                "failed_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                "node_type": "text-generation"
            }}

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _publish_completion_event(self, message: NodeExecutionMessage,
                                        output: Dict[str, Any], status: str, processing_time: int):
        """Publish completion event to Kafka"""
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
                logger.info(f"📤 Published completion event for node {message.nodeId} [{status}] in {processing_time}ms")

        except Exception as e:
            logger.error(f"❌ Failed to publish completion event: {e}", exc_info=True)

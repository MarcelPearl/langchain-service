import logging
import time
import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Any
import openai
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings

logger = logging.getLogger(__name__)

class TextGenerationHandler(BaseNodeHandler):
    """Simple OpenAI text generation handler with token limiting"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        logger.info("🔧 Initializing OpenAI text generation handler...")
        
        try:
            # Get API key
            api_key = getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            
            if not api_key:
                raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
            
            self.client = openai.OpenAI(api_key=api_key)
            
            # Simple settings
            self.default_model = "gpt-3.5-turbo"  # Cheapest option
            self.max_tokens_limit = 300           # Hard limit per request
            
            logger.info(f"✅ OpenAI client ready - Max tokens per request: {self.max_tokens_limit}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Generate text using OpenAI API"""
        start_time = time.time()
        logger.info(f"🚀 Executing text-generation node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available. Check your API key.")

            node_data = message.nodeData
            context = message.context or {}

            # Get prompt
            raw_prompt = node_data.get("prompt", "Hello, how are you?")
            prompt = self.substitute_template_variables(raw_prompt, context)

            # Limit tokens (SAFETY CHECK)
            requested_tokens = node_data.get("max_tokens", 100)
            max_tokens = min(requested_tokens, self.max_tokens_limit)
            
            if requested_tokens > self.max_tokens_limit:
                logger.warning(f"⚠️ Requested {requested_tokens} tokens, limited to {self.max_tokens_limit}")

            temperature = node_data.get("temperature", 0.7)
            model = node_data.get("model", self.default_model)

            logger.info(f"🤖 Generating with {model}, max_tokens: {max_tokens}")

            # Call OpenAI API
            generated_text = await self._generate_text(prompt, max_tokens, temperature, model)

            # Build response
            output = {
                **context,
                "generated_text": generated_text,
                "full_text": f"{prompt} {generated_text}",
                "original_prompt": prompt,
                "node_type": "text-generation",
                "node_executed_at": datetime.now().isoformat(),
                "model_used": model,
                "parameters": {
                    "max_tokens": max_tokens,
                    "temperature": temperature
                }
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Generated {len(generated_text)} chars in {processing_time}ms")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Generation failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "generated_text": None,
                "original_prompt": node_data.get("prompt", ""),
                "node_type": "text-generation"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _generate_text(self, prompt: str, max_tokens: int, temperature: float, model: str) -> str:
        """Call OpenAI API"""
        
        def _call_openai():
            try:
                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=temperature,
                    timeout=30
                )
                return response.choices[0].message.content
                
            except openai.RateLimitError:
                raise RuntimeError("Rate limit exceeded. Try again later.")
            except openai.AuthenticationError:
                raise RuntimeError("Invalid API key.")
            except Exception as e:
                raise RuntimeError(f"OpenAI error: {e}")

        # Run async
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _call_openai)

    async def _publish_completion_event(self, message: NodeExecutionMessage,
                                        output: Dict[str, Any], status: str, processing_time: int):
        """Publish completion event"""
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
                
        except Exception as e:
            logger.error(f"❌ Failed to publish event: {e}")

    def update_token_limit(self, new_limit: int):
        """Update the token limit"""
        old_limit = self.max_tokens_limit
        self.max_tokens_limit = new_limit
        logger.info(f"🔧 Token limit updated: {old_limit} -> {new_limit}")
        return {"old_limit": old_limit, "new_limit": new_limit}
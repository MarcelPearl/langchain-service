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

class SummarizationHandler(BaseNodeHandler):
    """Simple OpenAI summarization handler with token limiting"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        logger.info("📥 Loading OpenAI summarization handler...")
        
        try:
            # Get API key
            api_key = getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            
            if not api_key:
                raise ValueError("OpenAI API key not found. Set OPENAI_API_KEY environment variable.")
            
            self.client = openai.OpenAI(api_key=api_key)
            
            # Simple settings
            self.default_model = "gpt-3.5-turbo"  # Cheapest option
            self.max_summary_tokens = 200         # Hard limit for summary length
            
            logger.info(f"✅ OpenAI summarization ready - Max summary tokens: {self.max_summary_tokens}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Summarize text using OpenAI API"""
        start_time = time.time()
        logger.info(f"📝 Executing summarization node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available. Check your API key.")

            node_data = message.nodeData
            context = message.context or {}

            # Get text to summarize
            text = self.substitute_template_variables(node_data.get("text", ""), context)
            
            if not text.strip():
                raise ValueError("No text provided for summarization")

            # Get parameters with limits
            requested_length = node_data.get("max_length", 130)
            max_summary_tokens = min(requested_length, self.max_summary_tokens)
            
            if requested_length > self.max_summary_tokens:
                logger.warning(f"⚠️ Requested {requested_length} tokens, limited to {self.max_summary_tokens}")

            min_length = node_data.get("min_length", 30)
            model = node_data.get("model", self.default_model)

            logger.info(f"📄 Summarizing {len(text)} chars with {model}")

            # Generate summary
            summary = await self._generate_summary(text, max_summary_tokens, min_length, model)

            # Build response
            output = {
                **context,
                "summary": summary,
                "original_text": text,
                "node_type": "summarization",
                "node_executed_at": datetime.now().isoformat(),
                "model_used": model,
                "parameters": {
                    "max_length": max_summary_tokens,
                    "min_length": min_length,
                    "original_length": len(text)
                }
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Summarized {len(text)} chars -> {len(summary)} chars in {processing_time}ms")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Summarization failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "summary": None,
                "original_text": node_data.get("text", ""),
                "failed_at": datetime.now().isoformat(),
                "node_type": "summarization"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _generate_summary(self, text: str, max_tokens: int, min_length: int, model: str) -> str:
        """Generate summary using OpenAI API"""
        
        def _call_openai():
            try:
                # Create a good summarization prompt
                prompt = f"""Please summarize the following text in {min_length}-{max_tokens} words. Make it concise and capture the key points:

Text to summarize:
{text}

Summary:"""

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=max_tokens,
                    temperature=0.3,  # Lower temperature for more focused summaries
                    timeout=30
                )
                
                summary = response.choices[0].message.content.strip()
                
                # Clean up the summary (remove "Summary:" prefix if it exists)
                if summary.lower().startswith("summary:"):
                    summary = summary[8:].strip()
                
                return summary
                
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
                logger.info(f"📤 Published summarization completion: {message.nodeId}")
                
        except Exception as e:
            logger.error(f"❌ Failed to publish event: {e}")

    def update_token_limit(self, new_limit: int):
        """Update the summary token limit"""
        old_limit = self.max_summary_tokens
        self.max_summary_tokens = new_limit
        logger.info(f"🔧 Summary token limit updated: {old_limit} -> {new_limit}")
        return {"old_limit": old_limit, "new_limit": new_limit}
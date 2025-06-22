import logging
import time
import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Any, List
import openai
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings
logger = logging.getLogger(__name__)

class TextClassificationHandler(BaseNodeHandler):
    """AI-based text classification handler"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key = getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            self.default_model = "gpt-3.5-turbo"
            logger.info("✅ OpenAI text classification handler ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"🏷️ Executing text-classification node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}




            text = self.substitute_template_variables(node_data.get("text", ""), context)
            categories = node_data.get("categories", ["positive", "negative", "neutral"])
            model = node_data.get("model", self.default_model)

            if not text.strip():
                raise ValueError("No text provided for classification")

            logger.info(f"🏷️ Classifying text into categories: {categories}")

      


            classification_result = await self._classify_text(text, categories, model)

            output = {
                **context,
                "text": text,
                "classification": classification_result["category"],
                "confidence": classification_result["confidence"],
                "categories": categories,
                "model_used": model,
                "node_type": "text-classification",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Text classified as: {classification_result['category']}")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Text classification failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "text": node_data.get("text", ""),
                "classification": None,
                "node_type": "text-classification"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _classify_text(self, text: str, categories: List[str], model: str) -> Dict[str, Any]:
        def _call_openai():
            try:
                categories_str = ", ".join(categories)
                prompt = f"""Classify the following text into one of these categories: {categories_str}

Text: "{text}"

Respond with only the category name and a confidence score (0-1).
Format: Category: [category], Confidence: [score]"""

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=0.1,
                    timeout=30
                )
                
                result = response.choices[0].message.content.strip()
                
     


                try:
                    parts = result.split(", ")
                    category = parts[0].replace("Category: ", "").strip()
                    confidence = float(parts[1].replace("Confidence: ", "").strip())
                except:
      

      
                    category = result.split(",")[0] if "," in result else result
                    confidence = 0.5
                
                return {"category": category, "confidence": confidence}
                
            except Exception as e:
                raise RuntimeError(f"OpenAI error: {e}")

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _call_openai)

    async def _publish_completion_event(self, message: NodeExecutionMessage,
                                      output: Dict[str, Any], status: str, processing_time: int):
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
            logger.error(f"Failed to publish completion event: {e}")

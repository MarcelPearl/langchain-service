import logging
import time
import asyncio
import os
from datetime import datetime, timezone
from typing import Dict, Any
import openai
from app.core.config import settings
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage

logger = logging.getLogger(__name__)

class TranslationHandler(BaseNodeHandler):
    """Text translation handler using OpenAI"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key =getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            self.default_model = "gpt-3.5-turbo"
            logger.info("✅ OpenAI translation handler ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None



    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"🌍 Executing translation node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}




            text = self.substitute_template_variables(node_data.get("text", ""), context)
            source_language = node_data.get("source_language", "auto")
            target_language = node_data.get("target_language", "English")
            model = node_data.get("model", self.default_model)

            if not text.strip():
                raise ValueError("No text provided for translation")

            logger.info(f"🌍 Translating from {source_language} to {target_language}")




            translated_text = await self._translate_text(text, source_language, target_language, model)

            output = {
                **context,
                "original_text": text,
                "translated_text": translated_text,
                "source_language": source_language,
                "target_language": target_language,
                "model_used": model,
                "node_type": "translation",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Translation completed in {processing_time}ms")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Translation failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "original_text": node_data.get("text", ""),
                "translated_text": None,
                "node_type": "translation"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise



    async def _translate_text(self, text: str, source_lang: str, target_lang: str, model: str) -> str:
        def _call_openai():
            try:
                if source_lang == "auto":
                    prompt = f"Translate the following text to {target_lang}:\n\n{text}"
                else:
                    prompt = f"Translate the following {source_lang} text to {target_lang}:\n\n{text}"

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,
                    temperature=0.1,
                    timeout=30
                )
                
                return response.choices[0].message.content.strip()
                
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


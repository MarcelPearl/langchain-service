# app/handlers/questionanswerhandler.py
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

class QuestionAnswerHandler(BaseNodeHandler):
    """Question-Answer handler using OpenAI"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key=getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            self.default_model = "gpt-3.5-turbo"
            logger.info("✅ OpenAI Q&A handler ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"❓ Executing question-answer node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}


            question = self.substitute_template_variables(node_data.get("question", ""), context)
            context_text = self.substitute_template_variables(node_data.get("context_text", ""), context)
            model = node_data.get("model", self.default_model)

            if not question.strip():
                raise ValueError("No question provided")

            logger.info(f"🤔 Answering question: {question[:50]}...")


            answer = await self._generate_answer(question, context_text, model)

            output = {
                **context,
                "question": question,
                "answer": answer,
                "context_text": context_text,
                "model_used": model,
                "node_type": "question-answer",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Question answered in {processing_time}ms")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Question answering failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "question": node_data.get("question", ""),
                "answer": None,
                "node_type": "question-answer"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _generate_answer(self, question: str, context_text: str, model: str) -> str:
        def _call_openai():
            try:
                if context_text:
                    prompt = f"""Based on the following context, answer the question:

Context: {context_text}

Question: {question}

Answer:"""
                else:
                    prompt = f"Question: {question}\n\nAnswer:"

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.3,
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


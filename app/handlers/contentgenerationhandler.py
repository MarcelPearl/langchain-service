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

class ContentGenerationHandler(BaseNodeHandler):
    """Content generation handler for different content types"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key=getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            self.default_model = "gpt-3.5-turbo"
            logger.info("✅ OpenAI content generation handler ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize OpenAI: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"✍️ Executing content-generation node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}


            content_type = node_data.get("content_type", "blog_post")  
            topic = self.substitute_template_variables(node_data.get("topic", ""), context)
            style = node_data.get("style", "professional")
            length = node_data.get("length", "medium") 
            model = node_data.get("model", self.default_model)

            if not topic.strip():
                raise ValueError("No topic provided for content generation")

            logger.info(f"✍️ Generating {content_type} about: {topic}")

     
            generated_content = await self._generate_content(content_type, topic, style, length, model)

            output = {
                **context,
                "topic": topic,
                "generated_content": generated_content,
                "content_type": content_type,
                "style": style,
                "length": length,
                "word_count": len(generated_content.split()),
                "model_used": model,
                "node_type": "content-generation",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Generated {len(generated_content.split())} words of content")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Content generation failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "topic": node_data.get("topic", ""),
                "generated_content": None,
                "node_type": "content-generation"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _generate_content(self, content_type: str, topic: str, style: str, length: str, model: str) -> str:
        def _call_openai():
            try:
            
                prompts = {
                    "blog_post": f"Write a {style} {length} blog post about {topic}. Include an engaging introduction, main points, and conclusion.",
                    "email": f"Write a {style} {length} email about {topic}. Make it clear and actionable.",
                    "social_media": f"Create a {style} social media post about {topic}. Make it engaging and shareable.",
                    "article": f"Write a {style} {length} article about {topic}. Include detailed information and insights.",
                    "product_description": f"Write a {style} product description for {topic}. Highlight key features and benefits.",
                    "press_release": f"Write a {style} press release about {topic}. Follow standard press release format."
                }
                
                prompt = prompts.get(content_type, f"Write {style} {length} content about {topic}")

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=400 if length == "long" else 300 if length == "medium" else 200,
                    temperature=0.7,
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
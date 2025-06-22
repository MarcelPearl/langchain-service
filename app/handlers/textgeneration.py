import logging
import time
from datetime import datetime
from typing import Dict, Any
from transformers import GPT2LMHeadModel, GPT2Tokenizer
import torch
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings

logger = logging.getLogger(__name__)

class TextGenerationHandler(BaseNodeHandler):
    """Text generation handler matching Spring Boot handler patterns"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        self.model = None
        self.tokenizer = None
        self._load_model()
    
    def _load_model(self):
        """Load the text generation model"""
        try:
            logger.info(f"📥 Loading text generation model: {settings.huggingface_model}")
            self.tokenizer = GPT2Tokenizer.from_pretrained(settings.huggingface_model)
            self.model = GPT2LMHeadModel.from_pretrained(settings.huggingface_model)
            
            # Add padding token if it doesn't exist
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
            
            logger.info("✅ Text generation model loaded successfully")
        except Exception as e:
            logger.error(f"❌ Failed to load text generation model: {e}")
            raise
    
    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Execute text generation - exact match to Spring Boot handler pattern"""
        start_time = time.time()
        logger.info(f"Executing text-generation node: {message.nodeId}")
        
        try:
            node_data = message.nodeData
            context = message.context if message.context else {}
            
            logger.info(f"Text generation node context variables: {list(context.keys())}")
            
            # Get the prompt from node data, substitute template variables
            raw_prompt = node_data.get('prompt', 'Hello, how are you?')
            prompt = self.substitute_template_variables(raw_prompt, context)
            
            # Get generation parameters
            max_tokens = node_data.get('max_tokens', settings.max_tokens)
            temperature = node_data.get('temperature', settings.temperature)
            
            logger.info(f"🤖 Generating text for prompt: {prompt[:50]}...")
            
            # Tokenize input
            inputs = self.tokenizer.encode(prompt, return_tensors='pt')
            
            # Generate text
            with torch.no_grad():
                outputs = self.model.generate(
                    inputs,
                    max_length=inputs.shape[1] + max_tokens,
                    temperature=temperature,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id
                )
            
            # Decode the generated text
            generated_text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            generated_only = generated_text[len(prompt):].strip()
            
            # Build output matching Spring Boot pattern
            output = {}
            if context:
                output.update(context)  # Preserve entire context like Spring Boot
            
            # Add results
            output.update({
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
            })
            
            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)
            
            logger.info(f"✅ Text generation completed: {len(generated_only)} characters")
            return output
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Text generation node failed: {message.nodeId}", exc_info=True)
            
            # Build error output matching Spring Boot pattern
            error_output = {}
            if message.context:
                error_output.update(message.context)
            
            error_output.update({
                "error": str(e),
                "generated_text": None,
                "original_prompt": node_data.get('prompt', ''),
                "failed_at": datetime.now().isoformat(),
                "node_type": "text-generation"
            })
            
            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise
    
    async def _publish_completion_event(self, message: NodeExecutionMessage, 
                                      output: Dict[str, Any], status: str, processing_time: int):
        """Publish completion event - exact match to Spring Boot pattern"""
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
                timestamp=datetime.now().isoformat(),
                processingTime=processing_time
            )
            
            if hasattr(app.state, 'kafka_service'):
                await app.state.kafka_service.publish_completion(completion_message)
                logger.info(f"Published completion event for text generation node: {message.nodeId} "
                           f"with status: {status} in {processing_time}ms")
        except Exception as e:
            logger.error(f"Failed to publish completion event: {e}")
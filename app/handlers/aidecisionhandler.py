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

class AIDecisionHandler(BaseNodeHandler):
    """AI-powered decision node that returns true/false based on intelligent reasoning"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key=getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            self.default_model = "gpt-3.5-turbo"
            logger.info("🧠 AI decision handler ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize AI decision handler: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"🤔 Executing ai-decision node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}

            question = self.substitute_template_variables(node_data.get("question", ""), context)
            criteria = node_data.get("criteria", "")
            context_data = node_data.get("context_data", "")
            model = node_data.get("model", self.default_model)
            confidence_threshold = node_data.get("confidence_threshold", 0.7)

            if not question.strip():
                raise ValueError("No decision question provided")

            logger.info(f"🧠 AI deciding: {question[:50]}...")

            decision_result = await self._make_ai_decision(
                question, criteria, context_data, context, model
            )

            decision = decision_result["decision"]
            confidence = decision_result["confidence"]
            reasoning = decision_result["reasoning"]

            final_decision = decision and confidence >= confidence_threshold

            output = {
                **context,
                "question": question,
                "decision": final_decision,
                "raw_decision": decision,
                "confidence": confidence,
                "confidence_threshold": confidence_threshold,
                "reasoning": reasoning,
                "criteria": criteria,
                "passed_threshold": confidence >= confidence_threshold,
                "model_used": model,
                "node_type": "ai-decision",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ AI decision: {final_decision} (confidence: {confidence:.2f})")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ AI decision failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "question": node_data.get("question", ""),
                "decision": False,
                "confidence": 0.0,
                "node_type": "ai-decision"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _make_ai_decision(self, question: str, criteria: str, context_data: str, 
                               workflow_context: Dict, model: str) -> Dict[str, Any]:
        """AI makes a true/false decision with reasoning"""
        
        def _call_openai():
            try:
                context_info = ""
                if workflow_context:
                    context_keys = list(workflow_context.keys())[:10]
                    context_values = {k: str(v)[:100] for k, v in workflow_context.items() if k in context_keys}
                    context_info = f"\nWorkflow Context: {context_values}"
                
                if context_data:
                    context_info += f"\nAdditional Context: {context_data}"
                
                if criteria:
                    context_info += f"\nDecision Criteria: {criteria}"

                prompt = f"""You are an AI decision maker. Analyze the question and context, then make a TRUE or FALSE decision.

Question: {question}
{context_info}

Think through this step by step:
1. Analyze the available information
2. Consider all relevant factors
3. Make a logical decision
4. Assess your confidence

Respond in this exact format:
DECISION: [TRUE or FALSE]
CONFIDENCE: [0.0 to 1.0]
REASONING: [Brief explanation of your decision]

Example:
DECISION: TRUE
CONFIDENCE: 0.85
REASONING: Based on the sales data showing 15% growth and positive customer feedback, the product launch appears successful."""

                response = self.client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=300,
                    temperature=0.1,  
                    timeout=30
                )
                
                result = response.choices[0].message.content.strip()
                
            
                decision = False
                confidence = 0.5
                reasoning = "Unable to parse AI response"
                
                lines = result.split('\n')
                for line in lines:
                    if line.startswith('DECISION:'):
                        decision_text = line.replace('DECISION:', '').strip().upper()
                        decision = decision_text == 'TRUE'
                    elif line.startswith('CONFIDENCE:'):
                        try:
                            confidence = float(line.replace('CONFIDENCE:', '').strip())
                            confidence = max(0.0, min(1.0, confidence)) 
                        except ValueError:
                            confidence = 0.5
                    elif line.startswith('REASONING:'):
                        reasoning = line.replace('REASONING:', '').strip()
                
                return {
                    "decision": decision,
                    "confidence": confidence,
                    "reasoning": reasoning
                }
                
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



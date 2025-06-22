import logging
import time
from datetime import datetime
from typing import Dict, Any
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage

logger = logging.getLogger(__name__)

class PythonTaskHandler(BaseNodeHandler):
    """Python task handler matching Spring Boot handler patterns"""
    
    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Execute a custom Python task - exact match to Spring Boot handler pattern"""
        start_time = time.time()
        logger.info(f"Executing python-task node: {message.nodeId}")
        
        try:
            node_data = message.nodeData
            context = message.context if message.context else {}
            
            logger.info(f"Python task node context variables: {list(context.keys())}")
            
            # Get the Python code to execute
            python_code = node_data.get('code', 'result = "Hello from Python!"')
            
            # Substitute template variables in the code
            code = self.substitute_template_variables(python_code, context)
            
            logger.info(f"🐍 Executing Python task: {message.nodeId}")
            
            # Create a safe execution environment
            exec_globals = {
                'context': context,
                'np': __import__('numpy'),
                'pd': __import__('pandas'),
                'json': __import__('json'),
                'math': __import__('math'),
                'datetime': __import__('datetime'),
            }
            exec_locals = {}
            
            # Execute the code
            exec(code, exec_globals, exec_locals)
            
            # Get the result (look for 'result' variable or use all locals)
            if 'result' in exec_locals:
                task_result = exec_locals['result']
            else:
                task_result = {k: v for k, v in exec_locals.items() 
                             if not k.startswith('_')}
            
            # Build output matching Spring Boot pattern
            output = {}
            if context:
                output.update(context)  # Preserve entire context like Spring Boot
            
            # Add results
            output.update({
                "python_result": task_result,
                "code_executed": code,
                "node_type": "python-task",
                "node_executed_at": datetime.now().isoformat()
            })
            
            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)
            
            logger.info(f"✅ Python task completed successfully")
            return output
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Python task node failed: {message.nodeId}", exc_info=True)
            
            # Build error output matching Spring Boot pattern
            error_output = {}
            if message.context:
                error_output.update(message.context)
            
            error_output.update({
                "error": str(e),
                "python_result": None,
                "code_executed": node_data.get('code', ''),
                "failed_at": datetime.now().isoformat(),
                "node_type": "python-task"
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
                logger.info(f"Published completion event for python task node: {message.nodeId} "
                           f"with status: {status} in {processing_time}ms")
        except Exception as e:
            logger.error(f"Failed to publish completion event: {e}")
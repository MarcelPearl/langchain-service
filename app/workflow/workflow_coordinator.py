import asyncio
import logging
from typing import Dict, Any
from app.schema.nodecompletionmessage import NodeCompletionMessage
from app.services.workflow.workflow_store import WorkflowStore
from app.services.kafka.kafka_producer import send_node_execution

logger = logging.getLogger(__name__)

# Initialize or inject a persistent store for workflow state
workflow_store = WorkflowStore()

async def handle_node_completion(message: NodeCompletionMessage) -> None:
    """
    Processes a node completion event:
      1. Update the workflow context with the output of the completed node
      2. Mark the node as completed in the workflow state
      3. Enqueue any subsequent ready nodes for execution
    """
    execution_id = message.execution_id
    node_id = message.node_id
    status = message.status
    output = message.output
    
    logger.info(f"🔄 Handling completion: execution={execution_id}, node={node_id}, status={status}")
    
    try:
        # 1. Merge output into workflow context
        workflow_context = await workflow_store.get_context(execution_id)
        if workflow_context is None:
            logger.error(f"❌ No context found for execution {execution_id}")
            return
        
        # Merge output preserving existing context
        workflow_context.update(output)
        await workflow_store.save_context(execution_id, workflow_context)
        logger.debug(f"🌐 Context updated for execution {execution_id}")
        
        # 2. Mark node completion and handle failures
        await workflow_store.mark_node_completed(execution_id, node_id, status)
        
        if status == "FAILED":
            logger.warning(f"⚠️ Node {node_id} failed, checking workflow continuation policy")
            # Could implement failure handling policy here
            should_continue = await workflow_store.should_continue_on_failure(execution_id, node_id)
            if not should_continue:
                await workflow_store.mark_workflow_failed(execution_id, f"Node {node_id} failed")
                return
        
        # 3. Check if workflow is complete
        if await workflow_store.is_workflow_complete(execution_id):
            logger.info(f"🎉 Workflow execution {execution_id} completed successfully")
            await workflow_store.mark_workflow_completed(execution_id)
            return
        
        # 4. Identify and schedule next ready nodes
        ready_nodes = await workflow_store.get_ready_nodes(execution_id)
        
        if not ready_nodes:
            logger.info(f"⏸️ No ready nodes for execution {execution_id}, waiting for more completions")
            return
        
        # 5. Schedule ready nodes
        for next_node in ready_nodes:
            logger.info(f"▶️ Scheduling next node: {next_node['node_id']} (type={next_node['node_type']})")
            await schedule_node_execution(execution_id, next_node, workflow_context)
            
    except Exception as e:
        logger.exception(f"❌ Error handling node completion for {execution_id}: {e}")
        await workflow_store.mark_workflow_failed(execution_id, f"Coordination error: {str(e)}")

async def schedule_node_execution(execution_id: str, node: Dict[str, Any], context: Dict[str, Any]) -> None:
    """
    Publishes a NodeExecutionMessage to Kafka for the given node.
    """
    try:
        message = {
            "executionId": execution_id,
            "workflowId": node.get("workflow_id"),
            "nodeId": node.get("node_id"),
            "nodeType": node.get("node_type"),
            "nodeData": node.get("node_data", {}),
            "context": context,
            "dependencies": node.get("dependencies", []),
            "timestamp": node.get("timestamp"),
            "priority": node.get("priority", "NORMAL")
        }
        
        await send_node_execution(message)
        logger.debug(f"📨 Execution request sent for node {node.get('node_id')}")
        
    except Exception as e:
        logger.exception(f"❌ Failed to schedule node execution: {e}")
        # Mark node as failed if we can't schedule it
        await workflow_store.mark_node_completed(execution_id, node.get("node_id"), "FAILED")

async def start_workflow_execution(workflow_definition: Dict[str, Any], 
                                 payload: Dict[str, Any] = None) -> str:
    """
    Start a new workflow execution
    """
    execution_id = await workflow_store.create_execution(workflow_definition, payload)
    logger.info(f"🚀 Starting workflow execution: {execution_id}")
    
    try:
        # Get initial ready nodes (nodes with no dependencies)
        ready_nodes = await workflow_store.get_ready_nodes(execution_id)
        
        if not ready_nodes:
            raise ValueError("No initial ready nodes found in workflow")
        
        # Get initial context (payload + workflow variables)
        context = await workflow_store.get_context(execution_id)
        
        # Schedule initial nodes
        for node in ready_nodes:
            await schedule_node_execution(execution_id, node, context)
        
        logger.info(f"✅ Workflow execution {execution_id} started with {len(ready_nodes)} initial nodes")
        return execution_id
        
    except Exception as e:
        logger.exception(f"❌ Failed to start workflow execution: {e}")
        await workflow_store.mark_workflow_failed(execution_id, f"Startup error: {str(e)}")
        raise

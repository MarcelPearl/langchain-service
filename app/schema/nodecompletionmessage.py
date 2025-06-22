from pydantic import BaseModel
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime

class NodeCompletionMessage(BaseModel):
    executionId: UUID
    workflowId: UUID
    nodeId: str
    nodeType: str
    status: str
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: datetime
    processingTime: int
    service: Optional[str] = "fastapi-node-executor"

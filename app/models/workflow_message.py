from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum
import uuid

class Priority(str, Enum):
    """Priority enum matching Spring Boot exactly"""
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"

class NodeExecutionMessage(BaseModel):
    """Exact match to Spring Boot NodeExecutionMessage"""
    executionId: uuid.UUID = Field(alias="execution_id")
    workflowId: uuid.UUID = Field(alias="workflow_id")
    nodeId: str = Field(alias="node_id")
    nodeType: str = Field(alias="node_type")
    nodeData: Dict[str, Any] = Field(alias="node_data")
    context: Dict[str, Any]
    dependencies: List[str]
    timestamp: str  # ISO-8601 string to match Java Instant
    priority: Priority = Priority.NORMAL
    
    class Config:
        allow_population_by_field_name = True

class NodeCompletionMessage(BaseModel):
    """Exact match to Spring Boot NodeCompletionMessage"""
    executionId: uuid.UUID = Field(alias="execution_id")
    workflowId: uuid.UUID = Field(alias="workflow_id")
    nodeId: str = Field(alias="node_id")
    nodeType: str = Field(alias="node_type")
    status: str  # COMPLETED, FAILED
    output: Dict[str, Any]
    error: Optional[str] = None
    timestamp: str  # ISO-8601 string to match Java Instant
    processingTime: int = Field(alias="processing_time")  # milliseconds
    service: str = "fastapi"  # Different from Spring Boot default
    
    class Config:
        allow_population_by_field_name = True
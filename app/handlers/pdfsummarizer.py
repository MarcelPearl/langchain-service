# import logging
# import time
# from datetime import datetime, timezone
# from typing import Dict, Any
# import pdfplumber
# from pydantic import Field
# from transformers import pipeline
# from app.handlers.basehandler import BaseNodeHandler
# from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
# from app.core.config import settings

# logger = logging.getLogger(__name__)

# class PDFSummarizationHandler(BaseNodeHandler):
#     """PDF Summarization node handler"""

#     def __init__(self, redis_service):
#         super().__init__(redis_service)
#         self.summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
#         logger.info("✅ PDF summarization model loaded")

#     async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
#         start_time = time.time()
#         logger.info(f"📄 Executing PDF summarization for node: {message.nodeId}")

#         try:
#             node_data = message.nodeData
#             context = message.context or {}
#             file_path = node_data.get("pdf_path")

#             if not file_path:
#                 raise ValueError("PDF path not provided in nodeData")

#             # ✅ Extract text from PDF
#             text = ""
#             with pdfplumber.open(file_path) as pdf:
#                 for page in pdf.pages:
#                     text += page.extract_text() + "\n"

#             if not text.strip():
#                 raise ValueError("PDF has no extractable text")

#             # ✅ Summarize text (limit to 1024 tokens for small models)
#             input_text = text[:4000]  # truncate long docs
#             summary = self.summarizer(input_text, max_length=150, min_length=50, do_sample=False)[0]["summary_text"]

#             output = {
#                 "summary": summary,
#                 "original_length": len(text),
#                 "node_type": "pdf-summarization",
#                 "pdf_path": file_path
#             }

#             processing_time = int((time.time() - start_time) * 1000)
#             await self._publish_completion_event(message, output, "COMPLETED", processing_time)

#             logger.info(f"✅ PDF summarization completed for node {message.nodeId}")
#             return output

#         except Exception as e:
#             processing_time = int((time.time() - start_time) * 1000)
#             logger.error(f"❌ PDF summarization failed: {e}", exc_info=True)

#             error_output = {
#                 "error": str(e),
#                 "node_type": "pdf-summarization"
#             }

#             await self._publish_completion_event(message, error_output, "FAILED", processing_time)
#             raise

#     async def _publish_completion_event(self, message: NodeExecutionMessage, 
#                                         output: Dict[str, Any], status: str, processing_time: int):
#         try:
#             from app.main import app
#             completion_message = NodeCompletionMessage(
#                 executionId=message.executionId,
#                 workflowId=message.workflowId,
#                 nodeId=message.nodeId,
#                 nodeType=message.nodeType,
#                 status=status,
#                 output=output,
#                 error=output.get("error") if status == "FAILED" else None,
#                 timestamp=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
#                 processingTime=processing_time
#             )
#             if hasattr(app.state, 'kafka_service'):
#                 await app.state.kafka_service.publish_completion(completion_message)
#         except Exception as e:
#             logger.error(f"Failed to publish completion event: {e}")



# S3 support needed
import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage

logger = logging.getLogger(__name__)

class KMeansHandler(BaseNodeHandler):
    """K-means clustering handler matching Spring Boot handler patterns"""
    
    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        """Execute K-means clustering - exact match to Spring Boot handler pattern"""
        start_time = time.time()
        logger.info(f"Executing k-means node: {message.nodeId}")
        
        try:
            node_data = message.nodeData
            context = message.context if message.context else {}
            
            logger.info(f"K-means node context variables: {list(context.keys())}")
            
            # Get clustering parameters
            n_clusters = node_data.get('n_clusters', 3)
            data_source = node_data.get('data_source', 'context_data')
            features = node_data.get('features', [])
            scale_features = node_data.get('scale_features', True)
            
            logger.info(f"🔬 Running K-means clustering with {n_clusters} clusters")
            
            # Get data from context or generate sample data
            if data_source == 'context_data' and 'dataset' in context:
                data = context['dataset']
                if isinstance(data, str):
                    import json
                    data = json.loads(data)
                df = pd.DataFrame(data)
            else:
                # Generate sample data for demonstration
                logger.info("📊 Generating sample data for clustering")
                np.random.seed(42)
                data = {
                    'feature_1': np.random.randn(100).tolist(),
                    'feature_2': np.random.randn(100).tolist(),
                    'feature_3': np.random.randn(100).tolist()
                }
                df = pd.DataFrame(data)
                features = ['feature_1', 'feature_2', 'feature_3']
            
            # Select features for clustering
            if features:
                available_features = [f for f in features if f in df.columns]
                if not available_features:
                    raise ValueError(f"None of the specified features {features} found in data")
                df_features = df[available_features]
            else:
                df_features = df.select_dtypes(include=[np.number])
            
            if df_features.empty:
                raise ValueError("No numeric features found for clustering")
            
            # Scale features if requested
            if scale_features:
                scaler = StandardScaler()
                X = scaler.fit_transform(df_features)
                feature_means = scaler.mean_.tolist()
                feature_stds = scaler.scale_.tolist()
            else:
                X = df_features.values
                feature_means = None
                feature_stds = None
            
            # Perform K-means clustering
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            cluster_labels = kmeans.fit_predict(X)
            
            # Calculate additional metrics
            inertia = float(kmeans.inertia_)
            cluster_centers = kmeans.cluster_centers_.tolist()
            
            # Count points in each cluster
            unique, counts = np.unique(cluster_labels, return_counts=True)
            cluster_counts = dict(zip(unique.tolist(), counts.tolist()))
            
            # Build output matching Spring Boot pattern
            output = {}
            if context:
                output.update(context)  # Preserve entire context like Spring Boot
            
            # Add results
            output.update({
                "cluster_labels": cluster_labels.tolist(),
                "cluster_centers": cluster_centers,
                "cluster_counts": cluster_counts,
                "inertia": inertia,
                "n_clusters": n_clusters,
                "features_used": df_features.columns.tolist(),
                "data_points": len(df_features),
                "node_type": "k-means",
                "node_executed_at": datetime.now().isoformat(),
                "scaled": scale_features
            })
            
            if feature_means is not None:
                output["feature_scaling"] = {
                    "means": feature_means,
                    "stds": feature_stds
                }
            
            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)
            
            logger.info(f"✅ K-means clustering completed: {len(set(cluster_labels))} clusters formed")
            return output
            
        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ K-means clustering node failed: {message.nodeId}", exc_info=True)
            
            # Build error output matching Spring Boot pattern
            error_output = {}
            if message.context:
                error_output.update(message.context)
            
            error_output.update({
                "error": str(e),
                "cluster_labels": None,
                "cluster_centers": None,
                "failed_at": datetime.now().isoformat(),
                "node_type": "k-means"
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
                logger.info(f"Published completion event for k-means node: {message.nodeId} "
                           f"with status: {status} in {processing_time}ms")
        except Exception as e:
            logger.error(f"Failed to publish completion event: {e}")
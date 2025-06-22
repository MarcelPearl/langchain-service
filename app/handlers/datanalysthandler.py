import logging
import time
import asyncio
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
from datetime import datetime, timezone
from typing import Dict, Any
import openai
from app.handlers.basehandler import BaseNodeHandler
from app.models.workflow_message import NodeExecutionMessage, NodeCompletionMessage
from app.core.config import settings
logger = logging.getLogger(__name__)

class DataAnalystAgentHandler(BaseNodeHandler):
    """AI agent that analyzes data and creates insights with visualizations"""
    
    def __init__(self, redis_service):
        super().__init__(redis_service)
        try:
            api_key=getattr(settings, 'openai_api_key', None) or os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OpenAI API key not found")
            
            self.client = openai.OpenAI(api_key=api_key)
            logger.info("📊 Data analyst agent ready")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize data analyst agent: {e}")
            self.client = None

    async def execute(self, message: NodeExecutionMessage) -> Dict[str, Any]:
        start_time = time.time()
        logger.info(f"📈 Executing data-analyst-agent node: {message.nodeId}")

        try:
            if not self.client:
                raise RuntimeError("OpenAI client not available")

            node_data = message.nodeData
            context = message.context or {}



            analysis_request = self.substitute_template_variables(node_data.get("analysis_request", ""), context)
            create_visualization = node_data.get("create_visualization", True)
            
            if not analysis_request.strip():
                raise ValueError("No analysis request provided")




            if 'dataset' in context:
                data = context['dataset']
                if isinstance(data, str):
                    import json
                    data = json.loads(data)
                df = pd.DataFrame(data)
            else:


                df = await self._generate_sample_data(analysis_request)

            logger.info(f"📊 Analyzing data: {df.shape[0]} rows, {df.shape[1]} columns")



            analysis_plan = await self._create_analysis_plan(analysis_request, df)
            

            analysis_results = await self._perform_analysis(df, analysis_plan)
            
 

            visualization = None
            if create_visualization:
                visualization = await self._create_visualization(df, analysis_plan, analysis_results)
            


            insights = await self._generate_insights(analysis_request, analysis_results, df)

            output = {
                **context,
                "analysis_request": analysis_request,
                "analysis_plan": analysis_plan,
                "analysis_results": analysis_results,
                "insights": insights,
                "visualization": visualization,
                "data_shape": {"rows": df.shape[0], "columns": df.shape[1]},
                "columns": df.columns.tolist(),
                "node_type": "data-analyst-agent",
                "node_executed_at": datetime.now().isoformat()
            }

            processing_time = int((time.time() - start_time) * 1000)
            await self._publish_completion_event(message, output, "COMPLETED", processing_time)

            logger.info(f"✅ Data analysis completed with insights")
            return output

        except Exception as e:
            processing_time = int((time.time() - start_time) * 1000)
            logger.error(f"❌ Data analyst agent failed: {e}")

            error_output = {
                **context,
                "error": str(e),
                "analysis_request": node_data.get("analysis_request", ""),
                "node_type": "data-analyst-agent"
            }

            await self._publish_completion_event(message, error_output, "FAILED", processing_time)
            raise

    async def _create_analysis_plan(self, request: str, df: pd.DataFrame) -> Dict[str, Any]:
        """AI creates an analysis plan based on the request and data"""
        def _call_openai():
            data_info = f"Dataset shape: {df.shape}\nColumns: {df.columns.tolist()}\nData types: {df.dtypes.to_dict()}"
            
            prompt = f"""As a data analyst, create an analysis plan for this request:

Request: "{request}"

Dataset info:
{data_info}

Sample data:
{df.head().to_string()}

Create a JSON analysis plan with:
1. "analysis_type": (descriptive/correlation/trend/comparison)
2. "key_columns": [columns to focus on]
3. "calculations": [specific calculations to perform]
4. "visualization_type": (bar/line/scatter/histogram/heatmap)

Respond only with valid JSON."""

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.1
            )
            
            try:
                import json
                return json.loads(response.choices[0].message.content.strip())
            except:
     

                return {
                    "analysis_type": "descriptive",
                    "key_columns": df.select_dtypes(include=[np.number]).columns.tolist()[:3],
                    "calculations": ["mean", "std", "correlation"],
                    "visualization_type": "bar"
                }

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _call_openai)

    async def _perform_analysis(self, df: pd.DataFrame, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Perform the actual data analysis"""
        results = {}
        
        try:
            key_columns = plan.get("key_columns", [])
            calculations = plan.get("calculations", [])
            


            if "mean" in calculations:
                results["means"] = df[key_columns].mean().to_dict() if key_columns else {}
            
            if "std" in calculations:
                results["std_devs"] = df[key_columns].std().to_dict() if key_columns else {}
            
            if "correlation" in calculations and len(key_columns) > 1:
                results["correlations"] = df[key_columns].corr().to_dict()
            
   
            results["summary_stats"] = df.describe().to_dict()
            
   
            categorical_cols = df.select_dtypes(include=['object']).columns
            if len(categorical_cols) > 0:
                results["value_counts"] = {}
                for col in categorical_cols[:2]:  
                    results["value_counts"][col] = df[col].value_counts().to_dict()
            
            return results
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return {"error": str(e)}

    async def _create_visualization(self, df: pd.DataFrame, plan: Dict, results: Dict) -> str:
        """Create a visualization and return as base64 string"""
        try:
            plt.figure(figsize=(10, 6))
            viz_type = plan.get("visualization_type", "bar")
            key_columns = plan.get("key_columns", [])
            
            if viz_type == "bar" and key_columns:
                if len(key_columns) == 1:
                    df[key_columns[0]].value_counts().plot(kind='bar')
                    plt.title(f'Distribution of {key_columns[0]}')
                else:
                    df[key_columns].mean().plot(kind='bar')
                    plt.title('Mean Values by Column')
            
            elif viz_type == "line" and key_columns:
                for col in key_columns[:3]:
                    plt.plot(df.index, df[col], label=col)
                plt.legend()
                plt.title('Trends Over Time')
            
            elif viz_type == "scatter" and len(key_columns) >= 2:
                plt.scatter(df[key_columns[0]], df[key_columns[1]])
                plt.xlabel(key_columns[0])
                plt.ylabel(key_columns[1])
                plt.title(f'{key_columns[0]} vs {key_columns[1]}')
            
            elif viz_type == "histogram" and key_columns:
                df[key_columns[0]].hist(bins=20)
                plt.title(f'Histogram of {key_columns[0]}')
            
            elif viz_type == "heatmap" and len(key_columns) > 1:
                correlation_matrix = df[key_columns].corr()
                sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
                plt.title('Correlation Heatmap')
            
            else:
       
                numeric_cols = df.select_dtypes(include=[np.number]).columns[:5]
                df[numeric_cols].mean().plot(kind='bar')
                plt.title('Mean Values')
            
            plt.tight_layout()
            
      
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            
            return image_base64
            
        except Exception as e:
            logger.error(f"Visualization failed: {e}")
            return None

    async def _generate_sample_data(self, request: str) -> pd.DataFrame:
        """Generate sample data based on the analysis request"""

        np.random.seed(42)
        
        if "sales" in request.lower():
            data = {
                'month': pd.date_range('2024-01-01', periods=12, freq='M'),
                'sales': np.random.normal(10000, 2000, 12),
                'region': np.random.choice(['North', 'South', 'East', 'West'], 12)
            }
        elif "customer" in request.lower():
            data = {
                'customer_id': range(100),
                'age': np.random.normal(35, 10, 100),
                'spending': np.random.exponential(50, 100),
                'category': np.random.choice(['A', 'B', 'C'], 100)
            }
        else:

            data = {
                'value1': np.random.normal(100, 15, 50),
                'value2': np.random.normal(200, 30, 50),
                'category': np.random.choice(['X', 'Y', 'Z'], 50)
            }
        
        return pd.DataFrame(data)

    async def _generate_insights(self, request: str, results: Dict, df: pd.DataFrame) -> str:
        """AI generates insights from the analysis results"""
        def _call_openai():
            results_summary = str(results)[:1000]  # Limit length
            
            prompt = f"""As a data analyst, provide insights based on this analysis:

Original request: "{request}"
Data shape: {df.shape}
Analysis results: {results_summary}

Provide 3-5 key insights in bullet points. Focus on actionable findings and patterns."""

            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()

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

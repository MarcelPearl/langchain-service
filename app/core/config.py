from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka Configuration - matches Spring Boot
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "fastapi-node-executor"  # Different from Spring Boot
    kafka_node_execution_topic: str = "fastapi-nodes"
    kafka_node_completion_topic: str = "node-completion"
    kafka_auto_offset_reset: str = "earliest"
    
    # Redis Configuration - matches Spring Boot
    redis_url: str = "redis://localhost:6379"
    redis_db: int = 0
    redis_max_connections: int = 20
    huggingface_api_key: str = ""
    # AI/ML Configuration
    huggingface_model: str = "gpt2"
    max_tokens: int = 100
    temperature: float = 0.7
    summarization_model: str = "sshleifer/distilbart-cnn-12-6"
    
    # Service Configuration
    service_name: str = "fastapi-node-executor"
    log_level: str = "INFO"
    
    class Config:
        populate_by_name = True
        env_file = ".env"

settings = Settings()
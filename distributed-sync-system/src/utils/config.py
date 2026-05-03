import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    node_count: int = int(os.getenv("NODE_COUNT", "3"))
    node_host: str = os.getenv("NODE_HOST", "0.0.0.0")
    base_port: int = int(os.getenv("BASE_PORT", "8000"))
    election_timeout_min: int = int(os.getenv("ELECTION_TIMEOUT_MIN", "150"))
    election_timeout_max: int = int(os.getenv("ELECTION_TIMEOUT_MAX", "300"))
    heartbeat_interval: int = int(os.getenv("HEARTBEAT_INTERVAL", "50"))
    lock_default_ttl: int = int(os.getenv("LOCK_DEFAULT_TTL", "30"))
    lock_max_ttl: int = int(os.getenv("LOCK_MAX_TTL", "300"))
    virtual_nodes: int = int(os.getenv("VIRTUAL_NODES", "150"))
    message_max_retry: int = int(os.getenv("MESSAGE_MAX_RETRY", "3"))
    consumer_timeout: int = int(os.getenv("CONSUMER_TIMEOUT", "30"))
    cache_max_size: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))
    cache_default_ttl: int = int(os.getenv("CACHE_DEFAULT_TTL", "300"))
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    redis_db: int = int(os.getenv("REDIS_DB", "0"))
    api_key_admin: str = os.getenv("API_KEY_ADMIN", "admin-secret-key")
    api_key_producer: str = os.getenv("API_KEY_PRODUCER", "producer-key")
    api_key_consumer: str = os.getenv("API_KEY_CONSUMER", "consumer-key")
    api_key_reader: str = os.getenv("API_KEY_READER", "reader-key")
    enable_tls: bool = os.getenv("ENABLE_TLS", "false").lower() == "true"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def get_node_addresses(self) -> List[str]:
        return [f"{self.node_host}:{self.base_port + i}" for i in range(self.node_count)]

    def get_node_id(self, index: int) -> str:
        return f"node_{index}"


config = Config()

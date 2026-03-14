"""Centralized configuration loaded from environment variables and .env file."""

from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path


class Settings(BaseSettings):
    # Database
    database_url: str = Field(default="postgresql+asyncpg://safekube:safekube123@localhost:5432/safekube")
    database_url_sync: str = Field(default="postgresql://safekube:safekube123@localhost:5432/safekube")

    # Kubernetes
    kubeconfig: str = Field(default="~/.kube/config")
    target_namespace: str = Field(default="staging")
    system_namespace: str = Field(default="safekube-system")

    # Telemetry
    log_buffer_size: int = Field(default=100)
    pod_status_poll_interval: int = Field(default=5)
    metrics_poll_interval: int = Field(default=15)
    event_watch_timeout: int = Field(default=300)

    # Incident Builder
    correlation_window_minutes: int = Field(default=15)
    bfs_depth: int = Field(default=2)
    bfs_proximity_seconds: int = Field(default=60)

    # Classifier
    classifier_confidence_threshold: float = Field(default=0.7)
    classifier_model_path: str = Field(default="data/models/setfit")

    # Risk Scoring
    risk_tier1_threshold: float = Field(default=0.40)
    risk_tier2_threshold: float = Field(default=0.70)

    # OpenAI
    openai_api_key: str = Field(default="")

    # General
    log_level: str = Field(default="INFO")

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


# Singleton - import this everywhere
settings = Settings()
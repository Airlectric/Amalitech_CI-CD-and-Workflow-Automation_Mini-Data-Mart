"""
Centralized Configuration Settings for Mini Data Platform

This module provides a central location for all configuration values
used across the data platform. Values can be overridden via
environment variables or Airflow Variables.
"""

import os
from dataclasses import dataclass

# ============================================================================
# Data Quality Configuration
# ============================================================================


@dataclass
class QualityConfig:
    """Data Quality check configuration"""

    DRIFT_THRESHOLD_PCT: float = 10.0
    COMPLETENESS_THRESHOLD_PCT: float = 95.0
    FRESHNESS_SLA_HOURS: int = 24

    @classmethod
    def from_environment(cls) -> "QualityConfig":
        return cls(
            DRIFT_THRESHOLD_PCT=float(os.getenv("QUALITY_DRIFT_THRESHOLD", "10.0")),
            COMPLETENESS_THRESHOLD_PCT=float(os.getenv("QUALITY_COMPLETENESS_THRESHOLD", "95.0")),
            FRESHNESS_SLA_HOURS=int(os.getenv("QUALITY_FRESHNESS_SLA_HOURS", "24")),
        )


# ============================================================================
# Alert Configuration
# ============================================================================


@dataclass
class AlertConfig:
    """Alert and notification configuration"""

    RETRY_MAX_ATTEMPTS: int = 3
    RETRY_BASE_DELAY_SECONDS: int = 2
    RETRY_MAX_DELAY_SECONDS: int = 60
    THROTTLE_WARNING_HOURS: int = 1
    THROTTLE_INFO_HOURS: int = 6

    @classmethod
    def from_environment(cls) -> "AlertConfig":
        return cls(
            RETRY_MAX_ATTEMPTS=int(os.getenv("ALERT_RETRY_MAX_ATTEMPTS", "3")),
            RETRY_BASE_DELAY_SECONDS=int(os.getenv("ALERT_RETRY_BASE_DELAY", "2")),
            RETRY_MAX_DELAY_SECONDS=int(os.getenv("ALERT_RETRY_MAX_DELAY", "60")),
            THROTTLE_WARNING_HOURS=int(os.getenv("ALERT_THROTTLE_WARNING_HOURS", "1")),
            THROTTLE_INFO_HOURS=int(os.getenv("ALERT_THROTTLE_INFO_HOURS", "6")),
        )


# ============================================================================
# Database Configuration
# ============================================================================


@dataclass
class DatabaseConfig:
    """Database connection configuration"""

    HOST: str = "postgres"
    PORT: int = 5432
    DATABASE: str = "airflow"
    USERNAME: str = "airflow"
    PASSWORD: str = "airflow"

    @classmethod
    def from_environment(cls) -> "DatabaseConfig":
        return cls(
            HOST=os.getenv("POSTGRES_HOST", "postgres"),
            PORT=int(os.getenv("POSTGRES_PORT", "5432")),
            DATABASE=os.getenv("POSTGRES_DB", "airflow"),
            USERNAME=os.getenv("POSTGRES_USER", "airflow"),
            PASSWORD=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )


# ============================================================================
# MinIO Configuration
# ============================================================================


@dataclass
class MinioConfig:
    """MinIO/S3 configuration"""

    ENDPOINT: str = "minio:9000"
    ACCESS_KEY: str = "minio"
    SECRET_KEY: str = "minio123"
    BUCKET: str = "bronze"
    USE_SSL: bool = False

    @classmethod
    def from_environment(cls) -> "MinioConfig":
        return cls(
            ENDPOINT=os.getenv("MINIO_ENDPOINT", "minio:9000"),
            ACCESS_KEY=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER", "minio"),
            SECRET_KEY=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
            BUCKET=os.getenv("MINIO_BUCKET", "bronze"),
            USE_SSL=os.getenv("MINIO_USE_SSL", "false").lower() == "true",
        )


# ============================================================================
# SMTP Configuration
# ============================================================================


@dataclass
class SmtpConfig:
    """SMTP email configuration"""

    HOST: str = "smtp.gmail.com"
    PORT: int = 587
    USER: str = ""
    PASSWORD: str = ""

    @classmethod
    def from_environment(cls) -> "SmtpConfig":
        user = os.getenv("AIRFLOW__SMTP__SMTP_USER", "")
        password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD", "")

        if not user or not password:
            raise ValueError(
                "SMTP credentials not configured. Set AIRFLOW__SMTP__SMTP_USER and AIRFLOW__SMTP__SMTP_PASSWORD"
            )

        return cls(
            HOST=os.getenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com"),
            PORT=int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587")),
            USER=user,
            PASSWORD=password,
        )


# ============================================================================
# Pipeline Configuration
# ============================================================================


@dataclass
class PipelineConfig:
    """ETL pipeline configuration"""

    BATCH_SIZE: int = 1000
    QUARANTINE_BATCH_SIZE: int = 1000
    MAX_RETRIES: int = 3
    RETRY_DELAY_MINUTES: int = 5

    @classmethod
    def from_environment(cls) -> "PipelineConfig":
        return cls(
            BATCH_SIZE=int(os.getenv("PIPELINE_BATCH_SIZE", "1000")),
            QUARANTINE_BATCH_SIZE=int(os.getenv("QUARANTINE_BATCH_SIZE", "1000")),
            MAX_RETRIES=int(os.getenv("PIPELINE_MAX_RETRIES", "3")),
            RETRY_DELAY_MINUTES=int(os.getenv("PIPELINE_RETRY_DELAY", "5")),
        )


# ============================================================================
# Silver Layer Configuration
# ============================================================================

SILVER_TABLES = ["sales", "customers", "products"]

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
QUARANTINE_SCHEMA = "quarantine"
METADATA_SCHEMA = "metadata"

# Critical columns for completeness checks
CRITICAL_COLUMNS = {
    "silver.sales": ["transaction_id", "customer_id", "product_id", "sale_date", "net_amount"],
    "silver.customers": ["customer_id", "customer_name"],
    "silver.products": ["product_id", "product_name", "category"],
}


# ============================================================================
# Gold Layer Tables
# ============================================================================

GOLD_TABLES = ["daily_sales", "product_performance", "customer_analytics", "store_performance", "category_insights"]


# ============================================================================
# Alert Recipients (can be overridden via Airflow Variable)
# ============================================================================

DEFAULT_ALERT_EMAIL = "daniel.doe@a2sv.org"


# ============================================================================
# Helper Functions
# ============================================================================


def get_quality_config() -> QualityConfig:
    """Get quality configuration"""
    return QualityConfig.from_environment()


def get_alert_config() -> AlertConfig:
    """Get alert configuration"""
    return AlertConfig.from_environment()


def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return DatabaseConfig.from_environment()


def get_minio_config() -> MinioConfig:
    """Get MinIO configuration"""
    return MinioConfig.from_environment()


def get_smtp_config() -> SmtpConfig:
    """Get SMTP configuration"""
    return SmtpConfig.from_environment()


def get_pipeline_config() -> PipelineConfig:
    """Get pipeline configuration"""
    return PipelineConfig.from_environment()
